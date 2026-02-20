#' Fit age predictor (survey Q3 backbone + fishery quarter delta + priors)
#'
#' Robust to single-level factors (SEX/YEAR/AREA/QUARTER/REGION) and low unique-age support.
#' Automatically reduces spline basis dimensions (k) to avoid mgcv "fewer unique
#' covariate combinations than k" errors.
#'
#' Adds optional hierarchical spatial structure when REGION_GRP is present:
#' - REGION_GRP random effect: broad-scale shifts among user-defined regions
#' - AREA nested within REGION_GRP random effect: fine-scale shifts among areas within region
#'
#' @param fish_dt Fishery aged data (from get_fishery_age_wt_data()).
#'   Must include YEAR, QUARTER, LENGTH (cm), AGE, SEX (F/M/U), AREA.
#'   If REGION_GRP exists (recommended; from region_def), it will be used for hierarchical spatial REs.
#' @param srv_dt Survey aged data (from GET_SURV_AGE()).
#'   Must include YEAR, LENGTH_MM (mm), AGE, SEX (1/2/3 or F/M/U). All records are Q3.
#'   If REGION_GRP exists (recommended; from region_def), it will be used for hierarchical spatial REs.
#' @param maxage Plus-group maximum age.
#' @param min_n_cell Minimum aged fish in a cell to use cell prior (else pooled).
#' @param prior_mix Mixing weight for cell prior vs pooled prior.
#' @param k_age_survey Target k for survey age smooth (will be reduced if needed).
#' @param k_age_delta  Target k for fishery delta age smooth (will be reduced if needed).
#' @return A list with fitted models and prior tables.
#' @export
fit_age_predictor <- function(fish_dt,
                              srv_dt,
                              maxage = 12L,
                              min_n_cell = 30L,
                              prior_mix = 0.7,
                              k_age_survey = 10L,
                              k_age_delta  = 8L) {

  if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table required.", call. = FALSE)
  if (!requireNamespace("mgcv", quietly = TRUE)) stop("mgcv required.", call. = FALSE)

  DT <- data.table::as.data.table

  standardize_sex_fishery <- function(x) {
    x <- toupper(trimws(as.character(x)))
    x[!x %in% c("M","F","U")] <- NA_character_
    x
  }

  standardize_sex_survey <- function(x) {
    # survey: 1=M, 2=F, 3=U (also accept letters)
    x0 <- trimws(as.character(x))
    out <- rep(NA_character_, length(x0))
    out[x0 == "1"] <- "M"
    out[x0 == "2"] <- "F"
    out[x0 == "3"] <- "U"
    x1 <- toupper(x0)
    out[is.na(out) & x1 %in% c("M","F","U")] <- x1[is.na(out) & x1 %in% c("M","F","U")]
    out
  }

  # choose a safe k given number of unique ages (or unique combos for by-smooth)
  safe_k <- function(k_target, n_unique, min_k = 3L) {
    # mgcv typically needs k <= unique(x) (often unique(x)-1 is safest for 1D)
    k_max <- max(min_k, as.integer(n_unique) - 1L)
    as.integer(max(min_k, min(as.integer(k_target), k_max)))
  }

  safe_k_by <- function(k_target, age_vec, by_vec, min_k = 3L) {
    # conservative: take the MIN unique ages across groups
    tmp <- data.table::data.table(age = age_vec, by = by_vec)
    nmin <- tmp[, data.table::uniqueN(age), by = by][, min(V1)]
    safe_k(k_target, nmin, min_k = min_k)
  }

  # helper: build REGION_F + nested AREA_R if REGION_GRP is present & usable
  add_region_structure <- function(DT0, area_key_col = "AREA_K") {
    out <- data.table::copy(DT0)
    has_region <- "REGION_GRP" %in% names(out)
    if (!has_region) {
      out[, REGION_F := factor("ALL")]
      out[, AREA_F   := factor(get(area_key_col))]
      out[, AREA_R   := factor(get(area_key_col))] # fallback (not truly nested)
      return(list(DT = out, has_region = FALSE))
    }

    out[, REGION_GRP := as.character(REGION_GRP)]
    out <- out[!is.na(REGION_GRP) & nzchar(REGION_GRP)]

    # If user-provided region collapses to 1 level after filtering, treat as absent
    n_reg <- data.table::uniqueN(out$REGION_GRP)
    if (n_reg < 2) {
      out[, REGION_F := factor("ALL")]
      out[, AREA_F   := factor(get(area_key_col))]
      out[, AREA_R   := factor(get(area_key_col))] # fallback
      return(list(DT = out, has_region = FALSE))
    }

    out[, REGION_F := factor(REGION_GRP)]
    out[, AREA_F   := factor(get(area_key_col))]
    out[, AREA_R   := interaction(REGION_F, AREA_F, drop = TRUE)]
    list(DT = out, has_region = TRUE)
  }

  # ---- fishery ----
  f <- DT(fish_dt); names(f) <- toupper(names(f))
  need_f <- c("YEAR","QUARTER","LENGTH","AGE","SEX","AREA")
  miss_f <- setdiff(need_f, names(f))
  if (length(miss_f) > 0) stop("fish_dt missing: ", paste(miss_f, collapse=", "), call. = FALSE)

  suppressWarnings({
    f[, YEAR := as.integer(as.character(YEAR))]
    f[, QUARTER := as.integer(as.character(QUARTER))]
    f[, LENGTH := as.numeric(LENGTH)]
    f[, AGE := as.integer(as.character(AGE))]
  })
  f[, SEX := standardize_sex_fishery(SEX)]
  f <- f[SEX %in% c("F","M")]
  f <- f[!is.na(YEAR) & QUARTER %in% 1:4 & is.finite(LENGTH) & is.finite(AGE)]
  f[, AGE_G := pmin(pmax(AGE, 0L), as.integer(maxage))]
  f[, AREA_K := as.character(AREA)]

  if (nrow(f) == 0) stop("fish_dt has 0 usable rows after filtering.", call. = FALSE)

  # add region nesting if present
  tmp_f <- add_region_structure(f, area_key_col = "AREA_K")
  f <- tmp_f$DT
  has_region_f <- tmp_f$has_region

  if (nrow(f) == 0) stop("fish_dt has 0 usable rows after REGION_GRP filtering.", call. = FALSE)

  # ---- survey (Q3 only) ----
  s <- DT(srv_dt); names(s) <- toupper(names(s))
  if (!"YEAR" %in% names(s) && "HAUL_YEAR" %in% names(s)) data.table::setnames(s, "HAUL_YEAR", "YEAR")
  need_s <- c("YEAR","LENGTH_MM","AGE","SEX")
  miss_s <- setdiff(need_s, names(s))
  if (length(miss_s) > 0) stop("srv_dt missing: ", paste(miss_s, collapse=", "), call. = FALSE)

  suppressWarnings({
    s[, YEAR := as.integer(as.character(YEAR))]
    s[, LENGTH := as.numeric(LENGTH_MM) / 10]   # mm -> cm
    s[, AGE := as.integer(as.character(AGE))]
  })
  s[, SEX := standardize_sex_survey(SEX)]
  s <- s[SEX %in% c("F","M")]
  s <- s[!is.na(YEAR) & is.finite(LENGTH) & is.finite(AGE)]
  s[, AGE_G := pmin(pmax(AGE, 0L), as.integer(maxage))]
  s[, QUARTER := 3L]

  # Survey AREA handling (optional)
  if ("NMFS_AREA" %in% names(s)) {
    suppressWarnings(s[, NMFS_AREA := as.integer(as.character(NMFS_AREA))])
    s[, AREA_K := as.character(trunc(NMFS_AREA / 10))]
    s[AREA_K == "50", AREA_K := "51"]
  } else if ("AREA" %in% names(s)) {
    s[, AREA_K := as.character(AREA)]
  } else {
    s[, AREA_K := "ALL"]
  }

  if (nrow(s) == 0) stop("srv_dt has 0 usable rows after filtering.", call. = FALSE)

  # add region nesting if present
  tmp_s <- add_region_structure(s, area_key_col = "AREA_K")
  s <- tmp_s$DT
  has_region_s <- tmp_s$has_region

  if (nrow(s) == 0) stop("srv_dt has 0 usable rows after REGION_GRP filtering.", call. = FALSE)

  # ---- fit survey backbone: L_Q3 | age, sex, (year RE), (region/area REs) ----
  s_fit <- data.table::copy(s)
  s_fit[, YEAR_F := factor(YEAR)]
  s_fit[, SEX_F  := factor(SEX, levels = c("F","M"))]
  # REGION_F, AREA_F, AREA_R already created by add_region_structure()

  n_sex_s  <- data.table::uniqueN(s_fit$SEX_F)
  n_year_s <- data.table::uniqueN(s_fit$YEAR_F)
  n_reg_s  <- data.table::uniqueN(s_fit$REGION_F)
  n_area_s <- data.table::uniqueN(s_fit$AREA_F)
  n_areaR_s <- data.table::uniqueN(s_fit$AREA_R)

  # choose k safely
  if (n_sex_s >= 2) {
    k_s <- safe_k_by(k_age_survey, s_fit$AGE_G, s_fit$SEX_F)
  } else {
    k_s <- safe_k(k_age_survey, data.table::uniqueN(s_fit$AGE_G))
  }

  # build survey formula adaptively
  survey_terms <- c()

  # SEX main effect only if 2+ levels
  if (n_sex_s >= 2) survey_terms <- c(survey_terms, "SEX_F")

  # age smooth: by sex only if 2+ sex levels
  if (n_sex_s >= 2) {
    survey_terms <- c(survey_terms, sprintf("s(AGE_G, by = SEX_F, k = %d)", k_s))
  } else {
    survey_terms <- c(survey_terms, sprintf("s(AGE_G, k = %d)", k_s))
  }

  # YEAR RE only if 2+ levels
  if (n_year_s >= 2) survey_terms <- c(survey_terms, "s(YEAR_F, bs = 're')")

  # Spatial RE structure:
  # If REGION_GRP has support (2+ levels), use region + nested area-within-region.
  # Else fall back to AREA_F alone if it has 2+ levels.
  if (has_region_s && n_reg_s >= 2) {
    survey_terms <- c(survey_terms, "s(REGION_F, bs = 're')")
    if (n_areaR_s >= 2) survey_terms <- c(survey_terms, "s(AREA_R, bs = 're')")
  } else {
    if (n_area_s >= 2) survey_terms <- c(survey_terms, "s(AREA_F, bs = 're')")
  }

  survey_form <- stats::as.formula(paste("LENGTH ~", paste(survey_terms, collapse = " + ")))

  survey_fit <- mgcv::gam(
    formula = survey_form,
    data = s_fit,
    method = "REML"
  )

  # ---- fishery quarter delta: DELTA = L_obs - mu_Q3(age,sex,year,region/area) ----
  f_fit <- data.table::copy(f)
  f_fit[, YEAR_F := factor(YEAR)]
  f_fit[, SEX_F  := factor(SEX, levels = c("F","M"))]
  f_fit[, Q_F    := factor(QUARTER, levels = 1:4)]
  # REGION_F, AREA_F, AREA_R already created by add_region_structure()

  n_q_f    <- data.table::uniqueN(f_fit$Q_F)
  n_sex_f  <- data.table::uniqueN(f_fit$SEX_F)
  n_year_f <- data.table::uniqueN(f_fit$YEAR_F)
  n_reg_f  <- data.table::uniqueN(f_fit$REGION_F)
  n_area_f <- data.table::uniqueN(f_fit$AREA_F)
  n_areaR_f <- data.table::uniqueN(f_fit$AREA_R)

  # store levels used in survey fit for safer prediction
  lev_year_s  <- levels(s_fit$YEAR_F)
  lev_sex_s   <- levels(s_fit$SEX_F)
  lev_reg_s   <- levels(s_fit$REGION_F)
  lev_area_s  <- levels(s_fit$AREA_F)
  lev_areaR_s <- levels(s_fit$AREA_R)

  # Construct AREA_R for newdata consistently with survey levels
  new_region <- factor(as.character(f_fit$REGION_F), levels = lev_reg_s)
  new_area   <- factor(as.character(f_fit$AREA_F),   levels = lev_area_s)
  new_areaR  <- factor(interaction(new_region, new_area, drop = TRUE), levels = lev_areaR_s)

  nd <- data.frame(
    SEX_F    = factor(as.character(f_fit$SEX_F), levels = lev_sex_s),
    AGE_G    = f_fit$AGE_G,
    YEAR_F   = factor(f_fit$YEAR, levels = lev_year_s),
    REGION_F = new_region,
    AREA_F   = new_area,
    AREA_R   = new_areaR
  )

  mu_q3 <- as.numeric(stats::predict(survey_fit, newdata = nd, type = "response"))

  f_fit[, MU_Q3 := mu_q3]
  f_fit[, DELTA := LENGTH - MU_Q3]

  # choose k safely for delta smooth
  if (n_q_f >= 2) {
    k_d <- safe_k_by(k_age_delta, f_fit$AGE_G, f_fit$Q_F)
  } else {
    k_d <- safe_k(k_age_delta, data.table::uniqueN(f_fit$AGE_G))
  }

  delta_terms <- c()

  # quarter effect only if 2+ quarters
  if (n_q_f >= 2) delta_terms <- c(delta_terms, "Q_F")

  # age smooth by quarter only if 2+ quarters
  if (n_q_f >= 2) {
    delta_terms <- c(delta_terms, sprintf("s(AGE_G, by = Q_F, k = %d)", k_d))
  } else {
    delta_terms <- c(delta_terms, sprintf("s(AGE_G, k = %d)", k_d))
  }

  # sex main effect only if 2+ sexes in fishery ages
  if (n_sex_f >= 2) delta_terms <- c(delta_terms, "SEX_F")

  # YEAR RE only if 2+ levels
  if (n_year_f >= 2) delta_terms <- c(delta_terms, "s(YEAR_F, bs = 're')")

  # Spatial RE structure:
  # If REGION_GRP has support (2+ levels), use region + nested area-within-region.
  # Else fall back to AREA_F alone if it has 2+ levels.
  if (has_region_f && n_reg_f >= 2) {
    delta_terms <- c(delta_terms, "s(REGION_F, bs = 're')")
    if (n_areaR_f >= 2) delta_terms <- c(delta_terms, "s(AREA_R, bs = 're')")
  } else {
    if (n_area_f >= 2) delta_terms <- c(delta_terms, "s(AREA_F, bs = 're')")
  }

  delta_form <- stats::as.formula(paste("DELTA ~", paste(delta_terms, collapse = " + ")))

  delta_fit <- mgcv::gam(
    formula = delta_form,
    data = f_fit,
    method = "REML"
  )

  # ---- priors from fishery ages ----
  prior_pool <- f[, .N, by = .(AREA_K, SEX, AGE_G)]
  prior_pool[, P_POOL := N / sum(N), by = .(AREA_K, SEX)]

  prior_global <- f[, .N, by = .(SEX, AGE_G)]
  prior_global[, P_GLOB := N / sum(N), by = .(SEX)]

  prior_cell <- f[, .N, by = .(YEAR, QUARTER, AREA_K, SEX, AGE_G)]
  prior_cell[, N_CELL := sum(N), by = .(YEAR, QUARTER, AREA_K, SEX)]
  prior_cell[, P_CELL := N / sum(N), by = .(YEAR, QUARTER, AREA_K, SEX)]

  list(
    maxage = as.integer(maxage),
    min_n_cell = as.integer(min_n_cell),
    prior_mix = as.numeric(prior_mix),

    survey_fit = survey_fit,
    delta_fit  = delta_fit,

    # priors
    prior_pool = prior_pool,
    prior_global = prior_global,
    prior_cell = prior_cell,

    # store factor levels used in fitting (helps avoid surprises in predict)
    levels = list(
      survey = list(
        SEX_F    = levels(s_fit$SEX_F),
        YEAR_F   = levels(s_fit$YEAR_F),
        REGION_F = levels(s_fit$REGION_F),
        AREA_F   = levels(s_fit$AREA_F),
        AREA_R   = levels(s_fit$AREA_R)
      ),
      fishery = list(
        SEX_F    = levels(f_fit$SEX_F),
        YEAR_F   = levels(f_fit$YEAR_F),
        REGION_F = levels(f_fit$REGION_F),
        AREA_F   = levels(f_fit$AREA_F),
        AREA_R   = levels(f_fit$AREA_R),
        Q_F      = levels(f_fit$Q_F)
      )
    ),

    # store chosen ks (useful for debugging)
    k_used = list(
      survey_age_k = k_s,
      delta_age_k  = k_d
    ),

    # flags (useful for debugging / reporting)
    flags = list(
      has_region_survey = has_region_s,
      has_region_fishery = has_region_f
    )
  )
}