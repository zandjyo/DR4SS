#' Fit region-specific age predictor (survey Q3 backbone + fishery quarter delta + priors)
#'
#' This version enforces **no borrowing among regions**:
#' a separate predictor is fit for each REGION_GRP, and predictions must use the
#' matching REGION_GRP model.
#'
#' Borrowing is allowed:
#' * across YEARS **within** a region (missing YEAR random effects set to 0 at predict-time)
#' * across AREAS **within** a region (missing AREA random effects set to 0 at predict-time)
#'
#' @param fish_dt Fishery aged data. Must include YEAR, QUARTER, LENGTH (cm), AGE, SEX (F/M/U), AREA.
#'   REGION_GRP is required to fit separate regional models; if missing, all data are treated as one region ("ALL").
#' @param srv_dt Survey aged data (Q3). Must include YEAR, LENGTH_MM (mm), AGE, SEX, and optionally NMFS_AREA/AREA.
#'   REGION_GRP is required to fit separate regional models; if missing, all data are treated as one region ("ALL").
#' @param maxage Plus-group maximum age.
#' @param min_n_cell Minimum aged fish in a (YEAR,QUARTER,AREA,SEX) cell to use cell prior (else pooled).
#' @param prior_mix Mixing weight for cell prior vs pooled prior.
#' @param k_age_survey Target k for survey age smooth (auto-reduced if needed).
#' @param k_age_delta  Target k for fishery delta age smooth (auto-reduced if needed).
#'
#' @return A list with one element per REGION_GRP containing:
#'   survey_fit, delta_fit, priors, factor levels, and meta.
#' @export
fit_age_predictor <- function(fish_dt,
                              srv_dt,
                              maxage = 10L,
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
    x0 <- trimws(as.character(x))
    out <- rep(NA_character_, length(x0))
    out[x0 == "1"] <- "M"; out[x0 == "2"] <- "F"; out[x0 == "3"] <- "U"
    x1 <- toupper(x0)
    out[is.na(out) & x1 %in% c("M","F","U")] <- x1[is.na(out) & x1 %in% c("M","F","U")]
    out
  }

  safe_k <- function(k_target, n_unique, min_k = 3L) {
    k_max <- max(min_k, as.integer(n_unique) - 1L)
    as.integer(max(min_k, min(as.integer(k_target), k_max)))
  }
  safe_k_by <- function(k_target, age_vec, by_vec, min_k = 3L) {
    tmp <- data.table::data.table(age = age_vec, by = by_vec)
    nmin <- tmp[, data.table::uniqueN(age), by = by][, min(V1)]
    safe_k(k_target, nmin, min_k = min_k)
  }

  # ---- preprocess fishery ----
  f <- DT(fish_dt); names(f) <- toupper(names(f))
  need_f <- c("YEAR","QUARTER","LENGTH","AGE","SEX","AREA")
  miss_f <- setdiff(need_f, names(f))
  if (length(miss_f) > 0) stop("fish_dt missing: ", paste(miss_f, collapse=", "), call. = FALSE)

  if (!"REGION_GRP" %in% names(f)) f[, REGION_GRP := "ALL"]

  suppressWarnings({
    f[, YEAR := as.integer(as.character(YEAR))]
    f[, QUARTER := as.integer(as.character(QUARTER))]
    f[, LENGTH := as.numeric(LENGTH)]
    f[, AGE := as.integer(as.character(AGE))]
  })
  f[, SEX := standardize_sex_fishery(SEX)]
  f <- f[SEX %in% c("F","M")]  # predictor fit is sex-specific; U is handled at prediction time
  f <- f[!is.na(YEAR) & QUARTER %in% 1:4 & is.finite(LENGTH) & is.finite(AGE)]
  f[, AGE_G := pmin(pmax(AGE, 0L), as.integer(maxage))]
  f[, AREA_K := as.character(AREA)]
  f[, REGION_GRP := as.character(REGION_GRP)]
  f <- f[!is.na(REGION_GRP) & nzchar(REGION_GRP)]

  if (nrow(f) == 0) stop("fish_dt has 0 usable rows after filtering.", call. = FALSE)

  # ---- preprocess survey ----
  s <- DT(srv_dt); names(s) <- toupper(names(s))
  if (!"YEAR" %in% names(s) && "HAUL_YEAR" %in% names(s)) data.table::setnames(s, "HAUL_YEAR", "YEAR")

  need_s <- c("YEAR","LENGTH_MM","AGE","SEX")
  miss_s <- setdiff(need_s, names(s))
  if (length(miss_s) > 0) stop("srv_dt missing: ", paste(miss_s, collapse=", "), call. = FALSE)

  if (!"REGION_GRP" %in% names(s)) s[, REGION_GRP := "ALL"]

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
  s[, REGION_GRP := as.character(REGION_GRP)]
  s <- s[!is.na(REGION_GRP) & nzchar(REGION_GRP)]

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

  # Regions to fit: only those with at least some survey AND some fishery ages
  regs <- sort(intersect(unique(s$REGION_GRP), unique(f$REGION_GRP)))
  if (length(regs) == 0) {
    stop("No REGION_GRP overlap between survey and fishery data (need both per region).", call. = FALSE)
  }

  fit_one_region <- function(reg) {
    f_r <- f[REGION_GRP == reg]
    s_r <- s[REGION_GRP == reg]
    if (nrow(f_r) == 0 || nrow(s_r) == 0) return(NULL)

    # ---- survey backbone ----
    s_fit <- data.table::copy(s_r)
    s_fit[, YEAR_F := factor(YEAR)]
    s_fit[, SEX_F  := factor(SEX, levels = c("F","M"))]
    s_fit[, AREA_F := factor(AREA_K)]

    n_sex_s  <- data.table::uniqueN(s_fit$SEX_F)
    n_year_s <- data.table::uniqueN(s_fit$YEAR_F)
    n_area_s <- data.table::uniqueN(s_fit$AREA_F)

    if (n_sex_s >= 2) {
      k_s <- safe_k_by(k_age_survey, s_fit$AGE_G, s_fit$SEX_F)
    } else {
      k_s <- safe_k(k_age_survey, data.table::uniqueN(s_fit$AGE_G))
    }

    survey_terms <- c()
    if (n_sex_s >= 2) survey_terms <- c(survey_terms, "SEX_F")
    if (n_sex_s >= 2) {
      survey_terms <- c(survey_terms, sprintf("s(AGE_G, by = SEX_F, k = %d)", k_s))
    } else {
      survey_terms <- c(survey_terms, sprintf("s(AGE_G, k = %d)", k_s))
    }
    if (n_year_s >= 2) survey_terms <- c(survey_terms, "s(YEAR_F, bs = 're')")
    if (n_area_s >= 2) survey_terms <- c(survey_terms, "s(AREA_F, bs = 're')")

    survey_form <- stats::as.formula(paste("LENGTH ~", paste(survey_terms, collapse = " + ")))
    survey_fit <- mgcv::gam(survey_form, data = s_fit, method = "REML")

    # ---- fishery delta ----
    f_fit <- data.table::copy(f_r)
    f_fit[, YEAR_F := factor(YEAR)]
    f_fit[, SEX_F  := factor(SEX, levels = c("F","M"))]
    f_fit[, Q_F    := factor(QUARTER, levels = 1:4)]
    f_fit[, AREA_F := factor(AREA_K)]

    n_q_f    <- data.table::uniqueN(f_fit$Q_F)
    n_sex_f  <- data.table::uniqueN(f_fit$SEX_F)
    n_year_f <- data.table::uniqueN(f_fit$YEAR_F)
    n_area_f <- data.table::uniqueN(f_fit$AREA_F)

    # safe backbone prediction for fishery rows (year/area may be new vs survey)
    lev_s <- list(
      SEX_F  = levels(s_fit$SEX_F),
      YEAR_F = levels(s_fit$YEAR_F),
      AREA_F = levels(s_fit$AREA_F)
    )

    predict_backbone_safe <- function(gam_fit, newdata, lev) {
      exclude <- character(0)
      newdata$SEX_F  <- factor(as.character(newdata$SEX_F),  levels = lev$SEX_F)
      newdata$YEAR_F <- factor(as.character(newdata$YEAR_F), levels = lev$YEAR_F)
      if (any(is.na(newdata$YEAR_F))) {
        newdata$YEAR_F[is.na(newdata$YEAR_F)] <- lev$YEAR_F[1]
        exclude <- c(exclude, "s(YEAR_F)")
      }
      newdata$AREA_F <- factor(as.character(newdata$AREA_F), levels = lev$AREA_F)
      if (any(is.na(newdata$AREA_F))) {
        newdata$AREA_F[is.na(newdata$AREA_F)] <- lev$AREA_F[1]
        exclude <- c(exclude, "s(AREA_F)")
      }
      exclude <- unique(exclude)
      tryCatch(
        as.numeric(stats::predict(gam_fit, newdata = newdata, type = "response", exclude = exclude)),
        error = function(e) as.numeric(stats::predict(gam_fit, newdata = newdata, type = "response"))
      )
    }

    nd <- data.frame(
      SEX_F  = as.character(f_fit$SEX_F),
      AGE_G  = f_fit$AGE_G,
      YEAR_F = as.character(f_fit$YEAR),
      AREA_F = as.character(f_fit$AREA_F)
    )
    mu_q3 <- predict_backbone_safe(survey_fit, nd, lev_s)
    f_fit[, MU_Q3 := mu_q3]
    f_fit[, DELTA := LENGTH - MU_Q3]

    # choose k safely for delta smooth
    if (n_q_f >= 2) {
      k_d <- safe_k_by(k_age_delta, f_fit$AGE_G, f_fit$Q_F)
    } else {
      k_d <- safe_k(k_age_delta, data.table::uniqueN(f_fit$AGE_G))
    }

    delta_terms <- c()
    if (n_q_f >= 2) delta_terms <- c(delta_terms, "Q_F")
    if (n_q_f >= 2) {
      delta_terms <- c(delta_terms, sprintf("s(AGE_G, by = Q_F, k = %d)", k_d))
    } else {
      delta_terms <- c(delta_terms, sprintf("s(AGE_G, k = %d)", k_d))
    }
    if (n_sex_f >= 2) delta_terms <- c(delta_terms, "SEX_F")
    if (n_year_f >= 2) delta_terms <- c(delta_terms, "s(YEAR_F, bs = 're')")
    if (n_area_f >= 2) delta_terms <- c(delta_terms, "s(AREA_F, bs = 're')")

    delta_form <- stats::as.formula(paste("DELTA ~", paste(delta_terms, collapse = " + ")))
    delta_fit <- mgcv::gam(delta_form, data = f_fit, method = "REML")

    # ---- priors from fishery ages (region-specific) ----
    prior_pool <- f_r[, .N, by = .(AREA_K, SEX, AGE_G)]
    prior_pool[, P_POOL := N / sum(N), by = .(AREA_K, SEX)]

    prior_global <- f_r[, .N, by = .(SEX, AGE_G)]
    prior_global[, P_GLOB := N / sum(N), by = .(SEX)]

    prior_cell <- f_r[, .N, by = .(YEAR, QUARTER, AREA_K, SEX, AGE_G)]
    prior_cell[, N_CELL := sum(N), by = .(YEAR, QUARTER, AREA_K, SEX)]
    prior_cell[, P_CELL := N / sum(N), by = .(YEAR, QUARTER, AREA_K, SEX)]

    list(
      region = reg,
      maxage = as.integer(maxage),
      min_n_cell = as.integer(min_n_cell),
      prior_mix = as.numeric(prior_mix),
      survey_fit = survey_fit,
      delta_fit  = delta_fit,
      prior_pool = prior_pool,
      prior_global = prior_global,
      prior_cell = prior_cell,
      levels = list(
        survey = list(
          SEX_F  = levels(s_fit$SEX_F),
          YEAR_F = levels(s_fit$YEAR_F),
          AREA_F = levels(s_fit$AREA_F)
        ),
        fishery = list(
          SEX_F  = levels(f_fit$SEX_F),
          YEAR_F = levels(f_fit$YEAR_F),
          AREA_F = levels(f_fit$AREA_F),
          Q_F    = levels(f_fit$Q_F)
        )
      ),
      k_used = list(survey_age_k = k_s, delta_age_k = k_d)
    )
  }

  fits <- lapply(regs, fit_one_region)
  names(fits) <- regs
  fits <- fits[!vapply(fits, is.null, logical(1))]

  if (length(fits) == 0) stop("No regions had sufficient data to fit.", call. = FALSE)

  structure(
    list(
      by_region = fits,
      regions = names(fits),
      maxage = as.integer(maxage),
      min_n_cell = as.integer(min_n_cell),
      prior_mix = as.numeric(prior_mix)
    ),
    class = "age_predictor_by_region"
  )
}
