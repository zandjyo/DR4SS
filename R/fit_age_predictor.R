#' Fit age predictor (survey Q3 backbone + fishery quarter delta + priors)
#'
#' Robust to single-level factors (YEAR/AREA/QUARTER/SEX) by automatically
#' dropping terms that cannot be estimated (esp. bs="re" random effects).
#'
#' @param fish_dt Fishery aged data. Must include YEAR, QUARTER, LENGTH (cm), AGE, SEX (F/M/U), AREA.
#' @param srv_dt  Survey aged data. Must include YEAR, LENGTH_MM (mm), AGE, SEX (1/2/3 or F/M/U).
#'               All records are Q3.
#' @param maxage  Plus-group maximum age.
#' @param min_n_cell Minimum aged fish in a cell to use cell prior (else pooled).
#' @param prior_mix  Mixing weight for cell prior vs pooled prior.
#' @return list with fitted models + prior tables.
#' @export
fit_age_predictor <- function(fish_dt,
                              srv_dt,
                              maxage = 12L,
                              min_n_cell = 30L,
                              prior_mix = 0.7) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table required.", call. = FALSE)
  if (!requireNamespace("mgcv", quietly = TRUE)) stop("mgcv required.", call. = FALSE)

  DT <- data.table::as.data.table

  standardize_sex_fishery <- function(x) {
    x <- toupper(trimws(as.character(x)))
    x[!x %in% c("M","F","U")] <- NA_character_
    x
  }

  standardize_sex_survey <- function(x) {
    # survey: 1=M, 2=F, 3=U; also accept letters
    x0 <- trimws(as.character(x))
    out <- rep(NA_character_, length(x0))
    out[x0 == "1"] <- "M"
    out[x0 == "2"] <- "F"
    out[x0 == "3"] <- "U"
    x1 <- toupper(x0)
    out[is.na(out) & x1 %in% c("M","F","U")] <- x1[is.na(out) & x1 %in% c("M","F","U")]
    out
  }

  # -----------------------
  # fishery prep
  # -----------------------
  f <- DT(fish_dt); data.table::setnames(f, toupper(names(f)))
  need_f <- c("YEAR","QUARTER","LENGTH","AGE","SEX","AREA")
  miss_f <- setdiff(need_f, names(f))
  if (length(miss_f) > 0) stop("fish_dt missing: ", paste(miss_f, collapse=", "), call. = FALSE)

  suppressWarnings({
    f[, YEAR    := as.integer(as.character(YEAR))]
    f[, QUARTER := as.integer(as.character(QUARTER))]
    f[, LENGTH  := as.numeric(LENGTH)]
    f[, AGE     := as.integer(as.character(AGE))]
  })
  f[, SEX := standardize_sex_fishery(SEX)]
  f <- f[SEX %in% c("F","M")]  # fit sex-specific; U handled at prediction-time
  f <- f[!is.na(YEAR) & QUARTER %in% 1:4 & is.finite(LENGTH) & is.finite(AGE)]
  f[, AGE_G := pmin(pmax(AGE, 0L), as.integer(maxage))]
  f[, AREA_K := as.character(AREA)]

  # -----------------------
  # survey prep (Q3)
  # -----------------------
  s <- DT(srv_dt); data.table::setnames(s, toupper(names(s)))
  if (!"YEAR" %in% names(s) && "HAUL_YEAR" %in% names(s)) data.table::setnames(s, "HAUL_YEAR", "YEAR")

  need_s <- c("YEAR","LENGTH_MM","AGE","SEX")
  miss_s <- setdiff(need_s, names(s))
  if (length(miss_s) > 0) stop("srv_dt missing: ", paste(miss_s, collapse=", "), call. = FALSE)

  suppressWarnings({
    s[, YEAR   := as.integer(as.character(YEAR))]
    s[, LENGTH := as.numeric(LENGTH_MM) / 10]  # mm -> cm
    s[, AGE    := as.integer(as.character(AGE))]
  })
  s[, SEX := standardize_sex_survey(SEX)]
  s <- s[SEX %in% c("F","M")]  # backbone sex-specific
  s <- s[!is.na(YEAR) & is.finite(LENGTH) & is.finite(AGE)]
  s[, AGE_G := pmin(pmax(AGE, 0L), as.integer(maxage))]
  s[, QUARTER := 3L]

  # Survey AREA handling
  if ("NMFS_AREA" %in% names(s)) {
    suppressWarnings(s[, NMFS_AREA := as.integer(as.character(NMFS_AREA))])
    s[, AREA_K := as.character(trunc(NMFS_AREA / 10))]
    s[AREA_K == "50", AREA_K := "51"]
  } else if ("AREA" %in% names(s)) {
    s[, AREA_K := as.character(AREA)]
  } else {
    s[, AREA_K := "ALL"]
  }

  # -----------------------
  # factor scaffolding (stable levels)
  # -----------------------
  # Sex: always create with both levels so prediction grids can use both
  mk_sexF <- function(x) factor(x, levels = c("F","M"))

  # Years/areas: keep whatever is present, but we must avoid bs="re" if only 1 level
  s_fit <- data.table::copy(s)
  s_fit[, YEAR_F := factor(YEAR)]
  s_fit[, AREA_F := factor(AREA_K)]
  s_fit[, SEX_F  := mk_sexF(SEX)]

  f_fit <- data.table::copy(f)
  f_fit[, YEAR_F := factor(YEAR)]
  f_fit[, AREA_F := factor(AREA_K)]
  f_fit[, SEX_F  := mk_sexF(SEX)]
  f_fit[, Q_F    := factor(QUARTER, levels = 1:4)]

  n_year_s <- data.table::uniqueN(s_fit$YEAR_F)
  n_area_s <- data.table::uniqueN(s_fit$AREA_F)
  n_sex_s  <- data.table::uniqueN(s_fit$SEX)   # observed sexes (F/M), not levels()

  n_year_f <- data.table::uniqueN(f_fit$YEAR_F)
  n_area_f <- data.table::uniqueN(f_fit$AREA_F)
  n_q_f    <- data.table::uniqueN(f_fit$Q_F[!is.na(f_fit$Q_F)])
  n_sex_f  <- data.table::uniqueN(f_fit$SEX)

  # -----------------------
  # survey backbone GAM (Q3)
  # -----------------------
  # Base: always include an age smooth (single-sex or sex-specific)
  # If both sexes present -> include SEX_F + s(AGE_G, by=SEX_F)
  # If only one sex -> drop SEX_F and by-smooth, use s(AGE_G)
  survey_terms <- c()
  if (n_sex_s >= 2) {
    survey_terms <- c(survey_terms, "SEX_F", "s(AGE_G, by = SEX_F, k = 10)")
  } else {
    survey_terms <- c(survey_terms, "s(AGE_G, k = 10)")
  }
  if (n_year_s >= 2) survey_terms <- c(survey_terms, "s(YEAR_F, bs = 're')")
  if (n_area_s >= 2) survey_terms <- c(survey_terms, "s(AREA_F, bs = 're')")

  survey_formula <- stats::as.formula(paste("LENGTH ~", paste(survey_terms, collapse = " + ")))

  survey_fit <- mgcv::gam(
    formula = survey_formula,
    data    = s_fit,
    method  = "REML"
  )

  # -----------------------
  # fishery delta GAM
  # -----------------------
  # Compute MU_Q3 for fishery rows; new YEAR/AREA levels not in survey get 0 for REs.
  # IMPORTANT: keep YEAR_F/AREA_F factor levels aligned to survey for prediction.
  newdat_q3 <- data.frame(
    LENGTH = NA_real_,
    SEX_F  = f_fit$SEX_F,
    AGE_G  = f_fit$AGE_G,
    YEAR_F = factor(f_fit$YEAR, levels = levels(s_fit$YEAR_F)),
    AREA_F = factor(f_fit$AREA_K, levels = levels(s_fit$AREA_F))
  )
  mu_q3 <- as.numeric(stats::predict(survey_fit, newdata = newdat_q3, type = "response"))
  f_fit[, MU_Q3 := mu_q3]
  f_fit[, DELTA := LENGTH - MU_Q3]

  delta_terms <- c()

  # Quarter structure:
  # - if >=2 quarters: include Q_F main + s(AGE_G, by=Q_F)
  # - if 1 quarter: no Q_F, just s(AGE_G)
  if (n_q_f >= 2) {
    delta_terms <- c(delta_terms, "Q_F", "s(AGE_G, by = Q_F, k = 8)")
  } else {
    delta_terms <- c(delta_terms, "s(AGE_G, k = 8)")
  }

  # Sex effect in delta:
  # - if >=2 sexes: include SEX_F
  if (n_sex_f >= 2) delta_terms <- c(delta_terms, "SEX_F")

  # Random effects only if >=2 levels
  if (n_year_f >= 2) delta_terms <- c(delta_terms, "s(YEAR_F, bs = 're')")
  if (n_area_f >= 2) delta_terms <- c(delta_terms, "s(AREA_F, bs = 're')")

  delta_formula <- stats::as.formula(paste("DELTA ~", paste(delta_terms, collapse = " + ")))

  delta_fit <- mgcv::gam(
    formula = delta_formula,
    data    = f_fit,
    method  = "REML"
  )

  # -----------------------
  # priors from fishery ages
  # -----------------------
  prior_pool <- f[, .N, by = .(AREA_K, SEX, AGE_G)]
  prior_pool[, P_POOL := N / sum(N), by = .(AREA_K, SEX)]

  prior_global <- f[, .N, by = .(SEX, AGE_G)]
  prior_global[, P_GLOB := N / sum(N), by = .(SEX)]

  prior_cell <- f[, .N, by = .(YEAR, QUARTER, AREA_K, SEX, AGE_G)]
  prior_cell[, N_CELL := sum(N), by = .(YEAR, QUARTER, AREA_K, SEX)]
  prior_cell[, P_CELL := N / sum(N), by = .(YEAR, QUARTER, AREA_K, SEX)]

  list(
    maxage      = as.integer(maxage),
    min_n_cell  = as.integer(min_n_cell),
    prior_mix   = as.numeric(prior_mix),
    survey_fit  = survey_fit,
    delta_fit   = delta_fit,
    prior_pool  = prior_pool,
    prior_global= prior_global,
    prior_cell  = prior_cell,

    # (optional) stash factor levels so prediction code can align them
    .levels = list(
      sex_levels  = c("F","M"),
      survey_year_levels = levels(s_fit$YEAR_F),
      survey_area_levels = levels(s_fit$AREA_F)
    )
  )
}