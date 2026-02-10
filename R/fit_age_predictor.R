#' Fit age predictor (survey Q3 backbone + fishery quarter delta + priors)
#'
#' Call this once, then pass the returned object to predict_age_from_lf().
#'
#' @param fish_dt Fishery aged data (from get_fishery_age_wt_data()).
#'   Must include YEAR, QUARTER, LENGTH (cm), AGE, SEX (F/M), AREA.
#' @param srv_dt Survey aged data (from GET_SURV_AGE()).
#'   Must include YEAR, LENGTH_MM (mm), AGE, SEX (F/M). All records are Q3.
#' @param maxage Plus-group maximum age.
#' @param min_n_cell Minimum aged fish in a cell to use cell prior (else pooled).
#' @param prior_mix Mixing weight for cell prior vs pooled prior.
#' @return A list with fitted models and prior tables.
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
    # survey: 1=M, 2=F, 3=U
    x0 <- trimws(as.character(x))
    out <- rep(NA_character_, length(x0))
    out[x0 == "1"] <- "M"
    out[x0 == "2"] <- "F"
    out[x0 == "3"] <- "U"
  # if already coded as letters, accept them too
    x1 <- toupper(x0)
    out[is.na(out) & x1 %in% c("M","F","U")] <- x1[is.na(out) & x1 %in% c("M","F","U")]
    out
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
  f <- f[SEX %in% c("F","M")]   # keep only sexed for fitting (optional; you can keep U if desired)

  f <- f[!is.na(YEAR) & QUARTER %in% 1:4 & is.finite(LENGTH) & is.finite(AGE) & SEX %in% c("F","M")]
  f[, AGE_G := pmin(pmax(AGE, 0L), as.integer(maxage))]
  f[, AREA_K := as.character(AREA)]

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
  s <- s[SEX %in% c("F","M")]   # survey backbone should be sex-specific; drop U for fitting

  s <- s[!is.na(YEAR) & is.finite(LENGTH) & is.finite(AGE) & SEX %in% c("F","M")]
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

  # ---- fit survey backbone: L_Q3 | age, sex, (year RE), (area RE) ----
  s_fit <- data.table::copy(s)
  s_fit[, YEAR_F := factor(YEAR)]
  s_fit[, AREA_F := factor(AREA_K)]
  s_fit[, SEX_F  := factor(SEX, levels = c("F","M"))]

  survey_fit <- mgcv::gam(
    LENGTH ~ SEX_F +
      s(AGE_G, by = SEX_F, k = 10) +
      s(YEAR_F, bs = "re") +
      s(AREA_F, bs = "re"),
    data = s_fit,
    method = "REML"
  )

  # ---- fishery quarter delta: DELTA = L_obs - mu_Q3(age,sex,year,area) ----
  f_fit <- data.table::copy(f)
  f_fit[, YEAR_F := factor(YEAR)]
  f_fit[, AREA_F := factor(AREA_K)]
  f_fit[, SEX_F  := factor(SEX, levels = c("F","M"))]
  f_fit[, Q_F    := factor(QUARTER, levels = 1:4)]

  # predict survey backbone for fishery rows (new RE levels contribute ~0)
  mu_q3 <- as.numeric(stats::predict(
    survey_fit,
    newdata = data.frame(
      LENGTH = NA_real_,
      SEX_F  = f_fit$SEX_F,
      AGE_G  = f_fit$AGE_G,
      YEAR_F = factor(f_fit$YEAR, levels = levels(s_fit$YEAR_F)),
      AREA_F = factor(f_fit$AREA_K, levels = levels(s_fit$AREA_F))
    ),
    type = "response"
  ))

  f_fit[, MU_Q3 := mu_q3]
  f_fit[, DELTA := LENGTH - MU_Q3]

  delta_fit <- mgcv::gam(
    DELTA ~ Q_F +
      s(AGE_G, by = Q_F, k = 8) +
      SEX_F +
      s(YEAR_F, bs = "re") +
      s(AREA_F, bs = "re"),
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
    prior_pool = prior_pool,
    prior_global = prior_global,
    prior_cell = prior_cell
  )
}
