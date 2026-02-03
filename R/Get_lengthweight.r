#' Estimate annual length–weight parameters from survey data
#'
#' Pulls domestic and (optionally) foreign survey age–length–weight records,
#' applies standard outlier filtering, fits an overall log–log length–weight
#' relationship, and then estimates annual deviations in the intercept (alpha)
#' and slope (beta) using a generalized additive model (GAM).
#'
#' The resulting annual deviations are returned as environmental time series
#' suitable for use in Stock Synthesis:
#' \itemize{
#'   \item Alpha-series: \code{exp(alpha_year) - exp(alpha_overall)}
#'   \item Beta-series:  \code{beta_year - beta_overall}
#' }
#'
#' SQL is loaded from packaged templates:
#' \itemize{
#'   \item \code{dom_age_wt.sql} (domestic survey data)
#'   \item \code{for_age_wt.sql} (foreign survey data; optional)
#' }
#'
#' Both SQL templates must include placeholder flags:
#' \itemize{
#'   \item \code{-- insert species}
#'   \item \code{-- insert location}
#' }
#'
#' @param con_akfin A DBI connection used to run \code{dom_age_wt.sql}.
#' @param con_afsc Optional DBI connection used to run \code{for_age_wt.sql}.
#'   If \code{NULL}, foreign data are not included.
#' @param species Numeric species code (single value).
#' @param area Character area identifier: one of \code{"BS"}, \code{"AI"}, \code{"GOA"}.
#' @param K Integer basis dimension for cyclic GAM smooths on week-of-year.
#' @param Alpha_series Integer series ID for alpha deviations in the output.
#' @param Beta_series Integer series ID for beta deviations in the output.
#' @param min_year Integer; years prior to this are excluded when estimating the
#'   overall (baseline) length–weight relationship.
#'
#' @return A data.table with columns \code{YEAR}, \code{SERIES}, and \code{VALUE}.
#'
#' @section Statistical rationale:
#' Annual length–weight parameters are extracted by predicting model-based
#' weights over a **fixed, common length grid** shared across all years.
#' This grid is internally defined using the 5th–95th percentiles of observed
#' lengths, ensuring that year-to-year differences in estimated alpha and beta
#' reflect changes in the length–weight relationship itself, rather than
#' differences in the observed size composition among years. This approach
#' produces more stable and comparable time series for use as environmental
#' covariates in Stock Synthesis.
#'
#' @export
get_lengthweight <- function(con_akfin,
                             con_afsc = NULL,
                             species = 202,
                             area = "BS",
                             K = 7,
                             Alpha_series = 2,
                             Beta_series = 3,
                             min_year = 1977) {

  if (missing(con_akfin) || is.null(con_akfin)) {
    stop("`con_akfin` must be a valid DBI connection.", call. = FALSE)
  }
  if (length(species) != 1L || !is.numeric(species)) {
    stop("`species` must be a single numeric species code.", call. = FALSE)
  }

  for (pkg in c("mgcv", "lubridate", "data.table", "dplyr")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Package '", pkg, "' is required for get_lengthweight().", call. = FALSE)
    }
  }

  area <- toupper(area)
  location <- switch(
    area,
    "BS"  = "between 500 and 539",
    "AI"  = "between 540 and 543",
    "GOA" = "between 600 and 700",
    stop("Invalid `area`. Use one of: BS, AI, GOA.", call. = FALSE)
  )

  # ---- Domestic data ----
  dwt <- sql_reader("dom_age_wt.sql")
  dwt <- sql_filter("IN", species, dwt, flag = "-- insert species", value_type = "numeric")
  dwt <- sql_add(location, dwt, flag = "-- insert location")

  data_dom <- sql_run(con_akfin, dwt) |>
    data.table::as.data.table() |>
    dplyr::rename_with(toupper)

  if (!all(c("YEAR", "HAUL_OFFLOAD_DATE", "CRUISE", "LENGTH", "WEIGHT") %in% names(data_dom))) {
    stop("dom_age_wt.sql result missing required columns.", call. = FALSE)
  }

  # ---- Foreign data (optional) ----
  data_for <- NULL
  if (!is.null(con_afsc)) {
    fwt <- sql_reader("for_age_wt.sql")
    fwt <- sql_filter("IN", species, fwt, flag = "-- insert species", value_type = "numeric")
    fwt <- sql_add(location, fwt, flag = "-- insert location")

    data_for <- sql_run(con_afsc, fwt) |>
      data.table::as.data.table() |>
      dplyr::rename_with(toupper)

    data_for[, HAUL_OFFLOAD_DATE := DT]
    data_for[, WEIGHT := INDIV_WEIGHT]

    keep <- intersect(names(data_dom), names(data_for))
    data_for <- data_for[, ..keep]
  }

  data_LW <- if (is.null(data_for)) {
    data_dom
  } else {
    data.table::rbindlist(list(data_dom, data_for), use.names = TRUE, fill = TRUE)
  }

  # ---- Cleaning and transforms ----
  data_LW <- data_LW[!is.na(LENGTH) & !is.na(WEIGHT)]
  data_LW[, `:=`(
    LENGTH = as.numeric(LENGTH),
    WEIGHT = as.numeric(WEIGHT)
  )]

  data_LW <- data_LW[WEIGHT > 0.01 & WEIGHT < 40]

  data_LW[, `:=`(
    logL = log(LENGTH),
    logW = log(WEIGHT),
    YEAR1 = as.numeric(YEAR),
    WEEK1 = lubridate::week(HAUL_OFFLOAD_DATE)
  )]

  # ---- Overall model ----
  lm1 <- stats::lm(logW ~ logL, data = data_LW[YEAR1 >= min_year])
  alpha0 <- coef(lm1)[1]
  beta0  <- coef(lm1)[2]

  # ---- GAM ----
  gam_fit <- mgcv::gam(
    logW ~ YEAR1 * logL +
      mgcv::s(WEEK1, by = logL, bs = "cc", k = K) +
      mgcv::s(WEEK1, bs = "cc", k = K),
    data = data_LW
  )

  # ---- Common length grid (internal, fixed) ----
  L_rng <- stats::quantile(data_LW$LENGTH, probs = c(0.05, 0.95), na.rm = TRUE)
  length_grid <- seq(L_rng[1], L_rng[2], length.out = 50)

  years <- sort(unique(data_LW$YEAR1))
  pred_grid <- data.table::CJ(
    YEAR1 = years,
    WEEK1 = 2:52,
    LENGTH = length_grid
  )
  pred_grid[, logL := log(LENGTH)]

  # bias correction
  sigma_gam <- stats::sigma(gam_fit)
  cf <- (sigma_gam^2) / 2
  pred_grid[, predW := exp(predict(gam_fit, newdata = pred_grid) + cf)]

  # ---- Annual alpha / beta extraction ----
  alpha <- beta <- numeric(length(years))

  for (i in seq_along(years)) {
    fit_i <- lm(log(predW) ~ log(LENGTH),
                data = pred_grid[YEAR1 == years[i]])
    alpha[i] <- coef(fit_i)[1]
    beta[i]  <- coef(fit_i)[2]
  }

  env_alpha <- data.table::data.table(
    YEAR = years,
    SERIES = Alpha_series,
    VALUE = exp(alpha) - exp(alpha0)
  )

  env_beta <- data.table::data.table(
    YEAR = years,
    SERIES = Beta_series,
    VALUE = beta - beta0
  )

  data.table::rbindlist(list(env_alpha, env_beta))
}
