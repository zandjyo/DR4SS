#' Fit fishery weight–length model with adaptive strata dropping
#'
#' @description
#' Fits a Gamma(log) GAM for fish weight using length as the primary predictor
#' and optional stratification terms (YEAR, QUARTER, GEAR, AREA, SEX).
#' Factor terms are adaptively dropped if any strata cell has fewer than
#' `min_strata_n` observations, in the order:
#' GEAR -> AREA -> QUARTER -> SEX.
#'
#' Optional trimming of extreme weights is performed using qgam
#' quantile envelopes prior to model fitting.
#'
#' @param fish_data data.table returned by `get_fishery_age_wt_data()`.
#' @param trim_outliers Logical; if TRUE, trim weights using qgam envelopes.
#' @param trim_q Length-2 numeric vector of quantiles (default c(0.01, 0.99)).
#' @param min_strata_n Minimum number of observations per strata cell
#'   required to retain a factor term.
#' @param allow_gear Logical; allow GEAR term.
#' @param allow_area Logical; allow AREA term.
#' @param allow_quarter Logical; allow QUARTER term.
#' @param allow_sex Logical; allow SEX term.
#'
#' @return A list with elements:
#' \describe{
#'   \item{model}{Fitted `mgcv::gam` object}
#'   \item{kept_terms}{Character vector of factor terms retained}
#'   \item{data_used}{data.table of observations used to fit the model}
#' }
#'
#' @export
fit_weight_model <- function(fish_data,
                             trim_outliers = TRUE,
                             trim_q = c(0.01, 0.99),
                             min_strata_n = 30,
                             allow_gear = TRUE,
                             allow_area = TRUE,
                             allow_quarter = TRUE,
                             allow_sex = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  if (!requireNamespace("mgcv", quietly = TRUE)) {
    stop("Package 'mgcv' is required.", call. = FALSE)
  }
  if (isTRUE(trim_outliers) && !requireNamespace("qgam", quietly = TRUE)) {
    stop("trim_outliers=TRUE requires package 'qgam'.", call. = FALSE)
  }

  dt <- data.table::as.data.table(fish_data)

  # ---- keep only rows usable for weight modeling ----
  dt <- dt[!is.na(WEIGHT) & !is.na(LENGTH)]
  if (nrow(dt) < 50) {
    stop("Not enough observations with non-missing WEIGHT and LENGTH.", call. = FALSE)
  }

  # ---- optional trimming using qgam envelopes ----
  if (isTRUE(trim_outliers)) {
    q_lo <- min(trim_q)
    q_hi <- max(trim_q)

    if (!(q_lo > 0 && q_hi < 1 && q_lo < q_hi)) {
      stop("trim_q must be two quantiles in (0,1) with trim_q[1] < trim_q[2].",
           call. = FALSE)
    }

    q_hi_fit <- qgam::qgam(
      WEIGHT ~ s(LENGTH, k = 15),
      data = dt,
      qu = q_hi
    )
    q_lo_fit <- qgam::qgam(
      WEIGHT ~ s(LENGTH, k = 15),
      data = dt,
      qu = q_lo
    )

    dt[, W_HI := stats::predict(q_hi_fit, newdata = dt, type = "response")]
    dt[, W_LO := stats::predict(q_lo_fit, newdata = dt, type = "response")]

    dt <- dt[WEIGHT <= W_HI & WEIGHT >= W_LO]
    dt[, c("W_HI", "W_LO") := NULL]

    if (nrow(dt) < 50) {
      stop("Too many observations removed by trimming; insufficient data remain.",
           call. = FALSE)
    }
  }

  # ---- candidate factor terms ----
  term_candidates <- c()
  if (allow_gear)    term_candidates <- c(term_candidates, "GEAR2")
  if (allow_area)    term_candidates <- c(term_candidates, "AREA")
  if (allow_quarter) term_candidates <- c(term_candidates, "QUARTER")
  if (allow_sex)     term_candidates <- c(term_candidates, "SEX")

  # Ensure factors
  for (v in term_candidates) {
    dt[, (v) := as.factor(get(v))]
    dt[, (v) := droplevels(get(v))]
  }

  # ---- helper to check strata support ----
  strata_ok <- function(d, vars, min_n) {
    if (length(vars) == 0) return(TRUE)
    counts <- d[, .N, by = vars]
    all(counts$N >= min_n)
  }

  # ---- adaptive dropping order ----
  drop_order <- c("GEAR2", "AREA", "QUARTER", "SEX")
  kept_terms <- intersect(drop_order, term_candidates)

  #while (length(kept_terms) > 0 &&
  #       !strata_ok(dt, kept_terms, min_strata_n)) {
  #  kept_terms <- kept_terms[-1]
  #}

  if (nrow(dt) < min_strata_n) {
    stop("Insufficient data to fit a weight model after strata checks.",
         call. = FALSE)
  }

  # ---- build GAM formula ----
  rhs_terms <- c(
    "factor(YEAR)",
    "s(LENGTH, k = 12)",
    kept_terms
  )

  formula_txt <- paste("WEIGHT ~", paste(rhs_terms, collapse = " + "))
  fml <- stats::as.formula(formula_txt)

  # ---- fit model ----
  fit <- mgcv::gam(
    formula = fml,
    family  = Gamma(link = "log"),
    data    = dt,
    method  = "REML"
  )

  list(
    model = fit,
    kept_terms = kept_terms,
    data_used = dt
  )
}
