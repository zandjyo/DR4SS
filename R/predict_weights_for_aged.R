#' Predict weights for aged fish using a fitted weight model
#'
#' @description
#' Uses a fitted weight GAM (from `fit_weight_model()`) to predict weights for
#' records with observed ages. Factor levels are aligned to the model-fitting
#' data to avoid prediction failures from unseen levels.
#'
#' @param fit_obj Output from `fit_weight_model()` (list with elements `model`,
#'   `kept_terms`, and `data_used`).
#' @param fish_data data.table from `get_fishery_age_wt_data()`.
#' @param maxage Plus-group age (ages > maxage become maxage).
#' @param drop_unseen Logical. If TRUE (default), drop aged rows that have unseen
#'   factor levels for kept terms (e.g., a gear level not present in fit data).
#'   If FALSE, throw an error when unseen levels are encountered.
#'
#' @return A data.table with columns:
#' \describe{
#'   \item{YEAR}{Integer year}
#'   \item{QUARTER}{Integer quarter (1–4)}
#'   \item{AGE_YRS}{Integer age with plus group applied}
#'   \item{SEX}{Character "F"/"M" when available}
#'   \item{GEAR2}{Integer gear group (1/2/3) when available}
#'   \item{WEIGHT_KG}{Predicted weight (kg)}
#'   \item{LENGTH}{Length used for prediction (cm)}
#' }
#'
#' @export
predict_weights_for_aged <- function(fit_obj,
                                     fish_data,
                                     maxage = 10,
                                     drop_unseen = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }

  # basic validation
  if (!is.list(fit_obj) || is.null(fit_obj$model) || is.null(fit_obj$data_used)) {
    stop("fit_obj must be the output of fit_weight_model() (list with model and data_used).",
         call. = FALSE)
  }

  dt_fit <- data.table::as.data.table(fit_obj$data_used)
  dt_all <- data.table::as.data.table(fish_data)

  # require columns needed for prediction
  need <- c("YEAR", "LENGTH", "AGE", "MONTH", "QUARTER")
  miss <- setdiff(need, names(dt_all))
  if (length(miss) > 0) {
    stop("fish_data is missing required columns: ", paste(miss, collapse = ", "), call. = FALSE)
  }

  # subset to aged rows usable for prediction
  A <- dt_all[!is.na(AGE) & !is.na(LENGTH) & !is.na(YEAR)]
  if (nrow(A) == 0L) stop("No aged rows available for prediction.", call. = FALSE)

  # Ensure QUARTER exists (if fish_data didn’t precompute it, try to compute from MONTH)
  if (!"QUARTER" %in% names(A) || all(is.na(A$QUARTER))) {
    if (!"MONTH" %in% names(A)) stop("Need MONTH to derive QUARTER.", call. = FALSE)
    A[, QUARTER := data.table::fifelse(
      MONTH <= 3, 1L,
      data.table::fifelse(MONTH <= 6, 2L,
        data.table::fifelse(MONTH <= 9, 3L, 4L)
      )
    )]
  }

  # Align factor levels for kept terms
  kept <- fit_obj$kept_terms
  if (length(kept) > 0) {

    bad_any <- FALSE
    for (v in kept) {
      # If the term isn't in the aged data, we can't predict with it
      if (!v %in% names(A)) {
        stop("Model includes term '", v, "' but fish_data does not contain column '", v, "'.",
             call. = FALSE)
      }

      # Convert to factor with levels from fit data
      A[, (v) := factor(get(v), levels = levels(dt_fit[[v]]))]

      # Identify unseen levels -> become NA after re-leveling
      bad <- is.na(A[[v]])
      if (any(bad)) {
        bad_any <- TRUE
        if (!isTRUE(drop_unseen)) {
          stop("Unseen levels found in prediction data for term '", v,
               "'. Set drop_unseen=TRUE to drop those rows.", call. = FALSE)
        }
      }
    }

    if (bad_any && isTRUE(drop_unseen)) {
      # drop rows with any NA in kept factor terms
      na_mask <- FALSE
      for (v in kept) na_mask <- na_mask | is.na(A[[v]])
      A <- A[!na_mask]
    }
  }

  if (nrow(A) == 0L) {
    stop("All aged rows were removed due to unseen factor levels; cannot predict.", call. = FALSE)
  }

  # Predict
  A[, WEIGHTP := stats::predict(fit_obj$model, newdata = A, type = "response")]

  # Plus-group ages
  suppressWarnings(A[, AGE_YRS := as.integer(round(as.numeric(AGE)))])
  A[AGE_YRS > maxage, AGE_YRS := maxage]

  # Build standardized output (keep what downstream needs)
  out <- data.table::data.table(
    YEAR     = as.integer(A$YEAR),
    QUARTER  = as.integer(A$QUARTER),
    AGE_YRS  = as.integer(A$AGE_YRS),
    SEX      = if ("SEX" %in% names(A)) toupper(as.character(A$SEX)) else NA_character_,
    GEAR2    = if ("GEAR2" %in% names(A)) as.integer(A$GEAR2) else NA_integer_,
    WEIGHT_KG = as.numeric(A$WEIGHTP),
    LENGTH   = as.numeric(A$LENGTH)
  )

  # remove impossible predictions
  out <- out[is.finite(WEIGHT_KG) & !is.na(WEIGHT_KG)]

  data.table::setorder(out, YEAR, QUARTER, GEAR2, SEX, AGE_YRS)
  out
}
