#' Build a hierarchy of age-length keys (ALKs)
#'
#' Constructs multiple ALKs at progressively broader levels for use
#' in fallback matching when sparse data prevent use of the most
#' specific key.
#'
#' @param alk_raw_df A data.frame or data.table containing raw aged-fish
#'   data with at least YEAR, REGION, SPECIES_CODE, SEX, LENGTH, and AGE.
#'
#' @param plus_age Optional numeric (default = \code{NULL}).
#'   If provided, ages greater than or equal to \code{plus_age} are
#'   collapsed into a plus group before building ALKs.
#'
#' @param length_bins Optional numeric vector (default = \code{NULL}).
#'   If provided, lengths are binned using these breakpoints before
#'   building ALKs.
#'
#' @return A named list of ALKs:
#' \describe{
#'   \item{alk_y_r_s}{ALK by YEAR + REGION + SPECIES_CODE + SEX + LENGTH}
#'   \item{alk_r_s}{ALK by REGION + SPECIES_CODE + SEX + LENGTH}
#'   \item{alk_s}{ALK by SPECIES_CODE + SEX + LENGTH}
#'   \item{alk_all}{ALK by SPECIES_CODE + LENGTH}
#' }
#'
#' @export
build_alk_hierarchy <- function(alk_raw_df,
                                plus_age = NULL,
                                length_bins = NULL) {

  req <- c("YEAR", "REGION", "SPECIES_CODE", "SEX", "LENGTH", "AGE")

  if (!all(req %in% names(alk_raw_df))) {
    stop("alk_raw_df is missing required columns: ",
         paste(setdiff(req, names(alk_raw_df)), collapse = ", "))
  }

  x <- data.table::as.data.table(alk_raw_df)

  # ---- standardize types ----
  data.table::set(x, j = "YEAR", value = as.integer(x[["YEAR"]]))
  data.table::set(x, j = "LENGTH", value = as.numeric(x[["LENGTH"]]))
  data.table::set(x, j = "AGE", value = as.numeric(x[["AGE"]]))

  if (any(is.na(x[["YEAR"]]))) {
    stop("YEAR in alk_raw_df contains missing/non-numeric values.")
  }
  if (any(is.na(x[["LENGTH"]]))) {
    stop("LENGTH in alk_raw_df contains missing/non-numeric values.")
  }
  if (any(is.na(x[["AGE"]]))) {
    stop("AGE in alk_raw_df contains missing/non-numeric values.")
  }

  # ---- optional length binning ----
  if (!is.null(length_bins)) {
    data.table::set(
      x,
      j = "LENGTH",
      value = bin_length_values(x[["LENGTH"]], length_bins = length_bins)
    )
    x <- x[!is.na(x[["LENGTH"]])]
  }

  # ---- optional plus group ----
  if (!is.null(plus_age)) {
    plus_age <- as.numeric(plus_age)
    if (length(plus_age) != 1 || is.na(plus_age) || plus_age < 0) {
      stop("plus_age must be NULL or a single non-negative numeric value.")
    }

    data.table::set(
      x,
      j = "AGE",
      value = ifelse(x[["AGE"]] >= plus_age, plus_age, x[["AGE"]])
    )
  }

  # ---- internal helper ----
  make_alk <- function(dt, group_cols) {
    group_cols_age <- c(group_cols, "AGE")

    counts <- dt[, .(N = .N), by = group_cols_age]
    totals <- counts[, .(TOTAL_N = sum(N)), by = group_cols]
    out <- merge(counts, totals, by = group_cols, all.x = TRUE, sort = FALSE)
    data.table::set(out, j = "ALK_PROP", value = out[["N"]] / out[["TOTAL_N"]])

    data.frame(out)
  }

  list(
    alk_y_r_s = make_alk(x, c("YEAR", "REGION", "SPECIES_CODE", "SEX", "LENGTH")),
    alk_r_s   = make_alk(x, c("REGION", "SPECIES_CODE", "SEX", "LENGTH")),
    alk_s     = make_alk(x, c("SPECIES_CODE", "SEX", "LENGTH")),
    alk_all   = make_alk(x, c("SPECIES_CODE", "LENGTH"))
  )
}