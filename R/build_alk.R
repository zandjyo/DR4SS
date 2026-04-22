#' Build age-length key (ALK) from raw specimen data
#'
#' Constructs an age-length key (ALK) by calculating the conditional
#' probability of age given length within strata defined by
#' YEAR, REGION, SPECIES_CODE, and SEX.
#'
#' The function aggregates raw specimen-level data to counts-at-age
#' within each length bin and converts these to proportions
#' \eqn{P(\text{AGE} \mid \text{LENGTH})}.
#'
#' @param alk_raw_df A data.frame or data.table containing raw aged-fish
#'   data with at least the following columns:
#'   \code{YEAR}, \code{REGION}, \code{SPECIES_CODE},
#'   \code{SEX}, \code{LENGTH}, and \code{AGE}.
#'
#' @param plus_age Optional numeric (default = \code{NULL}).
#'   If provided, all ages greater than or equal to \code{plus_age}
#'   are grouped into a plus group prior to constructing the ALK.
#'
#' @param length_bins Optional numeric vector (default = \code{NULL}).
#'   If provided, fish lengths are binned using these breakpoints before
#'   constructing the ALK. Output \code{LENGTH} values will be the bin
#'   midpoints.
#'
#' @return A data.frame with the following columns:
#' \describe{
#'   \item{YEAR}{Integer survey year.}
#'   \item{REGION}{Survey region.}
#'   \item{SPECIES_CODE}{Species code.}
#'   \item{SEX}{Sex category.}
#'   \item{LENGTH}{Length bin midpoint or original length value.}
#'   \item{AGE}{Age (or plus group if specified).}
#'   \item{N}{Number of fish at age within length bin.}
#'   \item{TOTAL_N}{Total number of fish in length bin.}
#'   \item{ALK_PROP}{Proportion at age given length.}
#' }
#'
#' @details
#' If \code{length_bins} is supplied, raw lengths are first assigned to
#' bins and the ALK is then constructed on the binned lengths.
#'
#' @export
build_alk <- function(alk_raw_df,
                      plus_age = NULL,
                      length_bins = NULL) {
  req <- c("YEAR", "REGION", "SPECIES_CODE", "SEX", "LENGTH", "AGE")

  if (!all(req %in% names(alk_raw_df))) {
    stop("alk_raw_df is missing required columns: ",
         paste(setdiff(req, names(alk_raw_df)), collapse = ", "))
  }

  x <- alk_raw_df

  x$YEAR   <- as.integer(x$YEAR)
  x$LENGTH <- as.numeric(x$LENGTH)
  x$AGE    <- as.numeric(x$AGE)

  if (any(is.na(x$YEAR))) stop("YEAR in alk_raw_df contains missing/non-numeric values.")
  if (any(is.na(x$LENGTH))) stop("LENGTH in alk_raw_df contains missing/non-numeric values.")
  if (any(is.na(x$AGE))) stop("AGE in alk_raw_df contains missing/non-numeric values.")

  if (!is.null(length_bins)) {
    x$LENGTH <- bin_length_values(x$LENGTH, length_bins = length_bins)
    x <- x[!is.na(x$LENGTH), , drop = FALSE]
  }

  if (!is.null(plus_age)) {
    plus_age <- as.numeric(plus_age)
    if (length(plus_age) != 1 || is.na(plus_age) || plus_age < 0) {
      stop("plus_age must be NULL or a single non-negative numeric value.")
    }
    x$AGE <- ifelse(x$AGE >= plus_age, plus_age, x$AGE)
  }

  alk_counts <- aggregate(
    list(N = rep(1, nrow(x))),
    by = list(
      YEAR = x$YEAR,
      REGION = x$REGION,
      SPECIES_CODE = x$SPECIES_CODE,
      SEX = x$SEX,
      LENGTH = x$LENGTH,
      AGE = x$AGE
    ),
    FUN = sum
  )

  alk_totals <- aggregate(
    N ~ YEAR + REGION + SPECIES_CODE + SEX + LENGTH,
    data = alk_counts,
    sum
  )
  names(alk_totals)[names(alk_totals) == "N"] <- "TOTAL_N"

  alk <- merge(
    alk_counts,
    alk_totals,
    by = c("YEAR", "REGION", "SPECIES_CODE", "SEX", "LENGTH"),
    all.x = TRUE
  )

  alk$ALK_PROP <- alk$N / alk$TOTAL_N

  alk <- alk[order(alk$YEAR, alk$REGION, alk$SPECIES_CODE, alk$SEX, alk$LENGTH, alk$AGE), ]

  rownames(alk) <- NULL
  alk
}