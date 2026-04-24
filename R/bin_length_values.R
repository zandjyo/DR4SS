#' Bin length values using supplied breaks
#'
#' Converts raw length values to bin midpoints using a supplied vector
#' of break points.
#'
#' @param x Numeric vector of lengths.
#' @param length_bins Numeric vector of bin breakpoints.
#'
#' @return Numeric vector of length-bin midpoints.
#' @noRd
#' @export
bin_length_values <- function(x, length_bins) {
  if (is.null(length_bins)) {
    return(as.numeric(x))
  }

  if (!is.numeric(length_bins) || length(length_bins) < 2) {
    stop("length_bins must be NULL or a numeric vector of at least 2 break points.")
  }

  mids <- head(length_bins, -1) + diff(length_bins) / 2

  out <- cut(
    x,
    breaks = length_bins,
    include.lowest = TRUE,
    right = FALSE,
    labels = mids
  )

  as.numeric(as.character(out))
}