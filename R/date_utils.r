#' Calculate AFSC/AKFIN-style week-ending dates (Saturday)
#'
#' Computes the "week-ending date" used in many AFSC/AKFIN workflows, returning
#' a Saturday week-end date for each input date. This implementation follows a
#' historical convention change around 1993 and includes safeguards to prevent
#' week-ending dates from crossing calendar years.
#'
#' @param x A vector of dates or date-times coercible by lubridate to dates
#'   (e.g., \code{Date}, \code{POSIXct}). If \code{x} is not already a Date, it
#'   will be interpreted via lubridate/date coercion as used by
#'   \code{\link[lubridate]{ceiling_date}} and \code{\link[lubridate]{year}}.
#'
#' @return A \code{Date} vector of the same length as \code{x}, giving the
#'   computed week-ending (Saturday) date for each element.
#'
#' @details
#' The algorithm:
#' \enumerate{
#'   \item Computes \code{wed = ceiling_date(x, "week")}.
#'   \item Converts \code{wed} to the Saturday associated with the input date by
#'         adding \code{-1} day for most weekdays and \code{+6} days when the
#'         input date is a Sunday.
#'   \item For years prior to 1993, uses \code{date(wed)} directly (legacy rule).
#'   \item If the resulting week-ending date crosses into a different year than
#'         the input, clamps the week-ending date to December 31 of the input year.
#' }
#'
#' This function is intended to match existing legacy weekly binning behavior in
#' historical data pulls. If you need a simple ISO-like "end of week" calculation
#' without legacy adjustments, use a separate helper instead.
#'
#' @examples
#' \dontrun{
#' library(lubridate)
#' WED(as.Date("1992-12-30"))
#' WED(as.Date(c("2024-01-01", "2024-01-06", "2024-01-07")))
#' }
#'
#' @seealso
#' \code{\link[lubridate]{ceiling_date}},
#' \code{\link[lubridate]{year}},
#' \code{\link[lubridate]{date}}
#'
#' @export
WED <- function(x) {
  y <- data.table::data.table(
    weekday = weekdays(x),
    wed     = lubridate::ceiling_date(x, "week"),
    plus    = ifelse(weekdays(x) %in% c("Sunday"), 6, -1),
    YR      = lubridate::year(x)
  )
  y$next_saturday <- lubridate::date(y$wed) + y$plus
  y[YR < 1993]$next_saturday <- lubridate::date(y[YR < 1993]$wed)
  y$yr2 <- lubridate::year(y$next_saturday)
  y[YR != yr2]$next_saturday <- lubridate::date(paste0(y[YR != yr2]$YR, "-12-31"))
  return(y$next_saturday)
}