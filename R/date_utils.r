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


WED_safe <- function(x) {
  stopifnot(requireNamespace("data.table", quietly = TRUE))
  stopifnot(requireNamespace("lubridate", quietly = TRUE))

  # ---- 1) Coerce x to Date safely ----
  xd <- x

  # If already Date/POSIXt, convert cleanly
  if (inherits(xd, "POSIXt")) {
    xd <- as.Date(xd)
  } else if (!inherits(xd, "Date")) {

    # If numeric, try common encodings
    if (is.numeric(xd)) {
      # guess YYYYMMDD if it looks like 8 digits
      xd_chr <- as.character(xd)
      xd_chr[nchar(xd_chr) == 8] <- sprintf(
        "%s-%s-%s",
        substr(xd_chr[nchar(xd_chr) == 8], 1, 4),
        substr(xd_chr[nchar(xd_chr) == 8], 5, 6),
        substr(xd_chr[nchar(xd_chr) == 8], 7, 8)
      )
      xd <- suppressWarnings(lubridate::ymd(xd_chr, quiet = TRUE))
    } else {
      # character/factor/etc: try a few common orders
      xd_chr <- trimws(as.character(xd))
      xd_chr[xd_chr %in% c("", "NA", "NaN", "NULL")] <- NA_character_

      xd <- suppressWarnings(lubridate::parse_date_time(
        xd_chr,
        orders = c(
          "Y-m-d", "Y/m/d", "Ymd",
          "m/d/Y", "m-d-Y",
          "Y-m-d H:M:S", "Y/m/d H:M:S", "Ymd HMS"
        ),
        tz = "UTC"
      ))
      xd <- as.Date(xd)
    }
  }

  # ---- 2) Compute next Saturday (your intent) ----
  y <- data.table::data.table(
    xdate   = xd,
    weekday = weekdays(xd),
    wed     = lubridate::ceiling_date(xd, "week"),
    plus    = data.table::fifelse(weekdays(xd) == "Sunday", 6L, -1L),
    YR      = lubridate::year(xd)
  )

  # base next_saturday
  y[, next_saturday := as.Date(lubridate::date(wed)) + plus]

  # your special-case rule
  y[YR < 1993 & !is.na(wed), next_saturday := as.Date(lubridate::date(wed))]

  # fix year-crossing: force to Dec 31 of original year
  y[, yr2 := lubridate::year(next_saturday)]
  y[!is.na(YR) & !is.na(yr2) & YR != yr2,
    next_saturday := as.Date(sprintf("%04d-12-31", as.integer(YR)))
  ]

  y$next_saturday
}