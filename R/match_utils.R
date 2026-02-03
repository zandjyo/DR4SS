#' Fuzzy-match fish tickets to length (or sample) records by vessel and delivery date
#'
#' Performs a "fuzzy" join between a length/sample dataset and a fish ticket dataset
#' using \code{DELIVERING_VESSEL} as the primary key and allowing delivery dates to
#' differ by up to \code{ndays}. For each \code{(DELIVERING_VESSEL, DELIVERY_DATE)}
#' combination in the length data, the function selects the fish ticket record with
#' the smallest absolute date difference (ties resolved by the first minimum found).
#'
#' This helper is intended for workflows where the same landing/sample can appear
#' in multiple systems with small discrepancies in recorded delivery dates (e.g.,
#' landing vs ticket posting dates).
#'
#' @param length_data A data frame containing, at minimum, columns
#'   \code{DELIVERING_VESSEL} and \code{DELIVERY_DATE}. Typically a length or
#'   biological sample dataset.
#' @param Fish_ticket A data frame containing, at minimum, columns
#'   \code{DELIVERING_VESSEL} and \code{DELIVERY_DATE}. Additional columns are
#'   retained and merged onto matched \code{length_data} rows.
#' @param ndays Integer (or numeric) maximum allowed absolute difference between
#'   \code{DELIVERY_DATE} values in days. Matches with \code{date_diff > ndays} are
#'   dropped. Default is \code{7}.
#'
#' @return A data frame with the same rows as \code{length_data}, augmented with
#'   fish ticket fields for matched records. An \code{ID} column (internal ticket row
#'   identifier) and \code{date_diff} (absolute difference in days) are included for
#'   matched rows; unmatched rows have \code{NA} in the appended fields.
#'
#' @details
#' The matching procedure:
#' \enumerate{
#'   \item Subset the two inputs to \code{DELIVERING_VESSEL} and \code{DELIVERY_DATE},
#'         adding a row identifier \code{ID} to the fish ticket table.
#'   \item Join all combinations by \code{DELIVERING_VESSEL}.
#'   \item Compute \code{date_diff = abs(DELIVERY_DATE - DELIVERY_DATE2)}.
#'   \item For each \code{(DELIVERING_VESSEL, DELIVERY_DATE)} in \code{length_data},
#'         keep the fish ticket row with minimum \code{date_diff}.
#'   \item Filter to \code{date_diff <= ndays}.
#'   \item Merge matched ticket attributes back onto \code{length_data}.
#' }
#'
#' @section Assumptions and caveats:
#' \itemize{
#'   \item \code{DELIVERY_DATE} fields must be coercible to \code{Date}.
#'   \item This function chooses at most one ticket per \code{(vessel, date)} from the
#'         length data; if multiple tickets are equally close, the first minimum is used.
#'   \item The function relies on \pkg{dplyr} joins and grouping semantics; ensure the
#'         necessary packages are available/attached in your workflow.
#' }
#'
#' @examples
#' \dontrun{
#' # Match tickets within +/- 3 days
#' out <- fuzzy_dates(length_data = len_df, Fish_ticket = ticket_df, ndays = 3)
#' }
#'
#' @export
fuzzy_dates <- function(length_data, Fish_ticket, ndays = 7) {
  x <- length_data[, c("DELIVERING_VESSEL", "DELIVERY_DATE")]
  y <- Fish_ticket
  y$ID <- 1:nrow(y)
  y2 <- y[, c("ID", "DELIVERING_VESSEL", "DELIVERY_DATE")]
  colnames(y2)[which(names(y2) == "DELIVERY_DATE")] <- "DELIVERY_DATE2"

  # join all combos
  x2 <- dplyr::full_join(x, y2, by = "DELIVERING_VESSEL")

  # just resort the data
  x2 <- x2 %>%
    dplyr::arrange(DELIVERING_VESSEL, DELIVERY_DATE, DELIVERY_DATE2)

  # get absolute difference in date
  x2 <- x2 %>%
    dplyr::mutate(date_diff = abs(as.Date(DELIVERY_DATE) - as.Date(DELIVERY_DATE2)))

  # subset only those that have a match
  x3 <- subset(x2, !is.na(DELIVERY_DATE) & !is.na(DELIVERY_DATE2))

  # only pull the matches with the lowest difference in date
  x4 <- x3 %>%
    dplyr::group_by(DELIVERING_VESSEL, DELIVERY_DATE) %>%
    dplyr::slice(which.min(date_diff))

  # get rid of any matches greater than ndays
  x4 <- subset(x4, date_diff <= ndays)

  # merge back with original length data
  x5 <- merge(x, x4, by = c("DELIVERING_VESSEL", "DELIVERY_DATE"), all.x = TRUE)
  x5.1 <- subset(x5, !is.na(ID))
  x5.1 <- unique(x5.1[, c("DELIVERING_VESSEL", "DELIVERY_DATE", "ID", "date_diff")])

  x6 <- merge(x5.1, y, by = "ID")
  names(x6)[2:3] <- c("DELIVERING_VESSEL", "DELIVERY_DATE")
  x6 <- subset(x6, select = -c(DELIVERY_DATE.y, DELIVERING_VESSEL.y))

  x7 <- merge(length_data, x6, by = c("DELIVERING_VESSEL", "DELIVERY_DATE"), all.x = TRUE)
  colnames(x7)[which(names(x7) == "FISH_TICKET_NO.x")] <- "FISH_TICKET_NO"

  # preserve original naming used downstream (legacy)
  x7$DELIVERY_DATE.y <- x7$DELIVERY_DATE
  x7$DELIVERING_VESSEL.y <- x7$DELIVERING_VESSEL

  return(x7)
}
