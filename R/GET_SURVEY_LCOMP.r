#' Get survey length compositions from AKFIN and format for Stock Synthesis
#'
#' Pulls survey length composition data and associated haul/sample counts from
#' the AKFIN database for a specified region and species. Length data may be
#' binned into Stock Synthesis (SS) length bins and optionally formatted into
#' the SS3 length-composition matrix layout (one row per year).
#'
#' @param con_akfin A DBI connection to the AKFIN database.
#' @param species Numeric species code used by the SQL (e.g., 21720).
#' @param bins Numeric vector of SS length-bin lower edges
#'   (e.g., \code{seq(3.5, 119.5, 1)}).
#' @param bin Logical; if TRUE, bin integer lengths into \code{bins} using
#'   \code{BIN_LEN_DATA()}.
#' @param area Character area identifier: one of \code{"GOA"}, \code{"AI"},
#'   \code{"BS"}, \code{"SLOPE"}.
#' @param sex Integer; \code{1} aggregates across sex (supported).
#'   Other values return unformatted long data.
#' @param SS Logical; if TRUE, format output as an SS3 length-comp matrix
#'   (requires \code{sex == 1}).
#' @param seas Integer SS season.
#' @param flt Integer SS fleet/survey index.
#' @param gender Integer SS gender code.
#' @param part Integer SS partition code.
#'
#' @return If \code{SS = TRUE} and \code{sex == 1}, a numeric matrix formatted
#'   for Stock Synthesis. Otherwise, a data.frame/data.table of length
#'   compositions.
#'
#' @export
GET_SURVEY_LCOMP <- function(con_akfin,
                             species = 21720,
                             bins = seq(4.5, 119.5, 5),
                             bin = TRUE,
                             area = "BS",
                             sex = 1,
                             SS = TRUE,
                             seas = 1,
                             flt = 3,
                             gender = 1,
                             part = 0) {

  if (missing(con_akfin) || is.null(con_akfin)) {
    stop("`con_akfin` must be a valid DBI connection to AKFIN.", call. = FALSE)
  }

  # ---- area mapping ----
  area <- toupper(area)
  area_map <- switch(
    area,
    "GOA"   = list(survey = 47,        area_id = 99903),
    "AI"    = list(survey = 52,        area_id = 99904),
    "BS"    = list(survey = c(98,143), area_id = c(99900,99901,99902)),
    "SLOPE" = list(survey = 78,        area_id = 99905),
    stop("Unknown `area`: ", area, ". Use GOA, AI, BS, or SLOPE.", call. = FALSE)
  )

  if (area == "SLOPE") {
    stop("There are no survey size compositions available for SLOPE.", call. = FALSE)
  }

  # ---- haul/sample counts (Nsamp) ----
  Count_sql <- sql_reader("AKFIN_count.sql")
  Count_sql <- sql_filter("IN", species, Count_sql,
                          flag = "-- insert species",
                          value_type = "numeric")
  Count_sql <- sql_filter("IN", area_map$survey, Count_sql,
                          flag = "-- insert survey",
                          value_type = "numeric")

  Count <- sql_run(con_akfin, Count_sql) |>
    data.table::as.data.table() |>
    dplyr::rename_with(toupper)

  if (!all(c("YEAR", "HAULS","LENGTHS") %in% names(Count))) {
    stop("count_AKFIN.sql must return columns YEAR and HAULS.", call. = FALSE)
  }

  # ---- length compositions ----
  lcomp_sql <- sql_reader("length_comp.sql")
  lcomp_sql <- sql_filter("IN", species, lcomp_sql,
                          flag = "-- insert species",
                          value_type = "numeric")
  lcomp_sql <- sql_filter("IN", area_map$area_id, lcomp_sql,
                          flag = "-- insert area_id",
                          value_type = "numeric")

  lcomp <- sql_run(con_akfin, lcomp_sql) |>
    data.table::as.data.table() |>
    dplyr::rename_with(toupper)

  if (!all(c("YEAR", "LENGTH", "TOTAL") %in% names(lcomp))) {
    stop("length_comp.sql must return YEAR, LENGTH, and TOTAL.", call. = FALSE)
  }

  # ---- sex handling ----
  if (identical(sex, 1L) || identical(sex, 1)) {
    lcomp <- lcomp |>
      dplyr::group_by(YEAR, LENGTH) |>
      dplyr::summarise(TOTAL = sum(TOTAL), .groups = "drop") |>
      data.table::as.data.table()
  } else {
    message("Sex-specific SS formatting is not implemented; returning long data.")
    return(lcomp)
  }

  # ---- zero-fill YEAR x LENGTH ----
  len <- 0:(max(bins) + 1)
  yrs <- sort(unique(lcomp$YEAR))
  grid <- data.table::as.data.table(expand.grid(LENGTH = len, YEAR = yrs))

  lcomp <- merge(grid, lcomp, all = TRUE)
  lcomp$TOTAL[is.na(lcomp$TOTAL)] <- 0
  lcomp <- lcomp[order(lcomp$YEAR, lcomp$LENGTH), ]

  # ---- binning and proportions ----
  if (isTRUE(bin)) {
    lcomp <- BIN_LEN_DATA(data.table::as.data.table(lcomp), len_bins = bins)

    lcomp <- stats::aggregate(
      TOTAL ~ BIN + YEAR,
      data = lcomp,
      FUN = sum
    )

    grid2 <- expand.grid(YEAR = sort(unique(lcomp$YEAR)), BIN = bins)
    lcomp <- merge(grid2, lcomp, all.x = TRUE)
    lcomp$TOTAL[is.na(lcomp$TOTAL)] <- 0
    lcomp <- lcomp[order(lcomp$YEAR, lcomp$BIN), ]

    N_TOTAL <- stats::aggregate(TOTAL ~ YEAR, data = lcomp, FUN = sum)
    names(N_TOTAL)[2] <- "T_NUMBER"
    lcomp <- merge(lcomp, N_TOTAL, by = "YEAR", all.x = TRUE)

    lcomp$TOTAL <- ifelse(lcomp$T_NUMBER > 0,
                          lcomp$TOTAL / lcomp$T_NUMBER,
                          0)
  }

  # ---- Stock Synthesis matrix ----
  if (isTRUE(SS)) {
    years <- sort(unique(lcomp$YEAR))

    Nsamp_df <- merge(
      data.frame(YEAR = years),
      Count[, .(YEAR, HAULS)],
      by = "YEAR",
      all.x = TRUE
    )
    Nsamp <- Nsamp_df$HAULS
    Nsamp[is.na(Nsamp)] <- 0

    nbin <- length(bins)
    nyr  <- length(years)

    x <- matrix(ncol = nbin + 6, nrow = nyr)
    x[, 2] <- seas
    x[, 3] <- flt
    x[, 4] <- gender
    x[, 5] <- part
    x[, 6] <- Nsamp

    for (i in seq_len(nyr)) {
      x[i, 1] <- years[i]
      x[i, 7:(nbin + 6)] <- lcomp$TOTAL[lcomp$YEAR == years[i]]
    }

    return(x)
  }

  lcomp
}
