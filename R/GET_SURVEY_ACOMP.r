#' Get survey age compositions and format for Stock Synthesis
#'
#' Pulls survey age composition data from the AKFIN database (default) and formats
#' it into the Stock Synthesis age-composition matrix layout. Optionally, age
#' compositions can be sourced from a provided VAST age-composition table instead
#' of querying AKFIN.
#'
#' This function preserves legacy behavior from historical assessment scripts:
#' \itemize{
#'   \item Ages greater than or equal to \code{max_age} are pooled into a plus group.
#'   \item For Bering Sea (\code{area = "BS"}), an additional AKFIN query is run for
#'         \code{area_id = 99901}, \code{survey = 98}, and \code{YEAR <= 1986}, and
#'         appended to the main result set.
#'   \item When \code{use_vast = TRUE}, records are filtered to \code{Region == "Both"}
#'         and \code{Year != 2020} to match legacy processing.
#' }
#'
#' @param con_akfin A DBI connection to the AKFIN database. Required when
#'   \code{use_vast = FALSE}.
#' @param use_vast Logical; if TRUE, use \code{vast_agecomp} instead of querying AKFIN.
#' @param vast_agecomp A data.frame/data.table containing VAST age compositions.
#'   Expected columns include \code{Region}, \code{Year}, and age columns (0,1,2,...).
#'   Only used when \code{use_vast = TRUE}.
#' @param species Numeric species code.
#' @param start_yr Numeric start year (used when querying AKFIN: \code{YEAR >= start_yr}).
#' @param area Character; one of \code{"GOA"}, \code{"AI"}, \code{"BS"}, \code{"SLOPE"}.
#' @param max_age Integer maximum age (plus group). Ages >= \code{max_age} are pooled.
#' @param seas Integer SS season.
#' @param flt Integer SS fleet/survey index.
#' @param gender Integer SS gender code.
#' @param part Integer SS partition code.
#' @param ageerr Integer SS age error definition index.
#' @param lgin_lo Integer SS Lbin_lo (legacy field in SS agecomp block).
#' @param lgin_hi Integer SS Lbin_hi (legacy field in SS agecomp block).
#'
#' @return A data.frame formatted for the Stock Synthesis age-composition data block.
#'   Columns are: YEAR, Seas, FltSvy, Gender, Part, Ageerr, Lgin_lo, Lgin_hi, Nsamp, F0..Fmax_age.
#'
#' @export
GET_SURVEY_ACOMP <- function(con_akfin = NULL,
                             use_vast = FALSE,
                             vast_agecomp = NULL,
                             species,
                             start_yr,
                             area = "BS",
                             max_age = 12,
                             seas = 1,
                             flt = 2,
                             gender = 1,
                             part = 0,
                             ageerr = 0,
                             lgin_lo = 1,
                             lgin_hi = 120) {

  if (!isTRUE(use_vast)) {
    if (is.null(con_akfin)) stop("`con_akfin` must be provided when use_vast = FALSE.", call. = FALSE)
  } else {
    if (is.null(vast_agecomp)) stop("`vast_agecomp` must be provided when use_vast = TRUE.", call. = FALSE)
  }
  if (missing(species) || length(species) != 1L || !is.numeric(species)) {
    stop("`species` must be a single numeric species code.", call. = FALSE)
  }
  if (missing(start_yr) || length(start_yr) != 1L || !is.numeric(start_yr)) {
    stop("`start_yr` must be a single numeric year.", call. = FALSE)
  }
  if (length(max_age) != 1L || !is.numeric(max_age) || max_age < 0) {
    stop("`max_age` must be a single non-negative numeric value.", call. = FALSE)
  }
  max_age <- as.integer(max_age)

  # ---- area mapping (numeric IDs) ----
  area <- toupper(area)
  area_map <- switch(
    area,
    "GOA"   = list(survey = 47,        area_id = 99903),
    "AI"    = list(survey = 52,        area_id = 99904),
    "BS"    = list(survey = c(98,143), area_id = c(99900,99902)),
    "SLOPE" = list(survey = 78,        area_id = 99905),
    stop("Unknown `area`: ", area, ". Use GOA, AI, BS, or SLOPE.", call. = FALSE)
  )

  if (area == "SLOPE") {
    stop("There are no age compositions worked up for SLOPE in the database.", call. = FALSE)
  }

  # ---- helper: plus-group ages >= max_age ----
  .apply_plus_group <- function(dt, max_age) {
    dt <- data.table::as.data.table(dt)
    if (!all(c("YEAR", "AGE", "AGEPOP") %in% names(dt))) {
      stop("Agecomp table must contain YEAR, AGE, and AGEPOP.", call. = FALSE)
    }

    dt1 <- dt[AGE < max_age]
    dt2 <- dt[AGE >= max_age]

    if (nrow(dt2) > 0) {
      dt2[, AGE := max_age]
      dt2 <- dt2[, .(AGEPOP = sum(AGEPOP)), by = .(YEAR, AGE)]
      dt <- data.table::rbindlist(list(dt1, dt2), use.names = TRUE)
    } else {
      dt <- dt1
    }

    data.table::setorder(dt, YEAR, AGE)
    dt
  }

  # ---- Nsamp (haul/sample counts) from AKFIN ----
  Count_sql <- sql_reader("count_AKFIN.sql")
  Count_sql <- sql_filter("IN", species, Count_sql, flag = "-- insert species", value_type = "numeric")
  Count_sql <- sql_filter("IN", area_map$survey, Count_sql, flag = "-- insert survey", value_type = "numeric")

  Count <- sql_run(con_akfin, Count_sql) |>
    data.table::as.data.table() |>
    dplyr::rename_with(toupper)

  if (!all(c("YEAR", "HAULS") %in% names(Count))) {
    stop("count_AKFIN.sql must return YEAR and HAULS.", call. = FALSE)
  }

  # ---- build Acomp either from AKFIN query or VAST ----
  if (!isTRUE(use_vast)) {

    Age_sql <- sql_reader("survey_agecomp.sql")
    Age_sql <- sql_filter("IN",  area_map$area_id, Age_sql, flag = "-- insert area_id",  value_type = "numeric")
    Age_sql <- sql_filter("=",   species,          Age_sql, flag = "-- insert species",  value_type = "numeric")
    Age_sql <- sql_filter(">=",  start_yr,         Age_sql, flag = "-- insert start_year", value_type = "numeric")

    Acomp <- sql_run(con_akfin, Age_sql) |>
      data.table::as.data.table() |>
      dplyr::rename_with(toupper)

    # Expect YEAR, AGE, AGEPOP
    if (!all(c("YEAR", "AGE", "AGEPOP") %in% names(Acomp))) {
      stop("survey_agecomp.sql must return YEAR, AGE, and AGEPOP.", call. = FALSE)
    }

    # legacy BS add-on (pre-1987 area_id 99901, survey 98, YEAR <= 1986)
    if (area == "BS") {
      Age_sql2 <- sql_reader("survey_agecomp.sql")
      Age_sql2 <- sql_filter("IN",  99901,   Age_sql2, flag = "-- insert area_id", value_type = "numeric")
      Age_sql2 <- sql_filter("=",   species, Age_sql2, flag = "-- insert species", value_type = "numeric")
      Age_sql2 <- sql_filter("<=",  1986,    Age_sql2, flag = "-- insert start_year", value_type = "numeric")

      Acomp2 <- sql_run(con_akfin, Age_sql2) |>
        data.table::as.data.table() |>
        dplyr::rename_with(toupper)

      if (nrow(Acomp2) > 0) {
        Acomp <- data.table::rbindlist(list(Acomp2, Acomp), use.names = TRUE, fill = TRUE)
      }
    }

    # plus group + fill grid
    Acomp <- .apply_plus_group(Acomp, max_age)

    YR <- sort(unique(Acomp$YEAR))
    grid <- data.table::as.data.table(expand.grid(AGE = 0:max_age, YEAR = YR))
    Acomp <- merge(grid, Acomp, all.x = TRUE)
    Acomp$AGEPOP[is.na(Acomp$AGEPOP)] <- 0
    data.table::setorder(Acomp, YEAR, AGE)

  } else {

    Proportions <- data.table::as.data.table(vast_agecomp)

    # legacy filter
    if (all(c("Region", "Year") %in% names(Proportions))) {
      Proportions <- Proportions[Region == "Both" & Year != 2020]
    } else {
      stop("vast_agecomp must include columns Region and Year (legacy expectation).", call. = FALSE)
    }

    YR <- sort(unique(Proportions$Year))

    # assume first two columns are Region, Year; remaining are age columns (as in your script)
    n <- ncol(Proportions) - 2L
    AGECOMP <- Proportions[, 2:n, with = FALSE]  # keep legacy structure

    # ensure we do not exceed available ages
    max_available <- ncol(AGECOMP) - 1L
    if (max_age > max_available) max_age <- max_available

    MAGE <- max_age + 1L

    # pool plus group if needed
    if (ncol(AGECOMP) > MAGE) {
      AGECOMP1 <- AGECOMP[, 1:MAGE, with = FALSE]
      AGECOMP_plus <- AGECOMP[, (MAGE + 1L):ncol(AGECOMP), with = FALSE]
      AGEP <- rowSums(AGECOMP_plus)
      AGECOMP1[[MAGE]] <- AGECOMP1[[MAGE]] + AGEP
      AGECOMP <- AGECOMP1
    }

    AGECOMP$YEAR <- Proportions$Year
    names(AGECOMP) <- c(0:max_age, "YEAR")

    Acomp <- data.table::melt(AGECOMP, id.vars = "YEAR", variable.name = "AGE", value.name = "AGEPOP")
    Acomp[, AGE := as.integer(as.character(AGE))]
    data.table::setorder(Acomp, YEAR, AGE)
  }

  # ---- build SS output ----
  years <- sort(unique(Acomp$YEAR))

  # align Nsamp deterministically by YEAR
  Nsamp_df <- merge(
    data.frame(YEAR = years),
    data.frame(YEAR = Count$YEAR, HAULS = Count$HAULS),
    by = "YEAR",
    all.x = TRUE
  )
  Nsamp <- Nsamp_df$HAULS
  Nsamp[is.na(Nsamp)] <- 0

  FIN <- (max_age + 1L) + 9L  # 9 fixed cols + (max_age+1) age bins
  y <- matrix(ncol = FIN, nrow = length(years))
  SS_out <- as.data.frame(y)

  names(SS_out) <- c(
    "YEAR", "Seas", "FltSvy", "Gender", "Part", "Ageerr", "Lgin_lo", "Lgin_hi", "Nsamp",
    paste0("F", 0:max_age)
  )

  SS_out$YEAR <- years
  SS_out$Seas <- seas
  SS_out$FltSvy <- flt
  SS_out$Gender <- gender
  SS_out$Part <- part
  SS_out$Ageerr <- ageerr
  SS_out$Lgin_lo <- lgin_lo
  SS_out$Lgin_hi <- lgin_hi
  SS_out$Nsamp <- Nsamp

  for (i in seq_along(years)) {
    SS_out[i, 10:FIN] <- Acomp$AGEPOP[Acomp$YEAR == years[i]]
  }

  SS_out
}

# Backward-compatible alias (your original function name)
#' @export
GET_ACOMP <- GET_SURVEY_ACOMP
