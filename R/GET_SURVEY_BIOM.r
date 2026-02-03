#' Get survey biomass time series from AKFIN
#'
#' Pulls survey biomass (or related survey index output) from the AKFIN database
#' using the SQL template \code{survey_biom.sql}. Supports GOA/AI/BS/SLOPE area
#' mappings consistent with legacy assessment scripts.
#'
#' For Bering Sea (\code{area = "BS"}), this function preserves a legacy special case:
#' it additionally queries pre-1987 data for \code{survey = 98} and \code{area_id = 99901}
#' and appends those rows to the main results.
#'
#' @param con_akfin A DBI connection to the AKFIN database.
#' @param area Character; one of \code{"GOA"}, \code{"AI"}, \code{"BS"}, \code{"SLOPE"}.
#' @param species Numeric species code.
#' @param start_yr Numeric start year (used in SQL via placeholder \code{-- insert start_year}).
#'
#' @return A data.table with column names uppercased, as returned by the SQL query(ies).
#'
#' @export
GET_SURVEY_BIOM <- function(con_akfin,
                            area = "BS",
                            species,
                            start_yr) {

  if (missing(con_akfin) || is.null(con_akfin)) {
    stop("`con_akfin` must be a valid DBI connection to AKFIN.", call. = FALSE)
  }
  if (missing(species) || length(species) != 1L || !is.numeric(species)) {
    stop("`species` must be a single numeric species code.", call. = FALSE)
  }
  if (missing(start_yr) || length(start_yr) != 1L || !is.numeric(start_yr)) {
    stop("`start_yr` must be a single numeric year.", call. = FALSE)
  }

  # ---- area mapping (numeric IDs) ----
  area <- toupper(area)
  area_map <- switch(
    area,
    "GOA"   = list(survey = 47,        area_id = 99903),
    "AI"    = list(survey = 52,        area_id = 99904),
    "BS"    = list(survey = c(98,143), area_id = c(99900, 99902)),
    "SLOPE" = list(survey = 78,        area_id = 99905),
    stop("Unknown `area`: ", area, ". Use GOA, AI, BS, or SLOPE.", call. = FALSE)
  )

  # ---- main query ----
  pop <- sql_reader("survey_biom.sql")
  pop <- sql_filter(sql_precode = "IN",  x = area_map$survey,  sql_code = pop,
                    flag = "-- insert survey", value_type = "numeric")
  pop <- sql_filter(sql_precode = "IN",  x = area_map$area_id, sql_code = pop,
                    flag = "-- insert area_id", value_type = "numeric")
  pop <- sql_filter(sql_precode = ">=",  x = start_yr,         sql_code = pop,
                    flag = "-- insert start_year", value_type = "numeric")
  pop <- sql_filter(sql_precode = "=",   x = species,          sql_code = pop,
                    flag = "-- insert species", value_type = "numeric")

  BIOM <- sql_run(con_akfin, pop) |>
    data.table::as.data.table() |>
    dplyr::rename_with(toupper)

  # ---- legacy BS special case: append pre-1987 (survey 98, area_id 99901) ----
  if (area == "BS") {
    pop1 <- sql_reader("survey_biom.sql")
    pop1 <- sql_filter(sql_precode = "IN", x = 98,     sql_code = pop1,
                       flag = "-- insert survey", value_type = "numeric")
    pop1 <- sql_filter(sql_precode = "IN", x = 99901,  sql_code = pop1,
                       flag = "-- insert area_id", value_type = "numeric")
    pop1 <- sql_filter(sql_precode = "<",  x = 1987,   sql_code = pop1,
                       flag = "-- insert start_year", value_type = "numeric")
    pop1 <- sql_filter(sql_precode = "=",  x = species, sql_code = pop1,
                       flag = "-- insert species", value_type = "numeric")

    BIOM2 <- sql_run(con_akfin, pop1) |>
      data.table::as.data.table() |>
      dplyr::rename_with(toupper)

    BIOM <- data.table::rbindlist(list(BIOM2, BIOM), use.names = TRUE, fill = TRUE)
  }

  BIOM
}
