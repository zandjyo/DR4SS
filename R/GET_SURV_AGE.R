#' Get survey specimen age data
#'
#' Queries specimen-level survey age data from AKFIN (via the SQL template
#' \code{survey_age.sql}) and returns a table of specimen records. Adds \code{AGE1},
#' which pools all ages \code{>= max_age} into a plus group.
#'
#' Optionally, an external CSV can be provided (via \code{extra_csv}) to fit a
#' correction model (legacy GAM-based approach) and overwrite \code{AGE1} for
#' a subset of years. This replaces the previous hard-coded dependency on
#' \code{"Steves pcod4.csv"} while keeping the ability to use external data.
#'
#' @param con_akfin A DBI connection to the AKFIN database.
#' @param area Character area identifier: one of \code{"GOA"}, \code{"BS"},
#'   \code{"AI"}, \code{"SLOPE"}.
#' @param species Numeric species code (single value or vector).
#' @param start_yr Numeric start year (inclusive) for the AKFIN query.
#' @param max_age Integer maximum age; ages \code{>= max_age} are pooled into \code{max_age}.
#' @param extra_csv Optional path to a CSV file used for external age-correction.
#'   If \code{NULL}, no correction is applied.
#'
#' @return A data.table of specimen records.
#'
#' @export
GET_SURV_AGE<- function(con_akfin,
                             area = "BS",
                             species = 21720,
                             start_yr = 1977,
                             max_age = 10)
{

  if (missing(con_akfin) || is.null(con_akfin)) {
    stop("`con_akfin` must be a valid DBI connection.", call. = FALSE)
  }
  if (!is.numeric(species)) stop("`species` must be numeric.", call. = FALSE)
  if (!is.numeric(start_yr) || length(start_yr) != 1L) stop("`start_yr` must be a single numeric year.", call. = FALSE)
  if (!is.numeric(max_age) || length(max_age) != 1L) stop("`max_age` must be a single numeric value.", call. = FALSE)
  max_age <- as.integer(max_age)

  # ---- map area -> survey_definition_id (numeric) ----
  area <- toupper(area)
  survey <- switch(
    area,
    "GOA"   = 47,
    "BS"    = c(98,143),
    "NBS"   = 143,
    "BS-NBS" = c(98),
    "AI"    = 52,
    "SLOPE" = 78,
    "BS-WGOA" = c(47,98,143),
    stop("Not a valid `area`. Use GOA, BS, NBS, BS-NBS, AI, BS-WGOA, or SLOPE.", call. = FALSE)
  )

  # ---- query using packaged SQL template ----

  sql <- sql_reader("survey_age.sql")
  sql <- sql_filter(sql_precode = ">=", x = start_yr, sql_code = sql,
                    flag = "-- insert start_year", value_type = "numeric")
  sql <- sql_filter(sql_precode = "IN", x = species,  sql_code = sql,
                    flag = "-- insert species", value_type = "numeric")
  sql <- sql_filter(sql_precode = "IN", x = survey,   sql_code = sql,
                    flag = "-- insert survey", value_type = "numeric")

  Age <- sql_run(con_akfin, sql) |>
    data.table::as.data.table()

  names(Age)<-toupper(names(Age))
  
  if(area=='BS-WGOA') { Age[longitude_dd_end <= -158 | longitude_dd_end > 0]}

  Age<-Age[!is.na(AGE)]

  Age$AGE1<-Age$AGE
  Age[AGE1>max_age]$AGE1 <- max_age

  # Standardize column names to uppercase for consistency
  
  return(Age)
}
