#' Retrieve station CPUE and length-frequency data
#'
#' Pulls haul-level CPUE and specimen length-frequency data from AFSC
#' racebase and GAP products using a parameterized SQL script and returns
#' a standardized \code{data.table} suitable for station-level abundance-
#' at-age calculations.
#'
#' The function injects species and region filters into the SQL using
#' \code{sql_filter} and executes the query via \code{sql_run}.
#'
#' @param con_afsc DBI connection object.
#'   Active Oracle connection to the AFSC/GAP database.
#'
#' @param species Numeric vector (default = 21720).
#'   One or more species codes to retrieve.
#'
#' @param region Character vector (default = c('BS', 'GOA')).
#'   One or more survey regions to filter on (e.g., \code{'BS'},
#'   \code{'GOA'}).
#'
#' @return A \code{data.table} with the following columns:
#' \describe{
#'   \item{YEAR}{Integer survey year.}
#'   \item{REGION}{Character survey region.}
#'   \item{STATIONID}{Station (haul) identifier.}
#'   \item{MID_LAT}{Midpoint latitude of haul.}
#'   \item{MID_LON}{Midpoint longitude of haul.}
#'   \item{SPECIES_CODE}{Numeric species code.}
#'   \item{CPUE_NOKM2}{Numeric CPUE in numbers per km^2.}
#'   \item{SEX}{Sex code.}
#'   \item{LENGTH}{Fish length.}
#'   \item{FREQUENCY}{Length frequency.}
#' }
#'
#' @details
#' The underlying SQL script (\code{get_cpue_len_raw.sql}) must contain
#' flags \code{-- insert species} and \code{-- insert region} where
#' filters will be injected using \code{sql_filter}.
#'
#' Only records from abundance hauls (\code{abundance_haul = 'Y'}) are
#' returned.
#'
#' Output column names are converted to upper case and basic type
#' coercion is applied for consistency in downstream joins and summaries.
#'
#' @export
get_cpue_len_data <- function(con_afsc = con_afsc,
                              species = 21720,
                              region = c("BS", "GOA")) {

  sql_code <- sql_reader("get_cpue_length_raw.sql")

  # ---- inject filters ----
  sql_code <- sql_filter(
    sql_precode = "IN",
    x = region,
    sql_code = sql_code,
    flag = "-- insert region",
    value_type = "character"
  )

  sql_code <- sql_filter(
    sql_precode = "IN",
    x = species,
    sql_code = sql_code,
    flag = "-- insert species",
    value_type = "numeric"
  )

  dat <- DT(sql_run(con_afsc, sql_code))

  if (nrow(dat) == 0L) {
    return(dat)
  }

  # ---- standardize ----
  names(dat) <- toupper(names(dat))

  dat$YEAR         <- as.integer(dat$YEAR)
  dat$STATIONID    <- dat$STATIONID
  dat$HAULJOIN     <- dat$HAULJOIN
  dat$MID_LAT      <- as.numeric(dat$MID_LAT)
  dat$MID_LON      <- as.numeric(dat$MID_LON)
  dat$SPECIES_CODE <- as.integer(dat$SPECIES_CODE)
  dat$CPUE_NOKM2   <- as.numeric(dat$CPUE_NOKM2)
  dat$LENGTH       <- as.numeric(dat$LENGTH)
  dat$FREQUENCY    <- as.numeric(dat$FREQUENCY)

  dat
}