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

  # all sampled hauls / locations in requested region
  sql_hauls <- sql_reader("get_all_haul_locations.sql")
  sql_hauls <- sql_filter(
    sql_precode = "IN",
    x = region,
    sql_code = sql_hauls,
    flag = "-- insert region",
    value_type = "character"
  )

  hauls <- DT(sql_run(con_afsc, sql_hauls))

  # species-specific length data
  sql_species <- sql_reader("get_cpue_length_raw.sql")
  sql_species <- sql_filter(
    sql_precode = "IN",
    x = region,
    sql_code = sql_species,
    flag = "-- insert region",
    value_type = "character"
  )
  sql_species <- sql_filter(
    sql_precode = "IN",
    x = species,
    sql_code = sql_species,
    flag = "-- insert species",
    value_type = "numeric"
  )

  dat <- DT(sql_run(con_afsc, sql_species))

  # standardize names
  names(hauls) <- toupper(names(hauls))
  names(dat)   <- toupper(names(dat))

  # type conversions for hauls
  if (nrow(hauls) > 0L) {
    hauls$YEAR      <- as.integer(hauls$YEAR)
    hauls$STATIONID <- hauls$STATIONID
    hauls$HAULJOIN  <- hauls$HAULJOIN
    hauls$MID_LAT   <- as.numeric(hauls$MID_LAT)
    hauls$MID_LON   <- as.numeric(hauls$MID_LON)
  }

  # type conversions for species data
  if (nrow(dat) > 0L) {
    dat$YEAR         <- as.integer(dat$YEAR)
    dat$STATIONID    <- dat$STATIONID
    dat$HAULJOIN     <- dat$HAULJOIN
    dat$MID_LAT      <- as.numeric(dat$MID_LAT)
    dat$MID_LON      <- as.numeric(dat$MID_LON)
    dat$SPECIES_CODE <- as.integer(dat$SPECIES_CODE)
    dat$CPUE_NOKM2   <- as.numeric(dat$CPUE_NOKM2)
    dat$LENGTH       <- as.numeric(dat$LENGTH)
    dat$FREQUENCY    <- as.numeric(dat$FREQUENCY)
  }

  # if no species data, still return all haul locations
  if (nrow(dat) == 0L) {
    hauls$SPECIES_CODE <- as.integer(species[1])
    hauls$CPUE_NOKM2   <- NA_real_
    hauls$LENGTH       <- NA_real_
    hauls$FREQUENCY    <- NA_real_
    return(hauls)
  }

  # join all hauls to species-specific records
  out <- merge(
    x = hauls,
    y = dat,
    by = c("YEAR", "STATIONID", "HAULJOIN", "MID_LAT", "MID_LON"),
    all.x = TRUE,
    suffixes = c("", "_SP")
  )

  out
}