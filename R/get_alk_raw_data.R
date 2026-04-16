#' Retrieve raw aged-fish data for ALK construction
#'
#' Pulls specimen-level age and length data from AFSC racebase using a
#' parameterized SQL script and returns a standardized \code{data.table}
#' suitable for building age-length keys (ALKs).
#'
#' The function injects species and region filters into the SQL using
#' \code{sql_filter} and executes the query via \code{sql_run}.
#'
#' @param con_afsc DBI connection object.
#'   Active Oracle connection to the AFSC racebase database.
#'
#' @param species Numeric vector (default = 21720).
#'   One or more species codes to retrieve.
#'
#' @param region Character vector (default = c('BS','GOA')).
#'   One or more survey regions to filter on (e.g., 'BS', 'GOA').
#'
#' @return A \code{data.table} with the following columns:
#' \describe{
#'   \item{YEAR}{Integer survey year.}
#'   \item{REGION}{Character survey region.}
#'   \item{STATIONID}{Station (haul) identifier.}
#'   \item{MID_LAT}{Midpoint latitude of haul.}
#'   \item{MID_LON}{Midpoint longitude of haul.}
#'   \item{SPECIES_CODE}{Numeric species code.}
#'   \item{SEX}{Sex code.}
#'   \item{LENGTH}{Fish length.}
#'   \item{AGE}{Fish age.}
#' }
#'
#' @details
#' The underlying SQL script (\code{get_alk_raw.sql}) must contain flags
#' \code{-- insert region} and \code{-- insert species} where filters
#' will be injected using \code{sql_filter}.
#'
#' Only records with non-missing ages and \code{abundance_haul = 'Y'}
#' are returned.
#'
#' Output column names are converted to upper case and basic type
#' coercion is applied for consistency in downstream joins.
#'
#' @export
get_alk_raw_data <- function(con_afsc = con_afsc,
                             species =  21720,
                             region = c('BS','GOA')) {
  # ------------------------------------------------------------
  # Retrieve raw aged-fish data using sql_reader
  #
  # Args:
  #   species_code : numeric species code
  #   con          : DB connection
  #   package      : package where SQL file is stored
  #
  # Returns:
  #   data.frame of raw ALK data
  # ------------------------------------------------------------

  species <- as.integer(species)

  sql_code <- sql_reader("get_alk_raw.sql")
 
  # ---- inject filters ----
  
  sql_code <- sql_filter(
    sql_precode  = "IN",
    x            = region,
    sql_code     = sql_code,
    flag         = "-- insert region",
    value_type   = "character"
  )

sql_code <- sql_filter(
    sql_precode  = "IN",
    x            = species,
    sql_code     = sql_code,
    flag         = "-- insert species",
    value_type   = "numeric"
  )


  dat <- DT(sql_run(con_afsc, sql_code))
  if (nrow(dat) == 0L) {
    return(dat)
  }

  # ---- standardize ----
  names(dat) <- toupper(names(dat))

  # Basic type cleanup (important for downstream joins)
  dat$YEAR         <- as.integer(dat$YEAR)
  dat$SPECIES_CODE <- as.integer(dat$SPECIES_CODE)
  dat$LENGTH       <- as.numeric(dat$LENGTH)
  dat$AGE          <- as.numeric(dat$AGE)
  dat$MID_LAT      <- as.numeric(dat$MID_LAT)
  dat$MID_LON      <- as.numeric(dat$MID_LON)

  dat
}