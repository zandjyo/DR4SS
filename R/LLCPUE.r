#' Get longline observer CPUE haul records from AKFIN and create seasonal subsets
#'
#' Runs the packaged SQL template \code{LL_CPUE.sql} against an AKFIN connection
#' (schema \code{council.comprehensive_obs}) to retrieve longline (gear code 8)
#' observer haul records filtered by trip target code(s) and observed species code(s).
#'
#' The function then standardizes key numeric fields, derives date/month fields,
#' and returns both the full dataset and seasonal subsets (spring and fall).
#'
#' The SQL template must contain the placeholder flags:
#' \itemize{
#'   \item \code{-- insert target}  (after \code{trip_target_code})
#'   \item \code{-- insert species} (after \code{obs_specie_code})
#' }
#'
#' This function relies on package utilities \code{sql_reader()}, \code{sql_filter()},
#' and \code{sql_run()} from \code{sql_utils.R}. Place \code{LL_CPUE.sql} in
#' \code{inst/extdata/sql/}.
#'
#' @param con_akfin A DBI connection to the AKFIN database (must have access to
#'   \code{council.comprehensive_obs}).
#' @param target_codes Numeric vector of trip target code(s) to include (length 1+).
#' @param species_codes Numeric vector of observed species code(s) to include (length 1+).
#' @param spring_months Integer months defining "spring" (default \code{c(1,2)}).
#' @param fall_months Integer months defining "fall" (default \code{c(9,10)}).
#'
#' @return A named list with elements:
#' \describe{
#'   \item{all}{All filtered haul records (data.table; uppercased column names).}
#'   \item{spring}{Subset where \code{MONTH} in \code{spring_months}.}
#'   \item{fall}{Subset where \code{MONTH} in \code{fall_months}.}
#' }
#'
#' Added/standardized columns:
#' \itemize{
#'   \item \code{DATE1}: formatted haul date (\code{mm/dd/YYYY})
#'   \item \code{MONTH}: numeric month from \code{HAUL_DATE}
#' }
#'
#' @export
get_LLCPUE <- function(con_akfin,
                       target_codes,
                       species_codes,
                       spring_months = c(1L, 2L),
                       fall_months = c(9L, 10L)) {

  # ---- input checks ----
  if (missing(con_akfin) || is.null(con_akfin)) {
    stop("`con_akfin` must be a valid DBI connection.", call. = FALSE)
  }
  if (missing(target_codes) || length(target_codes) < 1L || !is.numeric(target_codes)) {
    stop("`target_codes` must be a non-empty numeric vector.", call. = FALSE)
  }
  if (missing(species_codes) || length(species_codes) < 1L || !is.numeric(species_codes)) {
    stop("`species_codes` must be a non-empty numeric vector.", call. = FALSE)
  }

  # ---- pull data ----
  sql <- sql_reader("LL_CPUE.sql")

  sql <- sql_filter(
    sql_precode = "IN",
    x = target_codes,
    sql_code = sql,
    flag = "-- insert target",
    value_type = "numeric"
  )

  sql <- sql_filter(
    sql_precode = "IN",
    x = species_codes,
    sql_code = sql,
    flag = "-- insert species",
    value_type = "numeric"
  )

  CPUE <- sql_run(con_akfin, sql) |>
    data.table::as.data.table()

  # standardize names
  data.table::setnames(CPUE, toupper(names(CPUE)))

  # ---- required columns ----
  req <- c(
    "HAUL_DATE",
    "RETRIEVAL_LATITUDE_DD",
    "RETRIEVAL_LONGITUDE_DD",
    "TOTAL_HOOKS_POTS",
    "EXTRAPOLATED_WEIGHT",
    "EXTRAPOLATED_NUMBER"
  )
  miss <- setdiff(req, names(CPUE))
  if (length(miss) > 0) {
    stop("get_LLCPUE(): missing required columns from SQL result: ",
         paste(miss, collapse = ", "),
         call. = FALSE)
  }

  # ---- type coercion ----
  num_cols <- c(
    "RETRIEVAL_LATITUDE_DD",
    "RETRIEVAL_LONGITUDE_DD",
    "TOTAL_HOOKS_POTS",
    "EXTRAPOLATED_WEIGHT",
    "EXTRAPOLATED_NUMBER"
  )
  for (nm in num_cols) {
    CPUE[, (nm) := suppressWarnings(as.numeric(get(nm)))]
  }

  # ---- derived date fields ----
  CPUE[, DATE1 := format(as.Date(HAUL_DATE), "%m/%d/%Y")]
  CPUE[, MONTH := lubridate::month(HAUL_DATE)]

  # ---- seasonal splits ----
  CPUE_SPRING <- CPUE[MONTH %in% spring_months]
  CPUE_FALL   <- CPUE[MONTH %in% fall_months]

  list(
    all = CPUE[],
    spring = CPUE_SPRING[],
    fall = CPUE_FALL[]
  )
}


