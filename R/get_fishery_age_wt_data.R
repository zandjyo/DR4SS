#' Pull and standardize fishery age–length–weight data
#'
#' Reads fishery age/length/weight observations using the
#' `dom_age_wt2.sql` query bundled with DR4SS, applies region filters,
#' standardizes fields (YEAR, QUARTER, SEX, GEAR2), and performs basic
#' cleaning suitable for downstream EWAA modeling.
#'
#' @param con A DBI connection to the AKFIN database.
#' @param species Fishery species code used in dom_age_wt2.sql (e.g., 202).
#' @param region Either a character region code
#'   ("AI","BS","GOA","BSWGOA") or a numeric vector of NMFS areas.
#' @param year_min Minimum YEAR to retain (default 2007).
#'
#' @return A data.table with standardized columns:
#' \describe{
#'   \item{YEAR}{Integer year}
#'   \item{MONTH}{Integer month (1–12)}
#'   \item{QUARTER}{Integer quarter (1–4)}
#'   \item{AGE}{Observed age (integer)}
#'   \item{LENGTH}{Length (cm)}
#'   \item{WEIGHT}{Weight (kg; NA for invalid/zero)}
#'   \item{SEX}{Character "F" or "M"}
#'   \item{GEAR}{Original gear code}
#'   \item{GEAR2}{Gear group (1 = other/trawl, 2 = HAL, 3 = POT)}
#'   \item{AREA}{Derived NMFS reporting area (factor)}
#' }
#'
#' @export
get_fishery_age_wt_data <- function(con,
                                    species,
                                    region,
                                    year_min = 2007) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }

  DT <- data.table::as.data.table

  # ---- region → NMFS area mapping ----
  region_map <- list(
    AI     = 540:544,
    BS     = 500:539,
    GOA    = 600:699,
    BSWGOA = c(500:539, 610:620)
  )

  if (is.numeric(region)) {
    area <- sort(unique(as.integer(region)))
  } else {
    reg <- unique(toupper(as.character(region)))
    bad <- setdiff(reg, names(region_map))
    if (length(bad) > 0) {
      stop(
        "Unknown region: ", paste(bad, collapse = ", "),
        ". Allowed: ", paste(names(region_map), collapse = ", "),
        " or provide numeric NMFS areas.",
        call. = FALSE
      )
    }
    area <- sort(unique(unlist(region_map[reg])))
  }

  # ---- read SQL from package ----
  sql_code <- sql_reader("dom_age_wt2.sql")
  sql_code <- sql_filter(
    sql_precode = "IN",
    x = species,
    sql_code = sql_code,
    flag = "-- insert species"
  )
  sql_code <- sql_filter(
    sql_precode = "IN",
    x = area,
    sql_code = sql_code,
    flag = "-- insert location"
  )

  d <- DT(sql_run(con, sql_code))
  if (nrow(d) == 0) stop("Fishery age/weight query returned 0 rows.", call. = FALSE)

  names(d) <- toupper(names(d))

  # ---- required columns ----
  need <- c(
    "YEAR", "MONTH", "AGE",
    "LENGTH", "WEIGHT",
    "GEAR", "NMFS_AREA", "SEX"
  )
  miss <- setdiff(need, names(d))
  if (length(miss) > 0) {
    stop("Missing required columns from dom_age_wt2.sql: ",
         paste(miss, collapse = ", "), call. = FALSE)
  }

  # ---- coerce + basic cleaning ----
  suppressWarnings({
    d[, YEAR   := as.integer(as.character(YEAR))]
    d[, MONTH  := as.integer(as.character(MONTH))]
    d[, AGE    := as.integer(round(as.numeric(AGE)))]
    d[, LENGTH := as.numeric(LENGTH)]
    d[, WEIGHT := as.numeric(WEIGHT)]
    d[, NMFS_AREA := as.integer(as.character(NMFS_AREA))]
  })

  d <- d[YEAR >= year_min]
  d <- d[MONTH %in% 1:12]
  d <- d[!is.na(LENGTH)]

  # Clean weights
  d[WEIGHT == 0 | WEIGHT > 50, WEIGHT := NA_real_]

  # ---- derive AREA ----
  d[, AREA := trunc(NMFS_AREA / 10)]
  d[AREA == 50, AREA := 51]
  d[, AREA := as.factor(AREA)]

  # ---- derive QUARTER ----
  d[, QUARTER := data.table::fifelse(
    MONTH <= 3, 1L,
    data.table::fifelse(
      MONTH <= 6, 2L,
      data.table::fifelse(MONTH <= 9, 3L, 4L)
    )
  )]

  # ---- standardize SEX ----
  d[, SEX := toupper(trimws(as.character(SEX)))]
  d <- d[SEX %in% c("F", "M")]

  # ---- map gear groups (GEAR2) ----
  d[, GEAR := toupper(trimws(as.character(GEAR)))]
  d[, GEAR2 := 1L]
  d[GEAR == 8, GEAR2 := 2L]
  d[GEAR == 6, GEAR2 := 3L]

  data.table::setorder(d, YEAR, MONTH)

  d
}
