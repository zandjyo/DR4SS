#' Pull domestic blend catch (COUNCIL.COMPREHENSIVE_BLEND_CA) for catch weighting
#'
#' @description
#' Runs the packaged blend-catch SQL (COMPREHENSIVE_BLEND_CA) and returns catch
#' aggregated by YEAR x MONTH_WED x GEAR (and other fields included in the SQL).
#' This is typically used to build quarterly catch proportions for catch-weighted EWAA.
#'
#' @param con A DBI connection to the COUNCIL/AKFIN Oracle database containing
#'   COUNCIL.COMPREHENSIVE_BLEND_CA.
#' @param species Species group code(s) for `SPECIES_GROUP_CODE` (numeric/integer).
#' @param subarea One or more FMP subarea values for `FMP_SUBAREA` (often character like "BS").
#' @param year_max Maximum year to include (inclusive).
#' @param year_min Optional minimum year to include (inclusive). Default NULL (no lower bound).
#' @param sql_file Name of the SQL file bundled with the package.
#'
#' @return A data.table with (at minimum) columns:
#'   YEAR, MONTH_WED, GEAR, TONS, RETAINED_OR_DISCARDED, SPECIES, AREA, SOURCE.
#'
#' @export
get_blend_catch <- function(con,
                            species_group,
                            subarea,
                            year_max,
                            sql_file = "dom_catch_table.sql") {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }

  DT <- data.table::as.data.table

  # ---- read SQL from package ----
  sql_code <- sql_reader(sql_file)

  # ---- inject filters ----
  # subarea can be character (e.g., "BS") or numeric; pass type explicitly
  subarea_type <- if (is.numeric(subarea)) "numeric" else "character"

  sql_code <- sql_filter(
    sql_precode  = "IN",
    x            = subarea,
    sql_code     = sql_code,
    flag         = "-- insert subarea",
    value_type   = subarea_type
  )

  # years: use <= year_max 
  # Prefer character if YEAR is stored as character; numeric if stored numeric.
  # We can safely use numeric literal in most Oracle schemas where YEAR is NUMBER.

  sql_code <- sql_filter(
    sql_precode = "<=",
    x           = year_max,
    sql_code    = sql_code,
    flag        = "-- insert years",
    value_type  = "numeric"
  )

  sql_code <- sql_filter(
    sql_precode = "IN",
    x           = species_group,
    sql_code    = sql_code,
    flag        = "-- insert species_catch",
    value_type  = "character"
  )

  # ---- run ----
  d <- DT(sql_run(con, sql_code))
  if (nrow(d) == 0L) {
    return(d)
  }

  # ---- standardize ----
  names(d) <- toupper(names(d))

  # expected columns from your query
  # SPECIES, RETAINED_OR_DISCARDED, TONS, MONTH_WED, GEAR, YEAR, AREA, SOURCE
  # coerce basic types
  suppressWarnings({
    if ("YEAR" %in% names(d))      d[, YEAR := as.integer(as.character(YEAR))]
    if ("MONTH_WED" %in% names(d)) d[, MONTH_WED := as.integer(as.character(MONTH_WED))]
    if ("TONS" %in% names(d))      d[, TONS := as.numeric(TONS)]
  })

  # basic sanity filters
  if ("MONTH_WED" %in% names(d)) d <- d[MONTH_WED %in% 1:12]
  if ("TONS" %in% names(d))      d <- d[is.finite(TONS) & !is.na(TONS)]

  if ("GEAR" %in% names(d)) d[, GEAR := toupper(trimws(as.character(GEAR)))]
  if ("RETAINED_OR_DISCARDED" %in% names(d)) {
    d[, RETAINED_OR_DISCARDED := toupper(trimws(as.character(RETAINED_OR_DISCARDED)))]
  }

  data.table::setorder(d, YEAR, MONTH_WED, GEAR)
  d
}
