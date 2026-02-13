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
#' @param wgoa_cod Logical. If TRUE, moves catch from NMFS area 620 west of -158 longitude into the 610 region group 
#'
#' @return A data.table with (at minimum) columns:
#'   YEAR, MONTH_WED, GEAR, TONS, RETAINED_OR_DISCARDED, SPECIES, AREA, SOURCE.
#'
#' @export
get_blend_catch_region <- function(con_akfin,
                            species_group,
                            region_def,
                            season_def,
                            start_year,
                            end_year,
                            wgoa_cod = TRUE
                            ) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }

  DT <- data.table::as.data.table

add_user_season <- function(dt, season_def, month_col = "MONTH_WED", verbose = TRUE) {
    dt <- DT(dt)
    if (is.null(season_def)) return(dt)

    if (!is.list(season_def) || length(season_def) == 0)
      stop("`season_def` must be a named list like list(A=1:3,B=4:6,...).", call. = FALSE)
    nm <- names(season_def)
    if (is.null(nm) || any(!nzchar(nm)))
      stop("`season_def` must be a *named* list (e.g., list(A=1:3,B=4:6)).", call. = FALSE)

    def <- lapply(season_def, function(x) as.integer(unique(as.vector(x))))
    bad <- vapply(def, function(x) any(is.na(x)) || any(x < 1L | x > 12L), logical(1))
    if (any(bad)) {
      stop("`season_def` contains invalid months (must be integers 1..12) in: ",
           paste(nm[bad], collapse = ", "), call. = FALSE)
    }

    allm <- unlist(def, use.names = FALSE)
    dupm <- allm[duplicated(allm)]
    if (length(dupm) > 0) {
      stop("`season_def` assigns the same month to multiple seasons. Duplicates: ",
           paste(sort(unique(dupm)), collapse = ", "), call. = FALSE)
    }

    missing_m <- setdiff(1:12, allm)
    if (length(missing_m) > 0 && isTRUE(verbose)) {
      message("Note: `season_def` does not include months: ",
              paste(missing_m, collapse = ", "),
              ". Those months will be dropped from seasonal outputs.")
    }

    key <- data.table::rbindlist(
      lapply(seq_along(def), function(i) data.table::data.table(SEASON = nm[i], MONTHX = def[[i]])),
      use.names = TRUE
    )

    if (!month_col %in% names(dt)) stop("Month column '", month_col, "' not found.", call. = FALSE)

    dt[, MONTHX := as.integer(as.character(get(month_col)))]
    dt <- merge(dt, key, by = "MONTHX", all.x = TRUE)
    dt[, MONTHX := NULL]
    dt[, SEASON := factor(SEASON, levels = nm)]
    dt
  }

add_region_group <- function(dt, region_def, area_col = "AREA", drop_unmapped = TRUE) {
    dt <- DT(dt)
    if (!area_col %in% names(dt)) stop("Area column '", area_col, "' not found.", call. = FALSE)

    dt[, (area_col) := suppressWarnings(as.integer(as.character(get(area_col))))]

    if (is.null(region_def)) {
      dt[, REGION_GRP := factor(sp_area, levels = sp_area)]
      return(dt)
    }

    # Build lookup table AREA -> REGION_GRP
    key <- data.table::rbindlist(
      lapply(names(region_def), function(nm) data.table::data.table(REGION_GRP = nm, AREA_KEY = region_def[[nm]])),
      use.names = TRUE
    )
    dt[, AREA_KEY := get(area_col)]
    dt <- merge(dt, key, by = "AREA_KEY", all.x = TRUE)
    dt[, AREA_KEY := NULL]
    dt[, REGION_GRP := factor(REGION_GRP, levels = names(region_def))]

    if (isTRUE(drop_unmapped)) dt <- dt[!is.na(REGION_GRP)]
    dt
  }


    if (!is.list(region_def) || length(region_def) == 0) {
      stop("`region_def` must be a named list like list(BS=500:539, WGOA=c(610,620)).", call. = FALSE)
    }
    nm <- names(region_def)
    if (is.null(nm) || any(!nzchar(nm))) {
      stop("`region_def` must be a *named* list.", call. = FALSE)
    }
    reg_clean <- lapply(region_def, function(x) suppressWarnings(as.integer(unique(as.vector(x)))))
    bad <- vapply(reg_clean, function(x) length(x) == 0 || any(is.na(x)), logical(1))
    if (any(bad)) {
      stop("`region_def` has empty/invalid AREA codes for: ", paste(nm[bad], collapse = ", "), call. = FALSE)
    }
    all_areas <- unlist(reg_clean, use.names = FALSE)
    dup <- all_areas[duplicated(all_areas)]
    if (length(dup) > 0) {
      stop("`region_def` assigns the same AREA to multiple regions. Duplicates: ",
           paste(sort(unique(dup)), collapse = ", "), call. = FALSE)
    }
    region_def <- reg_clean
    region_vec <- sort(unique(all_areas))
    region_levels <- nm

 
  # ---- read SQL from package ----
  sql_code <- sql_reader("dom_catch2.sql")

  # ---- inject filters ----
  
  sql_code <- sql_filter(
    sql_precode  = "IN",
    x            = region_vec,
    sql_code     = sql_code,
    flag         = "-- insert area",
    value_type   = "numeric"
  )

  # years: use <= year_max 
  # Prefer character if YEAR is stored as character; numeric if stored numeric.
  # We can safely use numeric literal in most Oracle schemas where YEAR is NUMBER.

  sql_code <- sql_filter(
    sql_precode = "<=",
    x           = end_year,
    sql_code    = sql_code,
    flag        = "-- insert eyears",
    value_type  = "numeric"
  )

  sql_code <- sql_filter(
    sql_precode = ">=",
    x           = start_year,
    sql_code    = sql_code,
    flag        = "-- insert syears",
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
  d <- DT(sql_run(con_akfin, sql_code))
  if (nrow(d) == 0L) {
    return(d)
  }

  # ---- standardize ----
  names(d) <- toupper(names(d))

  d <- add_user_season(dt=d, season_def=season_def, month_col = "MONTH_WED", verbose = FALSE)

  d <- add_region_group(dt=d, region_def=region_def, area_col = "AREA", drop_unmapped = TRUE)

  if(isTRUE(wgoa_cod)){d[AREA==620 & ADFG_AREA >= 580000]$REGION_GRP<-unique(d[AREA==610]$REGION_GRP)}

  # expected columns from your query
  # SPECIES, RETAINED_OR_DISCARDED, TONS, SEASON, MONTH_WED, GEAR, YEAR, AREA, REGION_GRP, SOURCE
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
  

  data.table::setorder(d, REGION_GRP, YEAR, SEASON, MONTH_WED, GEAR)
  d
}
