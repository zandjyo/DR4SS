#' Pull and prepare fishery age–length–weight observations for composition building
#'
#' Queries fishery biological observations (age, length, weight, sex, gear, and area)
#' from AKFIN (or equivalent) via a packaged SQL template, then standardizes fields and
#' derives commonly used stratification variables (season, region group, quarter, area, gear groups).
#'
#' @details
#' This function:
#' \enumerate{
#'   \item Reads the SQL template \code{dom_age_wt2.sql} using \code{sql_reader()} and inserts
#'         \code{species} and the requested \code{region_def} area codes using \code{sql_filter()}.
#'   \item Executes the query with \code{sql_run()} and enforces required columns.
#'   \item Filters to \code{start_year:end_year}, valid months (1--12), and non-missing lengths.
#'   \item Optionally remaps selected WGOA NMFS areas to WGOA (\code{610}) based on longitude
#'         when \code{wgoa_cod = TRUE}.
#'   \item Assigns a user-defined \code{SEASON} factor from \code{season_def} (months not mapped
#'         yield \code{SEASON = NA} and may be dropped downstream).
#'   \item Assigns \code{REGION_GRP} based on \code{region_def} and optionally drops unmapped rows.
#'   \item Cleans weights by setting \code{WEIGHT} to \code{NA} when \code{WEIGHT == 0} or
#'         \code{WEIGHT > max_wt}.
#'   \item Derives additional fields:
#'         \itemize{
#'           \item \code{AREA}: coarse area factor derived from \code{NMFS_AREA} (with 50 -> 51 adjustment)
#'           \item \code{QUARTER}: 1--4 from \code{MONTH}
#'           \item \code{SEX}: standardized to \code{"F"} or \code{"M"} (other values dropped)
#'           \item \code{GEAR2}: numeric gear grouping (default 1; 8 -> 2; 6 -> 3)
#'           \item \code{GEAR3}: character gear grouping (\code{"TRW"}, \code{"HAL"}, \code{"POT"})
#'         }
#' }
#'
#' @param con A live DBI connection to AKFIN (or equivalent) used by \code{sql_run()}.
#' @param species Species identifier inserted into the SQL query (typically a numeric code).
#' @param season_def Named list mapping season labels to months, e.g. \code{list(A = 1:4, B = 5:12)}.
#' @param region_def Named list mapping region labels to NMFS area codes. Area codes must be unique
#'   across regions (non-overlapping).
#' @param start_year,end_year Inclusive year range to retain after the SQL pull.
#' @param max_wt Maximum plausible individual weight. Weights of 0 or > \code{max_wt} are set to \code{NA}.
#' @param wgoa_cod Logical; if \code{TRUE}, applies a WGOA recode for NMFS area 620 to 610 based on longitude
#'   (requires longitude field present in the SQL output).
#' @param drop_unmapped Logical; if \code{TRUE}, rows whose \code{NMFS_AREA} does not map into \code{region_def}
#'   are removed.
#'
#' @return A \code{data.table} of fishery observations including standardized fields and derived columns
#' (e.g., \code{SEASON}, \code{REGION_GRP}, \code{QUARTER}, \code{AREA}, \code{GEAR2}, \code{GEAR3}).
#' The table is ordered by \code{YEAR, MONTH}.
#'
#' @seealso \code{sql_reader}, \code{sql_filter}, \code{sql_run}
#'
#' @importFrom data.table as.data.table fifelse rbindlist setorder
#' @export
get_fishery_age_wt_data <- function(con,
                                    species,
                                    season_def,
                                    region_def,
                                    start_year = 2007,
                                    end_year = 2025,
                                    max_wt=50,
                                    wgoa_cod=TRUE,
                                    drop_unmapped=TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }

  DT <- data.table::as.data.table

# Seasons: validate + attach SEASON column using month_col
  add_user_season <- function(dt, season_def, month_col = "MONTH", verbose = FALSE) {
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
    if (length(missing_m) > 0) {
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

  # Regions: attach REGION_GRP based on AREA mapping (or single-level default)
  add_region_group <- function(dt, region_def, area_col = "NMFS_AREA", drop_unmapped = TRUE) {
    dt <- DT(dt)
    if (!area_col %in% names(dt)) stop("Area column '", area_col, "' not found.", call. = FALSE)

    dt[, (area_col) := suppressWarnings(as.integer(as.character(get(area_col))))]

    
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


  # ---- region → NMFS area mapping ----
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
  sql_code <- sql_reader("dom_age_wt2.sql")
  sql_code <- sql_filter(
    sql_precode = "IN",
    x = species,
    sql_code = sql_code,
    flag = "-- insert species"
  )
  sql_code <- sql_filter(
    sql_precode = "IN",
    x = region_vec,
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

  d <- d[YEAR >= start_year & YEAR<= end_year]
  d <- d[MONTH %in% 1:12]
  d <- d[!is.na(LENGTH)]

  if(isTRUE(wgoa_cod)){d[NMFS_AREA == 620 & LONDD_END <= -158]$NMFS_AREA <- 610}  ## for WGOA Pcod
  
  d <- add_user_season(d, season_def = season_def, month_col = "MONTH", verbose = FALSE)
  d <- add_region_group(d, region_def = region_def, area_col = "NMFS_AREA", drop_unmapped = drop_unmapped)

  # Clean weights
  d[WEIGHT == 0 | WEIGHT > max_wt, WEIGHT := NA_real_]

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

    # ---- map gear groups (GEAR3) ----
  d[, GEAR := toupper(trimws(as.character(GEAR)))]
  d[, GEAR3 := "TRW"]
  d[GEAR == 8, GEAR3 := "HAL"]
  d[GEAR == 6, GEAR3 := "POT"]

  data.table::setorder(d, YEAR, MONTH)

  d
}
