#' Foreign  catch
#'
#' Builds annual (or seasonal) foreign catch by region and season,
#'
#' @param con_akfin A live DBI connection to the AKFIN (or equivalent) database used by
#'   \code{sql_run()}.
#' @param for_species_catch Character species label used in the catch query
#'   (e.g., \code{"PACIFIC COD"}). Must be one of:
#'   \itemize{
#'     \item ALL FLOUNDERS
#'     \item ALL ROCKFISH
#'     \item ARROWTOOTH FL
#'     \item ATKA MACKEREL
#'     \item DEMERSAL RF
#'     \item FLATFISH DISC
#'     \item FLOUNDER WO YFS
#'     \item GREENLAND TURBOT
#'     \item HERRING
#'     \item JACK DISCARD
#'     \item JACK MACKEREL
#'     \item OTHER DISCARD
#'     \item OTHER FISH
#'     \item OTHER RF DISC
#'     \item PACIFIC COD
#'     \item PACIFIC WHITING
#'     \item PELAGIC RF
#'     \item POLLOCK
#'     \item POP
#'     \item POP DISCARDS
#'     \item RATTAILS
#'     \item ROCK SOLE
#'     \item ROCKFISH WO POP
#'     \item SABLEFISH
#'     \item SABLEFISH DIS
#'     \item SHORTBELLY RF
#'     \item SLOPE ROCKFISH
#'     \item SNAILS
#'     \item SQUID
#'     \item SS THORNYHEAD
#'     \item TURBOTS
#'     \item YELLOWFIN SOLE
#'   }
#' @param start_year,end_year Numeric scalar years defining the inclusive year range.
#' @param season_def Optional named list mapping seasons to months, e.g.
#'   \code{list(A = 1:4, B = 5:12)}. If provided, outputs include \code{SEASON} and months
#'   not mapped are dropped (with a message when \code{verbose = TRUE}).
#' @param region_def Named list mapping region labels to AREA codes. Must be non-overlapping.
#' @param drop_unmapped Logical; drop rows whose AREA does not map into \code{region_def}.
#' @param verbose Logical; emit summaries and progress messages.
#'
#' @return A named list with two \code{data.table}s:
#'   \itemize{
#'     \item \code{aggregated}
#'     \item \code{by_gear}
#'   }
#' Each includes normalized \code{FREQ}, sample-size summaries, and \code{COMP_SOURCE}.
#'
#' @export

get_foreign_catch <- function(con_akfin,
                          for_species_catch = "PACIFIC COD",
                          start_year,
                          end_year,
                          season_def = list(A = 1:4, B = 5:12),
                          region_def = list(BS = c(50:53, 500:539),
                                            WGOA = c(61, 610),
                                            CGOA = c(62, 63, 64, 620:649),
                                            EGOA = c(65, 650:659),
                                            AI = c(54, 540:544)),
                          drop_unmapped = TRUE,
                          verbose = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table required.", call. = FALSE)
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("lubridate required.", call. = FALSE)

  DT <- data.table::as.data.table

  # ---- basic checks ----
  if (missing(con_akfin) || is.null(con_akfin)) stop("`con_akfin` is required.", call. = FALSE)
  if (missing(for_species_catch) || length(for_species_catch) != 1L) {
    stop("`for_species_catch` is required.", call. = FALSE)
  }
  if (missing(end_year) || length(end_year) != 1L || !is.numeric(end_year)) {
    stop("`end_year` must be a single numeric year.", call. = FALSE)
  }
  if (missing(start_year) || length(start_year) != 1L || !is.numeric(start_year)) {
    stop("`start_year` must be a single numeric year.", call. = FALSE)
  }

  if (!is.list(region_def) || length(region_def) == 0) {
    stop("`region_def` must be a named list like list(BS=500:539, WGOA=c(610)).", call. = FALSE)
  }
  nm <- names(region_def)
  if (is.null(nm) || any(!nzchar(nm))) stop("`region_def` must be a *named* list.", call. = FALSE)

  reg_clean <- lapply(region_def, function(x) suppressWarnings(as.integer(unique(as.vector(x)))))
  bad <- vapply(reg_clean, function(x) length(x) == 0 || any(is.na(x)), logical(1))
  if (any(bad)) {
    stop("`region_def` has empty/invalid AREA codes for: ",
         paste(nm[bad], collapse = ", "), call. = FALSE)
  }

  all_areas <- unlist(reg_clean, use.names = FALSE)
  dup <- all_areas[duplicated(all_areas)]
  if (length(dup) > 0) {
    stop("`region_def` assigns the same AREA to multiple regions. Duplicates: ",
         paste(sort(unique(dup)), collapse = ", "), call. = FALSE)
  }
  region_def <- reg_clean
  region_vec <- sort(unique(all_areas))

  # ---- helpers ----
  vcat <- function(...) if (isTRUE(verbose)) message(...)

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

  # ---- foreign historical catch (AFSC) ----
  fcatch <- sql_reader("for_catch_AKFIN.sql")
  fcatch <- sql_filter("IN", for_species_catch, fcatch, flag = "-- insert species_catch", value_type = "character")
  fcatch <- sql_filter(">=", start_year,        fcatch, flag = "-- insert syear",         value_type = "numeric")
  fcatch <- sql_filter("<=", end_year,          fcatch, flag = "-- insert eyear",         value_type = "numeric")
  fcatch <- sql_filter("IN", region_vec,        fcatch, flag = "-- insert area",          value_type = "numeric")

  FCATCH <- sql_run(con_akfin, fcatch) |> DT()
  data.table::setnames(FCATCH, toupper(names(FCATCH)))
  FCATCH[AREA < 100, AREA := AREA * 10]

  CATCHT <- DT(FCATCH)
  CATCHT[, AREA := suppressWarnings(as.integer(as.character(AREA)))]
  CATCHT <- add_region_group(CATCHT, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
  CATCHT <- add_user_season(CATCHT, season_def = season_def, month_col = "MONTH_WED", verbose = verbose)

  return(CATCHT)
}