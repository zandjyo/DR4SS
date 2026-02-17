#' Foreign length composition weighted by foreign catch
#'
#' Builds annual (or seasonal) foreign length compositions by region (and optionally sex),
#' using observer length frequencies reweighted to foreign catch totals. The weighting follows
#' a multi-stage approach: within-haul frequency scaling, haul-to-stratum scaling, and expansion
#' to catch-based totals (overall and by-gear variants).
#'
#' @details
#' The function:
#' \enumerate{
#'   \item Pulls foreign observer length frequency data for \code{species} and requested areas/years.
#'   \item Computes mean weights (AVEWT) at several strata resolutions (YEAR-AREA2-MONTH-GEAR, etc.)
#'         and uses a priority fallback to assign \code{AVEWT} to catch rows.
#'   \item Pulls foreign catch (tons) for \code{for_species_catch} and converts to numbers using AVEWT.
#'   \item Joins observer length data to catch numbers by REGION_GRP/YEAR/AREA2/GEAR/MONTH.
#'   \item Computes weights:
#'         \describe{
#'           \item{\code{WEIGHT1}}{Within-haul share: \code{SUM_FREQUENCY / YAGMH_SFREQ}}
#'           \item{\code{WEIGHT2}}{Haul-to-stratum scaling: \code{YAGMH_SNUM / YAGM_SNUM}}
#'           \item{\code{WEIGHT3}}{Gear share of annual catch numbers: \code{YAGM_TNUM / YG_TNUM}}
#'           \item{\code{WEIGHT4}}{Stratum share of annual catch numbers: \code{YAGM_TNUM / Y_TNUM}}
#'         }
#'   \item Produces normalized compositions \code{FREQ} on a dense 1:maxL grid for:
#'         \itemize{
#'           \item aggregated (by REGION_GRP x YEAR [x SEASON] [x SEX])
#'           \item by-gear (by REGION_GRP x YEAR x GEAR [x SEASON] [x SEX])
#'         }
#' }
#'
#' @param con_akfin A live DBI connection to the AKFIN (or equivalent) database used by
#'   \code{sql_run()}.
#' @param species Numeric species code used in the length-frequency query (e.g., 202).
#' @param for_species_catch Character species label used in the catch query (e.g., \code{"PACIFIC COD"}).
#' @param start_year,end_year Numeric scalar years defining the inclusive year range.
#' @param SEX Logical; if \code{TRUE}, keep sex-specific compositions (expects a \code{SEX} column).
#'   If \code{FALSE}, sexes are pooled.
#' @param season_def Optional named list mapping seasons to months, e.g.
#'   \code{list(A = 1:4, B = 5:12)}. If provided, outputs include \code{SEASON} and months not mapped
#'   are dropped (with a message when \code{verbose=TRUE}).
#' @param region_def Named list mapping region labels to AREA codes. Must be non-overlapping.
#'   Default corresponds to common Alaska groundfish groupings (BS/WGOA/CGOA/EGOA/AI).
#' @param drop_unmapped Logical; drop rows whose AREA does not map into \code{region_def}.
#' @param wgoa_cod Logical; if \code{TRUE}, optionally remaps selected WGOA areas based on longitude
#'   (see code block).
#' @param verbose Logical; emit summaries and progress messages.
#'
#' @return A named list with two \code{data.table}s:
#' \describe{
#'   \item{\code{aggregated}}{Compositions aggregated across gear. Columns include REGION_GRP, YEAR,
#'     LENGTH (and optionally SEASON, SEX), FREQ, plus sample size summaries (NSAMP, NHAUL, NSTRATA).}
#'   \item{\code{by_gear}}{Compositions by gear. Columns include REGION_GRP, YEAR, GEAR, LENGTH
#'     (and optionally SEASON, SEX), FREQ, plus sample size summaries.}
#' }
#'
#' @seealso \code{\link[data.table]{data.table}}, \code{\link[lubridate]{month}}
#'
#' @importFrom data.table as.data.table setnames fifelse uniqueN rbindlist
#' @importFrom lubridate month
#'
#' @export
foreign_length_by_catch <- function(con_akfin,
                                  species =202,
                                  for_species_catch ="PACIFIC COD",
                                  start_year,
                                  end_year,
                                  SEX = TRUE,
                                  season_def = list(A=1:4,B=5:12),
                                  region_def = list(BS=c(50:53,500:539),WGOA=c(61,610),CGOA=c(62,63,64,620:649),EGOA=c(65,650:659),AI=c(54,540:544)),
                                  drop_unmapped = TRUE,
                                  wgoa_cod =TRUE,
                                  verbose=TRUE
                                  ) {

if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table required.", call. = FALSE)
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("lubridate required.", call. = FALSE)

  DT <- data.table::as.data.table

  # ---- basic checks ----
  if (is.null(con_akfin)) stop("`con_akfin` is required.", call. = FALSE)
 
  # ---- package/import checks (lightweight) ----
  for (pkg in c("data.table", "dplyr", "lubridate")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Package '", pkg, "' is required.", call. = FALSE)
    }
  }

  if (missing(con_akfin) || is.null(con_akfin)) stop("`con_akfin` is required.", call. = FALSE)
  if (missing(species) || length(species) != 1L || !is.numeric(species)) {
    stop("`species` must be a single numeric code.", call. = FALSE)
  }
  
  if (missing(for_species_catch) || length(for_species_catch) != 1L) stop("`for_species_catch` is required.", call. = FALSE)
  if (missing(end_year) || length(end_year) != 1L || !is.numeric(end_year)) stop("`end_year` must be a single numeric year.", call. = FALSE)
  if (missing(start_year) || length(start_year) != 1L || !is.numeric(start_year)) stop("`start_year` must be a single numeric year.", call. = FALSE)

    if (!is.list(region_def) || length(region_def) == 0) {
      stop("`region_def` must be a named list like list(BS=500:539, WGOA=c(610)).", call. = FALSE)
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

  # ---- helpers ----
  vcat <- function(...) if (isTRUE(verbose)) message(...)

  summarize_by_year <- function(dt, label, col = "SUM_FREQUENCY") {
    if (!isTRUE(verbose)) return(invisible(NULL))
    if (is.null(dt) || nrow(dt) == 0) {
      message(label, ": 0 rows")
      return(invisible(NULL))
    }
    tmp <- dt[, .(N = sum(get(col), na.rm = TRUE)), by = .(YEAR,REGION_GRP)][order(YEAR)]
    message(label, " (sum ", col, " by YEAR):")
    print(tmp)
    invisible(NULL)
  }

  add_area2_quarter <- function(x) {
    x <- DT(x)
    x[, MONTH := as.integer(as.character(MONTH))]
    x[, QUARTER := 4L]
    x[MONTH < 3, QUARTER := 1L]
    x[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
    x[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
    x[, AREA2 := trunc(as.numeric(AREA) / 10) * 10]
    x[AREA2 == 500, AREA2 := 510]
    x
  }

  # AVEWT joiner: compute AVEWT by priority fallbacks
  assign_avewt <- function(DT0, joins, priority) {
    DT0 <- DT(DT0)
    for (j in joins) DT0 <- merge(DT0, j$table, by = j$by, all.x = TRUE)
    DT0[, AVEWT := get(priority[1])]
    if (length(priority) > 1) {
      for (nm in priority[-1]) DT0[is.na(AVEWT), AVEWT := get(nm)]
    }
    DT0
  }

  # Seasons: validate + attach SEASON column using month_col
  add_user_season <- function(dt, season_def, month_col = "MONTH", verbose = TRUE) {
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

  # Regions: attach REGION_GRP based on AREA mapping (or single-level default)
  add_region_group <- function(dt, region_def, area_col = "AREA", drop_unmapped = TRUE) {
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


  # ---- Pull foreign length/species-comp (AFSC) ----
  Flfreq <- sql_reader("for_length2.sql")

  Flfreq <- sql_filter(sql_precode = "IN", x = species, sql_code = Flfreq, flag = "-- insert species",value_type = c("numeric"))
  Flfreq <- sql_filter(sql_precode ="IN", x = region_vec, sql_code = Flfreq, flag = "-- insert region",value_type = c("numeric"))
  Flfreq <- sql_filter(sql_precode =">=", x = start_year, sql_code = Flfreq, flag = "-- insert syear",value_type = c("numeric"))
  Flfreq <- sql_filter(sql_precode ="<=", x = end_year, sql_code = Flfreq, flag = "-- insert eyear",value_type = c("numeric"))

  Fspcomp <- sql_run(con_akfin, Flfreq) |>
    data.table::as.data.table()
  data.table::setnames(Fspcomp, toupper(names(Fspcomp)))

  Fspcomp[AREA < 100, AREA := AREA * 10]
  
  if(isTRUE(wgoa_cod)){Fspcomp[AREA %in% 620 & LONGITUDE >= 15800, AREA := 610L]} ## moving the WGOA line in 610 to -158.

  Fspcomp[, MONTH := as.numeric(MONTH)]
  Fspcomp[, QUARTER := floor((MONTH / 3) - 0.3) + 1]

 # Fspcomp[, WED := WED(HDAY)]
 # Fspcomp[, MONTH_WED := lubridate::month(WED)]

 # ---- Safe date parsing for HDAY ----
  parse_safe_date <- function(x) {
    if (!requireNamespace("lubridate", quietly = TRUE)) {
      stop("lubridate required.", call. = FALSE)
    }

    xx <- trimws(as.character(x))
    xx[xx %in% c("", "NA", "NaN")] <- NA_character_

  # Try common formats quietly
    d <- suppressWarnings(
      lubridate::ymd(xx, quiet = TRUE)
    )

    # Try numeric YYYYMMDD fallback
    need <- is.na(d) & grepl("^\\d{8}$", xx)
    if (any(need)) {
      d[need] <- suppressWarnings(as.Date(xx[need], format = "%Y%m%d"))
    }

    d
  }

  # Parse HDAY
  Fspcomp[, HDATE := parse_safe_date(HDAY)]

  # Drop invalid dates
  bad_n <- Fspcomp[is.na(HDATE), .N]
  if (bad_n > 0 && isTRUE(verbose)) {
    message("Dropping ", bad_n, " rows with invalid HDAY values.")
  }

  Fspcomp <- Fspcomp[!is.na(HDATE)]

  # Compute week-ending date (Saturday example)
  Fspcomp[, WED := HDATE + ((6 - lubridate::wday(HDATE, week_start = 1)) %% 7)]

  Fspcomp[, MONTH_WED := lubridate::month(WED)]

  Fspcomp[, MONTH := as.numeric(MONTH)]
  Fspcomp[, AREA2 := trunc(AREA / 10) * 10]
  Fspcomp[AREA2 == 500, AREA2 := 510]
  Fspcomp <- Fspcomp[EXTRAPOLATED_WEIGHT > 0 & NUMB > 0]

  OBS_DATA <- Fspcomp[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER, CRUISE, VES_AKR_ADFG,
                          HAUL_JOIN, SEX, LENGTH, SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]

  OBS_DATA <- add_region_group(OBS_DATA, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
  summarize_by_year(OBS_DATA, "Observer length data", col = "SUM_FREQUENCY")

  Tspcomp <- DT(Fspcomp)

  YAGM_AVWT <- Tspcomp[, .(YAGM_AVE_WT = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, AREA2, MONTH, GEAR)]
  YAM_AVWT  <- Tspcomp[, .(YAM_AVE_WT  = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, AREA2, MONTH)]
  YGM_AVWT  <- Tspcomp[, .(YGM_AVE_WT  = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, GEAR, MONTH)]
  YGQ_AVWT  <- Tspcomp[, .(YGQ_AVE_WT  = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, GEAR, QUARTER)]
  YAQ_AVWT  <- Tspcomp[, .(YAQ_AVE_WT  = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, AREA2, QUARTER)]
  YG_AVWT   <- Tspcomp[, .(YG_AVE_WT   = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, GEAR)]

  for (x in list(YAGM_AVWT, YAM_AVWT, YGM_AVWT)) {
    x[, YEAR := as.numeric(YEAR)]
    x[, MONTH := as.numeric(MONTH)]
  }
  for (x in list(YAQ_AVWT, YGQ_AVWT)) {
    x[, YEAR := as.numeric(YEAR)]
    x[, QUARTER := as.numeric(QUARTER)]
  }
  YG_AVWT[, YEAR := as.numeric(YEAR)]

  avwt_joins_full <- list(
    list(table = YAGM_AVWT, by = c("YEAR", "AREA2", "MONTH", "GEAR")),
    list(table = YGM_AVWT,  by = c("YEAR", "GEAR", "MONTH")),
    list(table = YGQ_AVWT,  by = c("YEAR", "GEAR", "QUARTER")),
    list(table = YAM_AVWT,  by = c("YEAR", "AREA2", "MONTH")),
    list(table = YAQ_AVWT,  by = c("YEAR", "AREA2", "QUARTER")),
    list(table = YG_AVWT,   by = c("YEAR", "GEAR"))
  )

      ALL_DATA <- OBS_DATA
  
  # ---- foreign historical catch (AFSC) ----
 # foreign_catch_area <- if (sp_area == "BS") "LIKE '%BERING%'" else
 #   if (sp_area == "AI") "LIKE '%ALEUTIANS%'" else
 #     "IN ('KODIAK','YAKUTAT', 'SHUMAGIN','S.E. ALASKA', 'SHELIKOF STR.','CHIRIKOF')"

  fcatch <- sql_reader("for_catch2.sql")
  fcatch <- sql_filter("IN", for_species_catch, fcatch, flag = "-- insert species_catch",value_type = c("character"))
  fcatch <- sql_filter(">=", start_year, fcatch, flag = "-- insert syear",value_type = c("numeric"))
  fcatch <- sql_filter("<=", end_year, fcatch, flag = "-- insert eyear",value_type = c("numeric"))
  fcatch <- sql_filter("IN",region_vec , fcatch, flag = "-- insert area", value_type = c("numeric"))

  FCATCH <- sql_run(con_akfin, fcatch) |>
    data.table::as.data.table()
  data.table::setnames(FCATCH, toupper(names(FCATCH)))

  FCATCH <- data.table::as.data.table(FCATCH)
  FCATCH[AREA < 100, AREA := AREA * 10]

  CATCHT<-FCATCH

  CATCHT[, AREA := suppressWarnings(as.integer(as.character(AREA)))]
  CATCHT <- add_region_group(CATCHT, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
  if (nrow(CATCHT) == 0) stop("No foreign blend catch rows after region mapping/filtering.", call. = FALSE)

  CATCHT[, MONTH := suppressWarnings(as.integer(as.character(MONTH_WED)))]
  CATCHT[, QUARTER := 4L]
  CATCHT[MONTH < 3, QUARTER := 1L]
  CATCHT[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
  CATCHT[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
  CATCHT[, AREA2 := trunc(as.numeric(AREA) / 10) * 10]
  CATCHT[AREA2 == 500, AREA2 := 510]
  CATCHT <- CATCHT[GEAR %in% c("POT","TRW","HAL")]
  CATCHT[, YEAR := as.numeric(YEAR)]

  CATCHT2 <- assign_avewt(
    CATCHT,
    joins = list(
      list(table = YAGM_AVWT, by = c("YEAR","AREA2","MONTH","GEAR")),
      list(table = YGM_AVWT,  by = c("YEAR","GEAR","MONTH")),
      list(table = YGQ_AVWT,  by = c("YEAR","GEAR","QUARTER")),
      list(table = YAM_AVWT,  by = c("YEAR","AREA2","MONTH")),
      list(table = YG_AVWT,   by = c("YEAR","GEAR"))
    ),
    priority = c("YAGM_AVE_WT","YGM_AVE_WT","YGQ_AVE_WT","YAM_AVE_WT","YG_AVE_WT")
  )

  CATCHT2[, NUMBER := TONS / (CATCHT2$AVEWT / 1000)]
  CATCHT2[, SPECIES := as.numeric(species)]
  CATCHT2 <- CATCHT2[is.finite(NUMBER) & NUMBER > 0]

  # NOTE: now include REGION_GRP in catch totals so compositions are per-region
  CATCHT4 <- CATCHT2[, .(YAGM_TNUM = sum(NUMBER), YAGM_TONS = sum(TONS)),
                    by = .(REGION_GRP, SPECIES, YEAR, GEAR, AREA2, MONTH)]

  xt_YAG <- CATCHT4[, .(YAG_TNUM = sum(YAGM_TNUM)), by = .(REGION_GRP, YEAR, AREA2, GEAR)]
  xt_YG  <- CATCHT4[, .(YG_TNUM  = sum(YAGM_TNUM)), by = .(REGION_GRP, YEAR, GEAR)]
  xt_Y   <- CATCHT4[, .(Y_TNUM   = sum(YAGM_TNUM)), by = .(REGION_GRP, YEAR)]

  CATCHT4 <- merge(CATCHT4, xt_YAG, by = c("REGION_GRP","YEAR","AREA2","GEAR"), all.x = TRUE)
  CATCHT4 <- merge(CATCHT4, xt_YG,  by = c("REGION_GRP","YEAR","GEAR"),        all.x = TRUE)
  CATCHT4 <- merge(CATCHT4, xt_Y,   by = c("REGION_GRP","YEAR"),               all.x = TRUE)

  # ------------------------------------------------------------
  # 6) Length data prep for weighting
  # ------------------------------------------------------------
  Length <- ALL_DATA[GEAR %in% c("POT","TRW","HAL")]
  Length[, YEAR := as.numeric(YEAR)]
  Length[, MONTH := as.integer(as.character(MONTH))]
  if (!"MONTH_WED" %in% names(Length)) Length[, MONTH_WED := MONTH]
  Length[, YAGMH_SNUM := NUMB]
  Length[, YAGMH_STONS := EXTRAPOLATED_WEIGHT / 1000]

  # ensure region group exists (ALL_DATA should already carry it via OBS/PORT)
  if (!"REGION_GRP" %in% names(Length)) {
    Length <- add_region_group(Length, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
  }

  if (!isTRUE(SEX)) {
    Length <- Length[, .(SUM_FREQUENCY = sum(SUM_FREQUENCY)),
                     by = .(REGION_GRP, SPECIES,YEAR,AREA2,GEAR,MONTH,MONTH_WED,CRUISE,VES_AKR_ADFG,HAUL_JOIN,
                            LENGTH,YAGMH_STONS,YAGMH_SNUM)]
  } else {
    Length <- Length[, .(SUM_FREQUENCY = sum(SUM_FREQUENCY)),
                     by = .(REGION_GRP, SPECIES,YEAR,AREA2,GEAR,MONTH,MONTH_WED,CRUISE,VES_AKR_ADFG,HAUL_JOIN,
                            SEX,LENGTH,YAGMH_STONS,YAGMH_SNUM)]
  }

  L_YAGMH <- Length[, .(YAGMH_SFREQ = sum(SUM_FREQUENCY)), by = .(CRUISE,VES_AKR_ADFG,HAUL_JOIN)]
  Length <- merge(Length, L_YAGMH, by = c("CRUISE","VES_AKR_ADFG","HAUL_JOIN"), all.x = TRUE)

  L_YAGM <- Length[, .(YAGM_SNUM = sum(YAGMH_SNUM), YAGM_SFREQ = sum(SUM_FREQUENCY)),
                   by = .(REGION_GRP, YEAR,AREA2,GEAR,MONTH)]
  Length <- merge(Length, L_YAGM, by = c("REGION_GRP","YEAR","AREA2","GEAR","MONTH"), all.x = TRUE)

  Length[, SPECIES := as.numeric(SPECIES)]
  CATCHT4[, SPECIES := as.numeric(SPECIES)]
  x <- merge(Length, CATCHT4, by = c("REGION_GRP","SPECIES","YEAR","AREA2","GEAR","MONTH"), all.x = TRUE)
  y2 <- x[!is.na(YAGM_TNUM)]

  # attach SEASON (optional) using chosen month column
  y2 <- add_user_season(y2, season_def = season_def, month_col = "MONTH", verbose = verbose)
  if (!is.null(season_def)) y2 <- y2[!is.na(SEASON)]

  y2[, WEIGHT1 := SUM_FREQUENCY / YAGMH_SFREQ]
  y2[, WEIGHT2 := YAGMH_SNUM / YAGM_SNUM]
  y2[, WEIGHT3 := YAGM_TNUM / YG_TNUM]
  y2[, WEIGHT4 := YAGM_TNUM / Y_TNUM]

  y2[, WEIGHTX      := WEIGHT1 * WEIGHT2 * WEIGHT4]
  y2[, WEIGHTX_GEAR := WEIGHT1 * WEIGHT2 * WEIGHT3]

  y2 <- y2[YAGM_SFREQ > 30]

  # ------------------------------------------------------------
  # 7) Final compositions (annual or seasonal), now by REGION_GRP
  # ------------------------------------------------------------
  have_season <- !is.null(season_def)

  if (!isTRUE(SEX)) {
    if (!have_season) {
      agg <- y2[, .(WEIGHT = sum(WEIGHTX, na.rm = TRUE)), by = .(REGION_GRP, YEAR, LENGTH)]
      agg[, TWEIGHT := sum(WEIGHT), by = .(REGION_GRP, YEAR)]
      agg[, FREQ := data.table::fifelse(TWEIGHT > 0, WEIGHT / TWEIGHT, 0)]
      out_agg <- agg[, .(REGION_GRP, YEAR, LENGTH, FREQ)]

      byg <- y2[, .(WEIGHT = sum(WEIGHTX_GEAR, na.rm = TRUE)), by = .(REGION_GRP, YEAR, GEAR, LENGTH)]
      byg[, TWEIGHT := sum(WEIGHT), by = .(REGION_GRP, YEAR, GEAR)]
      byg[, FREQ := data.table::fifelse(TWEIGHT > 0, WEIGHT / TWEIGHT, 0)]
      out_byg <- byg[, .(REGION_GRP, YEAR, GEAR, LENGTH, FREQ)]
    } else {
      agg <- y2[, .(WEIGHT = sum(WEIGHTX, na.rm = TRUE)), by = .(REGION_GRP, YEAR, SEASON, LENGTH)]
      agg[, TWEIGHT := sum(WEIGHT), by = .(REGION_GRP, YEAR, SEASON)]
      agg[, FREQ := data.table::fifelse(TWEIGHT > 0, WEIGHT / TWEIGHT, 0)]
      out_agg <- agg[, .(REGION_GRP, YEAR, SEASON, LENGTH, FREQ)]

      byg <- y2[, .(WEIGHT = sum(WEIGHTX_GEAR, na.rm = TRUE)), by = .(REGION_GRP, YEAR, SEASON, GEAR, LENGTH)]
      byg[, TWEIGHT := sum(WEIGHT), by = .(REGION_GRP, YEAR, SEASON, GEAR)]
      byg[, FREQ := data.table::fifelse(TWEIGHT > 0, WEIGHT / TWEIGHT, 0)]
      out_byg <- byg[, .(REGION_GRP, YEAR, SEASON, GEAR, LENGTH, FREQ)]
    }
  } else {
    if (!have_season) {
      agg <- y2[, .(WEIGHT = sum(WEIGHTX, na.rm = TRUE)), by = .(REGION_GRP, YEAR, SEX, LENGTH)]
      agg[, TWEIGHT := sum(WEIGHT), by = .(REGION_GRP, YEAR, SEX)]
      agg[, FREQ := data.table::fifelse(TWEIGHT > 0, WEIGHT / TWEIGHT, 0)]
      out_agg <- agg[, .(REGION_GRP, YEAR, SEX, LENGTH, FREQ)]

      byg <- y2[, .(WEIGHT = sum(WEIGHTX_GEAR, na.rm = TRUE)), by = .(REGION_GRP, YEAR, GEAR, SEX, LENGTH)]
      byg[, TWEIGHT := sum(WEIGHT), by = .(REGION_GRP, YEAR, GEAR, SEX)]
      byg[, FREQ := data.table::fifelse(TWEIGHT > 0, WEIGHT / TWEIGHT, 0)]
      out_byg <- byg[, .(REGION_GRP, YEAR, GEAR, SEX, LENGTH, FREQ)]
    } else {
      agg <- y2[, .(WEIGHT = sum(WEIGHTX, na.rm = TRUE)), by = .(REGION_GRP, YEAR, SEASON, SEX, LENGTH)]
      agg[, TWEIGHT := sum(WEIGHT), by = .(REGION_GRP, YEAR, SEASON, SEX)]
      agg[, FREQ := data.table::fifelse(TWEIGHT > 0, WEIGHT / TWEIGHT, 0)]
      out_agg <- agg[, .(REGION_GRP, YEAR, SEASON, SEX, LENGTH, FREQ)]

      byg <- y2[, .(WEIGHT = sum(WEIGHTX_GEAR, na.rm = TRUE)), by = .(REGION_GRP, YEAR, SEASON, GEAR, SEX, LENGTH)]
      byg[, TWEIGHT := sum(WEIGHT), by = .(REGION_GRP, YEAR, SEASON, GEAR, SEX)]
      byg[, FREQ := data.table::fifelse(TWEIGHT > 0, WEIGHT / TWEIGHT, 0)]
      out_byg <- byg[, .(REGION_GRP, YEAR, SEASON, GEAR, SEX, LENGTH, FREQ)]
    }
  }

  # Dense grids
  maxL <- max(out_agg$LENGTH, na.rm = TRUE)
  if (!is.finite(maxL) || maxL <= 0) stop("No valid LENGTH/AGE bins after weighting.", call. = FALSE)

  regions <- sort(unique(as.character(out_agg$REGION_GRP)))
  years <- sort(unique(out_agg$YEAR))
  if (!have_season) {
    if (!isTRUE(SEX)) {
      grid1 <- DT(expand.grid(REGION_GRP = regions, YEAR = years, LENGTH = 1:maxL))
      out_agg <- merge(grid1, out_agg, by = c("REGION_GRP","YEAR","LENGTH"), all.x = TRUE)
      out_agg[is.na(FREQ), FREQ := 0]

      grid2 <- DT(expand.grid(REGION_GRP = regions,
                              YEAR = sort(unique(out_byg$YEAR)),
                              GEAR = sort(unique(out_byg$GEAR)),
                              LENGTH = 1:maxL))
      out_byg <- merge(grid2, out_byg, by = c("REGION_GRP","YEAR","GEAR","LENGTH"), all.x = TRUE)
      out_byg[is.na(FREQ), FREQ := 0]
    } else {
      sex_levels <- sort(unique(out_agg$SEX))
      grid1 <- DT(expand.grid(REGION_GRP = regions, YEAR = years, SEX = sex_levels, LENGTH = 1:maxL))
      out_agg <- merge(grid1, out_agg, by = c("REGION_GRP","YEAR","SEX","LENGTH"), all.x = TRUE)
      out_agg[is.na(FREQ), FREQ := 0]

      grid2 <- DT(expand.grid(REGION_GRP = regions,
                              YEAR = sort(unique(out_byg$YEAR)),
                              GEAR = sort(unique(out_byg$GEAR)),
                              SEX = sex_levels,
                              LENGTH = 1:maxL))
      out_byg <- merge(grid2, out_byg, by = c("REGION_GRP","YEAR","GEAR","SEX","LENGTH"), all.x = TRUE)
      out_byg[is.na(FREQ), FREQ := 0]
    }
  } else {
    seas_levels <- levels(y2$SEASON)
    if (!isTRUE(SEX)) {
      grid1 <- DT(expand.grid(REGION_GRP = regions, YEAR = years, SEASON = seas_levels, LENGTH = 1:maxL))
      out_agg <- merge(grid1, out_agg, by = c("REGION_GRP","YEAR","SEASON","LENGTH"), all.x = TRUE)
      out_agg[is.na(FREQ), FREQ := 0]
      out_agg[, SEASON := factor(SEASON, levels = seas_levels)]

      grid2 <- DT(expand.grid(REGION_GRP = regions,
                              YEAR = sort(unique(out_byg$YEAR)),
                              SEASON = seas_levels,
                              GEAR = sort(unique(out_byg$GEAR)),
                              LENGTH = 1:maxL))
      out_byg <- merge(grid2, out_byg, by = c("REGION_GRP","YEAR","SEASON","GEAR","LENGTH"), all.x = TRUE)
      out_byg[is.na(FREQ), FREQ := 0]
      out_byg[, SEASON := factor(SEASON, levels = seas_levels)]
    } else {
      sex_levels <- sort(unique(out_agg$SEX))
      grid1 <- DT(expand.grid(REGION_GRP = regions, YEAR = years, SEASON = seas_levels, SEX = sex_levels, LENGTH = 1:maxL))
      out_agg <- merge(grid1, out_agg, by = c("REGION_GRP","YEAR","SEASON","SEX","LENGTH"), all.x = TRUE)
      out_agg[is.na(FREQ), FREQ := 0]
      out_agg[, SEASON := factor(SEASON, levels = seas_levels)]

      grid2 <- DT(expand.grid(REGION_GRP = regions,
                              YEAR = sort(unique(out_byg$YEAR)),
                              SEASON = seas_levels,
                              GEAR = sort(unique(out_byg$GEAR)),
                              SEX = sex_levels,
                              LENGTH = 1:maxL))
      out_byg <- merge(grid2, out_byg, by = c("REGION_GRP","YEAR","SEASON","GEAR","SEX","LENGTH"), all.x = TRUE)
      out_byg[is.na(FREQ), FREQ := 0]
      out_byg[, SEASON := factor(SEASON, levels = seas_levels)]
    }
  }

  # Sample size summaries (include REGION_GRP, and SEASON if present)
  if (!isTRUE(SEX)) {
    if (!have_season) {
      samp <- y2[, .(
        NSAMP = sum(SUM_FREQUENCY, na.rm = TRUE),
        NHAUL = data.table::uniqueN(HAUL_JOIN),
        NSTRATA = data.table::uniqueN(paste(AREA2, MONTH, GEAR))
      ), by = .(REGION_GRP, YEAR)]
      out_agg <- merge(out_agg, samp, by = c("REGION_GRP","YEAR"), all.x = TRUE)

      sampg <- y2[, .(
        NSAMP = sum(SUM_FREQUENCY, na.rm = TRUE),
        NHAUL = data.table::uniqueN(HAUL_JOIN),
        NSTRATA = data.table::uniqueN(paste(AREA2, MONTH, GEAR))
      ), by = .(REGION_GRP, YEAR, GEAR)]
      out_byg <- merge(out_byg, sampg, by = c("REGION_GRP","YEAR","GEAR"), all.x = TRUE)
    } else {
      samp <- y2[, .(
        NSAMP = sum(SUM_FREQUENCY, na.rm = TRUE),
        NHAUL = data.table::uniqueN(HAUL_JOIN),
        NSTRATA = data.table::uniqueN(paste(AREA2, as.character(SEASON), GEAR))
      ), by = .(REGION_GRP, YEAR, SEASON)]
      out_agg <- merge(out_agg, samp, by = c("REGION_GRP","YEAR","SEASON"), all.x = TRUE)

      sampg <- y2[, .(
        NSAMP = sum(SUM_FREQUENCY, na.rm = TRUE),
        NHAUL = data.table::uniqueN(HAUL_JOIN),
        NSTRATA = data.table::uniqueN(paste(AREA2, as.character(SEASON), GEAR))
      ), by = .(REGION_GRP, YEAR, SEASON, GEAR)]
      out_byg <- merge(out_byg, sampg, by = c("REGION_GRP","YEAR","SEASON","GEAR"), all.x = TRUE)
    }
  } else {
    if (!have_season) {
      samp <- y2[, .(
        NSAMP = sum(SUM_FREQUENCY, na.rm = TRUE),
        NHAUL = data.table::uniqueN(HAUL_JOIN),
        NSTRATA = data.table::uniqueN(paste(AREA2, MONTH, GEAR))
      ), by = .(REGION_GRP, YEAR, SEX)]
      out_agg <- merge(out_agg, samp, by = c("REGION_GRP","YEAR","SEX"), all.x = TRUE)

      sampg <- y2[, .(
        NSAMP = sum(SUM_FREQUENCY, na.rm = TRUE),
        NHAUL = data.table::uniqueN(HAUL_JOIN),
        NSTRATA = data.table::uniqueN(paste(AREA2, MONTH, GEAR))
      ), by = .(REGION_GRP, YEAR, GEAR, SEX)]
      out_byg <- merge(out_byg, sampg, by = c("REGION_GRP","YEAR","GEAR","SEX"), all.x = TRUE)
    } else {
      samp <- y2[, .(
        NSAMP = sum(SUM_FREQUENCY, na.rm = TRUE),
        NHAUL = data.table::uniqueN(HAUL_JOIN),
        NSTRATA = data.table::uniqueN(paste(AREA2, as.character(SEASON), GEAR))
      ), by = .(REGION_GRP, YEAR, SEASON, SEX)]
      out_agg <- merge(out_agg, samp, by = c("REGION_GRP","YEAR","SEASON","SEX"), all.x = TRUE)

      sampg <- y2[, .(
        NSAMP = sum(SUM_FREQUENCY, na.rm = TRUE),
        NHAUL = data.table::uniqueN(HAUL_JOIN),
        NSTRATA = data.table::uniqueN(paste(AREA2, as.character(SEASON), GEAR))
      ), by = .(REGION_GRP, YEAR, SEASON, GEAR, SEX)]
      out_byg <- merge(out_byg, sampg, by = c("REGION_GRP","YEAR","SEASON","GEAR","SEX"), all.x = TRUE)
    }
  }

  vcat("Done.")
  list(aggregated = out_agg[], by_gear = out_byg[])
}
