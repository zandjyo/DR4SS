#' Construct catch-weighted length or age compositions for Stock Synthesis
#'
#' @description
#' Builds annual fishery length (or age) compositions by reweighting observed
#' length-frequency samples (observer + optional port) so they are proportional
#' to total catch in numbers derived from AKFIN blend catch.
#'
#' If `age_length="AGE"`, observed lengths are first converted to predicted ages
#' (bulk) using `predict_age_from_lf()` with an age predictor fit from:
#' (1) survey age data (Q3 backbone) and (2) fishery age data (quarter deltas),
#' plus shrinkage priors from fishery ages.
#'
#' If `season_def` is provided, outputs are returned by YEAR x SEASON (and by GEAR,
#' and optionally SEX), where SEASON is a user-defined mapping of months to season
#' labels (e.g., quarters, halves, custom month groupings).
#'
#' If `region_def` is provided, outputs are returned by YEAR x REGION_GRP (and
#' optionally SEASON, GEAR, SEX), where REGION_GRP is a user-defined mapping of
#' AKFIN/observer AREA codes (e.g., 500:539, 610, 620, etc.) to region labels.
#'
#' @details
#' Workflow:
#' 1) Pull observer length-frequency + species composition expansions (AKFIN).
#' 2) Optionally pull port length-frequency across eras:
#'    - 1990–1998 (AKFIN)
#'    - 1999–2007 (AKFIN lengths + AKFIN fish tickets; uses fuzzy matching)
#'    - 2008–2010 (AKFIN lengths + fuzzy matching to fish tickets)
#'    - 2011–present (AKFIN; direct landings fields)
#'    The `fuzzy_dates()` steps are retained because they are required to recover
#'    port samples that do not join to fish tickets by ID.
#' 3) Optionally filter ALL_DATA by `max_length` (removes LENGTH > max_length).
#' 4) If AGE mode, build predictor and convert LENGTH -> AGE_HAT in bulk.
#' 5) Pull blend catch (AKFIN) and convert tons to numbers using AVEWT derived
#'    from observer data with hierarchical fallback across strata.
#' 6) Compute hierarchical weights and produce compositions (optionally by season
#'    and/or user-defined region group).
#'
#' @param con_akfin DBI connection to AKFIN.
#' @param species Numeric observer species code used in observer/port length SQL.
#' @param sp_area Stock assessment area code: "BS","AI","GOA","BSWGOA".
#'   Used for default region filtering (if `region_def` is NULL) and passed to
#'   age-predictor helper functions.
#' @param start_year First year to include (must be >= 1990).
#' @param end_year Last year to include.
#' @param SEX Logical. If TRUE, compositions are sex-specific (SS 1/2/3 coding).
#' @param PORT Logical. If TRUE, include port sampling data when available.
#' @param age_length "LENGTH" or "AGE". If "AGE", lengths are converted to predicted ages.
#' @param map_sample "MAP" or "sample" used by predict_age_from_lf() when returning integer ages.
#' @param n_samples for age composition if "sample" enter the number of samples you wish to produce, output becomes a list 
#' @param max_length Optional numeric. If provided, drop rows in ALL_DATA with LENGTH > max_length
#'   (applied before AGE prediction, so it affects the predictor inputs too).
#' @param max_age Plus-group maximum age (used when age_length="AGE").
#' @param verbose Logical. If TRUE, print data availability summaries while running.
#' @param seed RNG seed when sampling (used when age_length="AGE").
#' @param season_def Optional named list mapping season labels to month integers (1..12),
#'   e.g. list(A=1:3,B=4:6,C=7:9,D=10:12) or list(A=1:6,B=7:12).
#' @param season_month Which month column to use for season mapping: "MONTH" or "MONTH_WED".
#'   Default "MONTH".
#' @param region_def Optional named list mapping REGION_GRP labels to AREA codes, e.g.
#'   listlist(BS=c(500:539),WGOA=c(610),CGOA=c(620:649),EGOA=650:659),AI=c(541:544)). If provided, outputs are by REGION_GRP.
#'   If NULL, a single REGION_GRP is used (equal to `sp_area`) and default area filtering is used.
#' @param drop_unmapped Logical. If TRUE and region_def is provided, drop rows whose AREA does not map
#'   to a REGION_GRP. Default TRUE.
#' @param wgoa_cod Logical. If TRUE, moves catch from NMFS area 620 west of -158 longitude into the 610 region group 
#' @param return_predictor Logical. if TRUE returns age at length predictor model in list
#' @return A list with two elements:
#' \describe{
#'   \item{aggregated}{data.table of YEAR x REGION_GRP x LENGTH/AGE (and optionally SEASON/SEX) with FREQ and sample summaries.}
#'   \item{by_gear}{data.table of YEAR x REGION_GRP x GEAR x LENGTH/AGE (and optionally SEASON/SEX) with FREQ and sample summaries.}
#' }
#'
#' @export
LENGTH_AGE_BY_CATCH <- function(con_akfin,
                                species,
                                sp_area,
                                start_year,
                                end_year,
                                SEX = TRUE,
                                PORT = TRUE,
                                age_length = c("LENGTH","AGE"),
                                map_sample = c("MAP","sample"),
                                n_samples = 1,
                                max_length = NULL,
                                max_age = 12L,
                                max_wt = 50L,
                                verbose = TRUE,
                                seed = 1L,
                                season_def = NULL,
                                region_def = NULL,
                                drop_unmapped = TRUE,
                                wgoa_cod =TRUE,
                                return_predictor = FALSE) {

  age_length <- match.arg(age_length)
  map_sample <- match.arg(map_sample)

  # ---- deps ----
  if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table required.", call. = FALSE)
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("lubridate required.", call. = FALSE)

  DT <- data.table::as.data.table

  # ---- basic checks ----
  if (is.null(con_akfin)) stop("`con_akfin` is required.", call. = FALSE)
  
  if (!is.numeric(species) || length(species) != 1L) stop("`species` must be a single numeric code.", call. = FALSE)
  if (!is.numeric(start_year) || length(start_year) != 1L) stop("`start_year` must be a single year.", call. = FALSE)
  if (start_year < 1990) stop("`start_year` must be >= 1990.", call. = FALSE)
  if (!is.numeric(end_year) || length(end_year) != 1L) stop("`end_year` must be a single year.", call. = FALSE)

  if (!is.null(max_length)) {
    if (!is.numeric(max_length) || length(max_length) != 1L || !is.finite(max_length) || max_length <= 0) {
      stop("`max_length` must be a single positive number (or NULL).", call. = FALSE)
    }
  }

  n_samples <- as.integer(n_samples)
  if (!is.finite(n_samples) || n_samples < 1L) stop("`n_samples` must be >= 1.", call. = FALSE)

  do_samples <- identical(map_sample, "sample") && n_samples > 1L && identical(age_length,"AGE")

  sp_area <- toupper(sp_area)

  # ---- default region vector from sp_area (used if region_def is NULL) ----
  default_region_vec <- switch(
    sp_area,
    "AI"     = 540:544,
    "GOA"    = 600:699,
    "BS"     = 500:539,
    "BSWGOA" = c(500:539, 610, 620),
    "ALL" = c(500:699),
    stop("Invalid `sp_area` (use BS, AI, GOA, BSWGOA,ALL).", call. = FALSE)
  )

  # ---- validate region_def + build region_vec ----
  if (!is.null(region_def)) {
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
  } else {
    region_vec <- default_region_vec
    region_levels <- sp_area
  }

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

  # ------------------------------------------------------------
  # 1) Observer lengths (AKFIN)
  # ------------------------------------------------------------
  lfreq <- sql_reader("dom_length.sql")
  lfreq <- sql_filter("IN", species, lfreq, flag = "-- insert species", value_type = "numeric")
  lfreq <- sql_filter("IN", species, lfreq, flag = "-- insert spec",    value_type = "numeric")
  lfreq <- sql_filter("IN", region_vec,  lfreq, flag = "-- insert region",  value_type = "numeric")
  lfreq <- sql_filter("<=", end_year, lfreq, flag = "-- insert end",    value_type = "numeric")
  lfreq <- sql_filter(">=", start_year, lfreq, flag = "-- insert start",value_type = "numeric")

  Dspcomp <- DT(sql_run(con_akfin, lfreq))
  data.table::setnames(Dspcomp, toupper(names(Dspcomp)))
  Dspcomp <- Dspcomp[EXTRAPOLATED_WEIGHT > 0 & NUMB > 0]

  if(isTRUE(wgoa_cod)){Dspcomp[AREA == 620 & LONDD_END <= -158 ]$AREA <- 610} ## moving the WGOA line in 610 to -158.
  
  # WED/MONTH_WED derived from HDAY (assumes WED() exists in your namespace)

 # robust parse of HDAY into a true Date (NOT week-ending yet)
  Dspcomp[, HDAY_DATE := as.Date(lubridate::parse_date_time(
    trimws(as.character(HDAY)),
    orders = c("Y-m-d","Y/m/d","Ymd","m/d/Y","m-d-Y","Ymd HMS","Y-m-d H:M:S"),
    tz = "UTC"
  ))]

  bad <- Dspcomp[is.na(HDAY_DATE), .N]
  if (bad > 0 && isTRUE(verbose)) message("Dropping ", bad, " rows with unparseable HDAY.")
  Dspcomp <- Dspcomp[!is.na(HDAY_DATE)]

  # now compute WED safely
  Dspcomp[, WED := WED_safe(HDAY_DATE)]

  Dspcomp[, MONTH_WED := lubridate::month(WED)]
  Dspcomp[, MONTH := as.integer(as.character(MONTH))]
  Dspcomp[, QUARTER := 4L]
  Dspcomp[MONTH < 3, QUARTER := 1L]
  Dspcomp[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
  Dspcomp[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
  Dspcomp[, AREA2 := trunc(as.numeric(AREA) / 10) * 10]
  Dspcomp[AREA2 == 500, AREA2 := 510]

  OBS_DATA <- Dspcomp[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER, MONTH_WED,
                          CRUISE, PERMIT, VES_AKR_ADFG, HAUL_JOIN, SEX, LENGTH,
                          SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]
  OBS_DATA <- OBS_DATA[YEAR >= start_year & YEAR <= end_year]
  OBS_DATA <- add_region_group(OBS_DATA, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
  summarize_by_year(OBS_DATA, "Observer length data", col = "SUM_FREQUENCY")

  # ------------------------------------------------------------
  # 2) AVEWT lookup tables from observer data (still based on AREA2/month/gear)
  # ------------------------------------------------------------
  Tspcomp <- DT(Dspcomp)

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

  # ------------------------------------------------------------
  # 3) Optional PORT data (AKFIN + AKFIN fish tickets + fuzzy_dates)
  # ------------------------------------------------------------
  port_list <- list()

  if (isTRUE(PORT)) {

    # ---- Era A: 1990–1998 (AKFIN)
    if (start_year <= 1998) {
      PAlfreq <- sql_reader("dom_length_port_A2.sql")
      PAlfreq <- sql_filter("IN", species, PAlfreq, flag = "-- insert species", value_type = "numeric")
      PAlfreq <- sql_filter("IN", species, PAlfreq, flag = "-- insert spec",    value_type = "numeric")
      PAlfreq <- sql_filter("IN", region_vec,  PAlfreq, flag = "-- insert region",  value_type = "numeric")

      PAD <- DT(sql_run(con_akfin, PAlfreq))
      data.table::setnames(PAD, toupper(names(PAD)))
      PAD <- PAD[!is.na(EXTRAPOLATED_WEIGHT) & !is.na(GEAR)]
      PAD <- add_area2_quarter(PAD)

      PAD2 <- assign_avewt(PAD, joins = avwt_joins_full,
                           priority = c("YAGM_AVE_WT","YAM_AVE_WT","YAQ_AVE_WT","YGM_AVE_WT","YGQ_AVE_WT","YG_AVE_WT"))
      PAD[, NUMB := PAD2$EXTRAPOLATED_WEIGHT / PAD2$AVEWT]

      PORTAL <- PAD[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER,
                        MONTH_WED = MONTH,
                        CRUISE, PERMIT,VES_AKR_ADFG, HAUL_JOIN, SEX, LENGTH,
                        SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]
      PORTAL <- PORTAL[YEAR >= start_year & YEAR <= end_year]
      PORTAL <- add_region_group(PORTAL, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
      port_list[["PORTAL"]] <- PORTAL
      summarize_by_year(PORTAL, "Port length data 1990–1998", col = "SUM_FREQUENCY")
    }

    # ---- fish tickets needed for eras B/D
    PBFTCKT3 <- NULL
    if (start_year <= 2010 && end_year >= 1999) {
      pb_sql <- sql_reader("fish_ticket2.sql")
      pb_sql <- sql_filter("IN", species, pb_sql, flag = "-- insert species", value_type = "character")
      PBFTCKT <- DT(sql_run(con_akfin, pb_sql))
      data.table::setnames(PBFTCKT, toupper(names(PBFTCKT)))

      PBFTCKT[, DELIVERING_VESSEL := suppressWarnings(as.numeric(DELIVERING_VESSEL))]
      PBFTCKT2 <- PBFTCKT[, .(
        TONS_LANDED = sum(TONS),
        AREA_NUMBER = .N,
        ALL_AREA = paste(unique(REPORTING_AREA_CODE), collapse = ",")
      ), by = .(AKFIN_SPECIES_CODE, AKFIN_YEAR, DELIVERY_DATE, WEEK_END_DATE, SPECIES_NAME,
                VESSEL_ID, FMP_SUBAREA, FMP_GEAR, VES_AKR_CG_NUM, VES_AKR_NAME, DELIVERING_VESSEL)]

      PBFTCKT3 <- merge(PBFTCKT, PBFTCKT2, all.x = TRUE)
    }

    # ---- Era B: 1999–2007
    if (start_year <= 2007 && end_year >= 1999) {
      PBlfreq <- sql_reader("dom_length_port_B2.sql")
      PBlfreq <- sql_filter("IN", species, PBlfreq, flag = "-- insert species", value_type = "numeric")
      PBlfreq <- sql_filter("IN", region_vec,  PBlfreq, flag = "-- insert region",  value_type = "numeric")

      PBLFREQ <- DT(sql_run(con_akfin, PBlfreq))
      data.table::setnames(PBLFREQ, toupper(names(PBLFREQ)))

      PBLFREQ[, DELIVERY_DATE := format(as.Date(DELIVERY_DATE, format = "%Y-%m-%d", origin = "1970-01-01"))]
      if (!is.null(PBFTCKT3)) {
        PBFTCKT3[, DELIVERY_DATE := format(as.Date(DELIVERY_DATE, format = "%Y%m%d", origin = "1970-01-01"))]
      }


      PBCOMB <- merge(PBLFREQ, PBFTCKT3, by = c("DELIVERY_DATE","DELIVERING_VESSEL","FISH_TICKET_NO"), all.x = TRUE)

      na_block <- DT(PBCOMB[is.na(TONS), 1:ncol(PBLFREQ)])
      #data.table::setnames(na_block, names(PBLFREQ))

      ok_block <- PBCOMB[!is.na(TONS)]

      if (nrow(na_block) > 0) {
        fuzz <- DT(fuzzy_dates(length_data = as.data.frame(na_block), Fish_ticket = PBFTCKT3, ndays = 7))
        fuzz <- fuzz[!is.na(TONS)]
        PBCOMB_all <- data.table::rbindlist(list(ok_block, fuzz), use.names = TRUE, fill = TRUE)
      } else {
        PBCOMB_all <- ok_block
      }

      if(nrow(PBCOMB_all) > 0){
        PBCOMB_all[, MONTH := as.integer(lubridate::month(DELIVERY_DATE))]
        PBCOMB_all[, QUARTER := 4L]
        PBCOMB_all[MONTH < 3, QUARTER := 1L]
        PBCOMB_all[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
        PBCOMB_all[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
        PBCOMB_all[, AREA2 := trunc(as.numeric(AREA) / 10) * 10]
        PBCOMB_all[AREA2 == 500, AREA2 := 510]

        PBCOMB5 <- assign_avewt(
          PBCOMB_all,
          joins = list(
            list(table = YAGM_AVWT, by = c("YEAR","AREA2","MONTH","GEAR")),
            list(table = YGM_AVWT,  by = c("YEAR","GEAR","MONTH")),
            list(table = YGQ_AVWT,  by = c("YEAR","GEAR","QUARTER")),
            list(table = YAM_AVWT,  by = c("YEAR","AREA2","MONTH")),
            list(table = YAQ_AVWT,  by = c("YEAR","AREA2","QUARTER")),
            list(table = YG_AVWT,   by = c("YEAR","GEAR"))
          ),
          priority = c("YAGM_AVE_WT","YAM_AVE_WT","YGQ_AVE_WT","YGM_AVE_WT","YGQ_AVE_WT","YG_AVE_WT")
       )

        PBCOMB_all[, NUMB := PBCOMB5$TONS_LANDED / (PBCOMB5$AVEWT / 1000)]
        data.table::setnames(PBCOMB_all, "DELIVERING_VESSEL", "VES_AKR_ADFG")
        PBCOMB_all[, EXTRAPOLATED_WEIGHT := TONS_LANDED * 1000]

        PORTBL <- PBCOMB_all[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER,
                                 MONTH_WED = MONTH,
                                 CRUISE, PERMIT, VES_AKR_ADFG, HAUL_JOIN, SEX, LENGTH,
                                 SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]
        PORTBL <- PORTBL[YEAR >= start_year & YEAR <= end_year]
        PORTBL <- add_region_group(PORTBL, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
        summarize_by_year(PORTBL, "Port length data 1999–2007", col = "SUM_FREQUENCY")
        } else { PORTBL <- NULL}

      port_list[["PORTBL"]] <- PORTBL
      
    }

    # ---- Era D: 2008–2010
    if (start_year <= 2010 && end_year >= 2008) {
      PDlfreq <- sql_reader("dom_length_port_D2.sql")
      PDlfreq <- sql_filter("IN", species, PDlfreq, flag = "-- insert species", value_type = "numeric")
      PDlfreq <- sql_filter("IN", region_vec,  PDlfreq, flag = "-- insert region",  value_type = "numeric")

      PDLFREQ <- DT(sql_run(con_akfin, PDlfreq))
      data.table::setnames(PDLFREQ, toupper(names(PDLFREQ)))
      PDLFREQ[, DELIVERING_VESSEL := suppressWarnings(as.numeric(DELIVERING_VESSEL))]

      if (is.null(PBFTCKT3)) stop("Fish ticket table required for 2008–2010 port era, but not available.", call. = FALSE)

      PDCOMB <- DT(fuzzy_dates(length_data = as.data.frame(PDLFREQ), Fish_ticket = PBFTCKT3, ndays = 7))
      PDCOMB[, MONTH := as.integer(lubridate::month(DELIVERY_DATE))]
      PDCOMB[, QUARTER := 4L]
      PDCOMB[MONTH < 3, QUARTER := 1L]
      PDCOMB[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
      PDCOMB[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
      PDCOMB[, AREA2 := trunc(as.numeric(AREA) / 10) * 10]
      PDCOMB[AREA2 == 500, AREA2 := 510]

      PDCOMB2 <- assign_avewt(
        PDCOMB,
        joins = list(
          list(table = YAGM_AVWT, by = c("YEAR","AREA2","MONTH","GEAR")),
          list(table = YGM_AVWT,  by = c("YEAR","GEAR","MONTH")),
          list(table = YGQ_AVWT,  by = c("YEAR","GEAR","QUARTER")),
          list(table = YAM_AVWT,  by = c("YEAR","AREA2","MONTH")),
          list(table = YG_AVWT,   by = c("YEAR","GEAR"))
        ),
        priority = c("YAGM_AVE_WT","YGQ_AVE_WT","YGM_AVE_WT","YGQ_AVE_WT","YG_AVE_WT")
      )

      PDCOMB[, NUMB := PDCOMB2$TONS_LANDED / (PDCOMB2$AVEWT / 1000)]
      data.table::setnames(PDCOMB, "DELIVERING_VESSEL", "VES_AKR_ADFG")
      PDCOMB[, EXTRAPOLATED_WEIGHT := TONS_LANDED * 1000]

      PORTDL <- PDCOMB[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER,
                           MONTH_WED = MONTH,
                           CRUISE, PERMIT,VES_AKR_ADFG, HAUL_JOIN, SEX, LENGTH,
                           SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]
      PORTDL <- PORTDL[YEAR >= start_year & YEAR <= end_year]
      PORTDL <- add_region_group(PORTDL, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
      port_list[["PORTDL"]] <- PORTDL
      summarize_by_year(PORTDL, "Port length data 2008–2010", col = "SUM_FREQUENCY")
    }

    # ---- Era C: 2011–present
    if (end_year >= 2011) {
      PClfreq <- sql_reader("dom_length_port_C2.sql")
      PClfreq <- sql_filter("IN", species, PClfreq, flag = "-- insert species",             value_type = "numeric")
      PClfreq <- sql_filter("IN", region_vec,  PClfreq, flag = "-- insert region",              value_type = "numeric")

      PCD <- DT(sql_run(con_akfin, PClfreq))
      data.table::setnames(PCD, toupper(names(PCD)))
      PCD <- PCD[!is.na(TONS_LANDED)]
      PCD <- add_area2_quarter(PCD)

      PCD2 <- assign_avewt(PCD, joins = avwt_joins_full,
                           priority = c("YAGM_AVE_WT","YAM_AVE_WT","YAQ_AVE_WT","YGM_AVE_WT","YGQ_AVE_WT","YG_AVE_WT"))

      PCD[, NUMB := PCD2$TONS_LANDED / (PCD2$AVEWT / 1000)]
      PCD$VES_AKR_ADFG <- PCD$PERMIT
      PCD[, EXTRAPOLATED_WEIGHT := TONS_LANDED * 1000]

      PORTCL <- PCD[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER,
                        MONTH_WED = MONTH,
                        CRUISE, PERMIT,VES_AKR_ADFG, HAUL_JOIN, SEX, LENGTH,
                        SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]
      PORTCL <- PORTCL[YEAR >= start_year & YEAR <= end_year]
      PORTCL <- add_region_group(PORTCL, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
      port_list[["PORTCL"]] <- PORTCL
      summarize_by_year(PORTCL, paste0("Port length data 2011–", end_year), col = "SUM_FREQUENCY")
    }
  }

  PORT_DATA <- if (length(port_list) > 0) data.table::rbindlist(port_list, use.names = TRUE, fill = TRUE) else NULL
  if (!is.null(PORT_DATA)) summarize_by_year(PORT_DATA, "Port data (combined)", col = "SUM_FREQUENCY")

  ALL_DATA <- if (!is.null(PORT_DATA)) {
    data.table::rbindlist(list(OBS_DATA, PORT_DATA), use.names = TRUE, fill = TRUE)
  } else {
    OBS_DATA
  }
  if (nrow(ALL_DATA) == 0) stop("No fishery length data available for this request.", call. = FALSE)

  # ------------------------------------------------------------
  # 3b) Optional max_length filter on ALL_DATA (pre-AGE QC)
  # ------------------------------------------------------------
  if (!is.null(max_length)) {
    before_n <- nrow(ALL_DATA)
    ALL_DATA <- ALL_DATA[is.finite(LENGTH) & LENGTH <= max_length]
    after_n <- nrow(ALL_DATA)
    if (isTRUE(verbose)) message("Applied max_length=", max_length, ": kept ", after_n, " / ", before_n, " rows in ALL_DATA.")
    if (after_n == 0) stop("After applying `max_length`, ALL_DATA has 0 rows.", call. = FALSE)
  }


  # ------------------------------------------------------------
  # 4) If AGE, build predictor and convert LENGTH -> AGE_HAT in bulk
  # ------------------------------------------------------------
  if (age_length == "AGE") {

    if(!is.null(region_def)){ sp_area <- "ALL"}

    srv_age <- GET_SURV_AGE(
      con_akfin = con_akfin,
      area      = sp_area,
      species   = species,
      start_yr  = start_year,
      max_age   = max_age
    )
    srv_age <- DT(srv_age)
    if (nrow(srv_age) == 0) stop("No survey age data available for this area/date range.", call. = FALSE)

    

    srv_age <- add_nmfs_area_to_survey(
      survey_dt = srv_age,
      lon_col   = "LONGITUDE_DD_END",
      lat_col   = "LATITUDE_DD_END",
      join      = "within"
    )

   
    #srv_age<-srv_age[NMFS_AREA %in% all_areas]

    srv_age<-add_region_group(srv_age, region_def = region_def, area_col = "NMFS_AREA", drop_unmapped = drop_unmapped)

    if(isTRUE(wgoa_cod)){srv_age[NMFS_AREA==620 & LONGITUDE_DD_END <= -158]$NMFS_AREA <- 610} ## for WGOA Pcod

    if (isTRUE(verbose)) {
      message("Survey ages (rows with non-missing AGE by YEAR):")
      print(srv_age[!is.na(AGE), .(N = .N), by = .(YEAR,REGION_GRP)][order(YEAR)])
    }
    

    fish_raw <- get_fishery_age_wt_data(
                  con=con_akfin,
                  species=species,
                  season_def=season_def,
                  region_def = region_def,
                  start_year = start_year,
                  end_year = end_year,
                  wgoa_cod=wgoa_cod,
                  max_wt=50,
                  drop_unmapped=drop_unmapped
    )
    fish_raw <- DT(fish_raw)
    
    if (nrow(fish_raw[!is.na(AGE)]) == 0) stop("No fishery age data available for age predictor.", call. = FALSE)

    if (isTRUE(verbose)) {
      message("Fishery ages (rows with non-missing AGE by YEAR):")
      print(fish_raw[!is.na(AGE), .(N = .N), by = .(YEAR,REGION_GRP)][order(YEAR)])

      obs_yrs <- ALL_DATA[, sort(unique(YEAR))]
      fsh_age_yrs <- sort(unique(fish_raw$YEAR))
      missing <- setdiff(obs_yrs, fsh_age_yrs)
      if (length(missing) > 0) {
        cat(
          "Note! There are no fishery ages and all age-length relationships are derived from survey data for years:\n",
          paste(missing, collapse = ", "),
          "\n"
        )
      }
    }

    age_pred <- fit_age_predictor(
      fish_dt    = fish_raw,
      srv_dt     = srv_age,
      maxage     = max_age,
      min_n_cell = 30L,
      prior_mix  = 0.7
    )
 
  
  if (do_samples) {
    # produce a list of sampled ALL_DATA draws
    ALL_DATA_list <- lapply(seq_len(n_samples), function(i) {
      predict_age_from_lf(
        lf_dt         = ALL_DATA,
        predictor     = age_pred,
        target        = "row_age",
        map_or_sample = "sample",
        seed          = as.integer(seed + i - 1L)
      )
    })

    # convert predicted age -> LENGTH (as integer bin) for each draw
    ALL_DATA_list <- lapply(ALL_DATA_list, function(ad) {
      ad <- data.table::as.data.table(ad)
      ad[, LENGTH := as.integer(AGE_HAT)]
      ad
    })

    ALL_DATA <- ALL_DATA_list
    rm(ALL_DATA_list)

  } else {
    # single deterministic draw (MAP) or single stochastic sample
    ALL_DATA <- predict_age_from_lf(
      lf_dt         = ALL_DATA,
      predictor     = age_pred,
      target        = "row_age",
      map_or_sample = map_sample,
      seed          = seed
    )
    ALL_DATA[, LENGTH := as.integer(AGE_HAT)]
  }
    

  }

  # ------------------------------------------------------------
  # 5) Blend catch -> numbers (AKFIN)
  # ------------------------------------------------------------
  catch_sql <- sql_reader("dom_catch3.sql")
  catch_sql <- sql_filter("<=", end_year,   catch_sql, flag = "-- insert eyear", value_type = "numeric")
  catch_sql <- sql_filter(">=", start_year, catch_sql, flag = "-- insert syear", value_type = "numeric")
  catch_sql <- sql_filter("IN", species, catch_sql, flag = "-- insert species", value_type = "character")
  catch_sql <- sql_filter("IN", region_vec, catch_sql, flag = "-- insert area", value_type = "character")

  CATCHT <- DT(sql_run(con_akfin, catch_sql))
  data.table::setnames(CATCHT, toupper(names(CATCHT)))
  CATCHT <- CATCHT[YEAR >= start_year & YEAR <= end_year][TONS > 0]

  if(isTRUE(wgoa_cod)){ CATCHT[AREA==620 & ADFG_AREA<= 580000]$AREA <- 610}  ## for WGOA Pcod

  CATCHT[, AREA := suppressWarnings(as.integer(as.character(AREA)))]
  CATCHT <- add_region_group(CATCHT, region_def = region_def, area_col = "AREA", drop_unmapped = drop_unmapped)
  if (nrow(CATCHT) == 0) stop("No blend catch rows after region mapping/filtering.", call. = FALSE)

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
  CATCHT4[, SPECIES := as.numeric(SPECIES)]

  # ------------------------------------------------------------
  # 6) Length data prep for weighting
  # ------------------------------------------------------------
  compute_comps_one <- function(ALL_DATA_one) {
    Length <- data.table::as.data.table(ALL_DATA_one)[GEAR %in% c("POT","TRW","HAL")]
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
        grid1 <- data.table::CJ(REGION_GRP = regions, YEAR = years, LENGTH = 1:maxL, unique = TRUE)
        out_agg <- merge(grid1, out_agg, by = c("REGION_GRP","YEAR","LENGTH"), all.x = TRUE)
        out_agg[is.na(FREQ), FREQ := 0]

        grid2 <- data.table::CJ(REGION_GRP = regions,
                                YEAR = sort(unique(out_byg$YEAR)),
                                GEAR = sort(unique(out_byg$GEAR)),
                                LENGTH = 1:maxL)
        out_byg <- merge(grid2, out_byg, by = c("REGION_GRP","YEAR","GEAR","LENGTH"), all.x = TRUE)
        out_byg[is.na(FREQ), FREQ := 0]
      } else {
        sex_levels <- sort(unique(out_agg$SEX))
        grid1 <- data.table::CJ(REGION_GRP = regions, YEAR = years, SEX = sex_levels, LENGTH = 1:maxL)
        out_agg <- merge(grid1, out_agg, by = c("REGION_GRP","YEAR","SEX","LENGTH"), all.x = TRUE)
        out_agg[is.na(FREQ), FREQ := 0]

        grid2 <- data.table::CJ(REGION_GRP = regions,
                                YEAR = sort(unique(out_byg$YEAR)),
                                GEAR = sort(unique(out_byg$GEAR)),
                                SEX = sex_levels,
                                LENGTH = 1:maxL)
        out_byg <- merge(grid2, out_byg, by = c("REGION_GRP","YEAR","GEAR","SEX","LENGTH"), all.x = TRUE)
        out_byg[is.na(FREQ), FREQ := 0]
      }
    } else {
      seas_levels <- levels(y2$SEASON)
      if (!isTRUE(SEX)) {
        grid1 <- data.table::CJ(REGION_GRP = regions, YEAR = years, SEASON = seas_levels, LENGTH = 1:maxL)
        out_agg <- merge(grid1, out_agg, by = c("REGION_GRP","YEAR","SEASON","LENGTH"), all.x = TRUE)
        out_agg[is.na(FREQ), FREQ := 0]
        out_agg[, SEASON := factor(SEASON, levels = seas_levels)]

        grid2 <- data.table::CJ(REGION_GRP = regions,
                                YEAR = sort(unique(out_byg$YEAR)),
                                SEASON = seas_levels,
                                GEAR = sort(unique(out_byg$GEAR)),
                                LENGTH = 1:maxL)
        out_byg <- merge(grid2, out_byg, by = c("REGION_GRP","YEAR","SEASON","GEAR","LENGTH"), all.x = TRUE)
        out_byg[is.na(FREQ), FREQ := 0]
        out_byg[, SEASON := factor(SEASON, levels = seas_levels)]
      } else {
        sex_levels <- sort(unique(out_agg$SEX))
        grid1 <- data.table::CJ(REGION_GRP = regions, YEAR = years, SEASON = seas_levels, SEX = sex_levels, LENGTH = 1:maxL)
        out_agg <- merge(grid1, out_agg, by = c("REGION_GRP","YEAR","SEASON","SEX","LENGTH"), all.x = TRUE)
        out_agg[is.na(FREQ), FREQ := 0]
        out_agg[, SEASON := factor(SEASON, levels = seas_levels)]

        grid2 <- data.table::CJ(REGION_GRP = regions,
                                YEAR = sort(unique(out_byg$YEAR)),
                                SEASON = seas_levels,
                                GEAR = sort(unique(out_byg$GEAR)),
                                SEX = sex_levels,
                                LENGTH = 1:maxL)
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

    if (age_length == "AGE") {
      data.table::setnames(out_byg, "LENGTH", "AGE")
      data.table::setnames(out_agg, "LENGTH", "AGE")
    }
    
    list(aggregated = out_agg[], by_gear = out_byg[])
  }


  wrap_out <- function(x) {
    if (!isTRUE(return_predictor) && age_length=="AGE") return(x)
    list(
      output = x,
      age_predictor = age_pred
    )
  }

  if (do_samples) {
    out_list <- lapply(seq_len(n_samples), function(i) {
      if (isTRUE(verbose)) message("Compositions: sample ", i, " / ", n_samples)
      compute_comps_one(ALL_DATA[[i]])
    })
    vcat("Done.")
    return(wrap_out(out_list))
  } else {
    out <- compute_comps_one(ALL_DATA)
    vcat("Done.")
    return(wrap_out(out))
  }

}