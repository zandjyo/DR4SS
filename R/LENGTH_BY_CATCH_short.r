#' Construct catch-weighted length compositions for Stock Synthesis
#'
#' @description
#' Builds annual fishery length compositions by reweighting observed length
#' frequencies from at-sea observer and (optionally) port sampling so that they
#' are proportional to total catch in numbers derived from blend catch data.
#' Output is suitable for use as length composition input to Stock Synthesis,
#' either as a single aggregated fishery or split by gear.
#'
#' @details
#' This function combines length-frequency observations, species composition
#' expansions, and blend catch information to estimate length compositions that
#' reflect total fishery removals in numbers.
#'
#' The workflow is:
#' \enumerate{
#'   \item Pull observer and (optionally) port-based length-frequency data and
#'   associated species composition expansions that provide an estimated number
#'   of fish represented by each haul or delivery.
#'   \item Pull domestic blend catch (and historical foreign catch, where
#'   applicable), convert catch weight (tons) to numbers using an average weight
#'   (AVEWT) estimated from observer data.
#'   \item Distribute catch numbers across lengths using a hierarchical weighting
#'   scheme that accounts for within-haul length proportions, haul-level
#'   representation within strata, and stratum-level contributions to total
#'   catch.
#'   \item Produce annual length compositions either as a single aggregated
#'   fishery or split by gear (HAL, POT, TRW).
#'   \item Calculate auxiliary sample-size summaries (number of samples, hauls,
#'   and strata); bootstrap effective sample sizes are retained as placeholders
#'   for consistency with legacy implementations.
#' }
#'
#' Length compositions are constructed on an integer length grid and are
#' normalized to sum to one within each year (and gear, if applicable). Strata
#' with insufficient observed sample size are excluded.
#'
#' @section Mathematical formulation:
#'
#' Let a stratum be indexed by
#' \deqn{s \equiv (y, a, g, m)}
#' where \eqn{y} is year, \eqn{a} is area (AREA2), \eqn{g} is gear, and \eqn{m} is
#' month. Let \eqn{h} index hauls or deliveries within stratum \eqn{s}, \eqn{l}
#' index integer length, and \eqn{k} index sex when \code{SEX = TRUE}.
#'
#' Observed length frequency in haul \eqn{h}:
#' \deqn{f_{h,l,(k)}}
#'
#' Total observed frequency in haul \eqn{h}:
#' \deqn{F_h = \sum_l f_{h,l,(k)}}
#'
#' Estimated number of fish represented by haul \eqn{h}:
#' \deqn{N_h}
#'
#' Total estimated numbers in stratum \eqn{s}:
#' \deqn{N_s = \sum_{h \in s} N_h}
#'
#' Total catch in numbers from blend catch in stratum \eqn{s}:
#' \deqn{C_s}
#'
#' with totals
#' \deqn{C_{y,g} = \sum_{a,m} C_{y,a,g,m}, \qquad
#'       C_y = \sum_{a,g,m} C_{y,a,g,m}}
#'
#' Within-haul length proportion:
#' \deqn{p_{h,l,(k)} = \frac{f_{h,l,(k)}}{F_h}}
#'
#' Haul weight within stratum:
#' \deqn{w_{h \mid s} = \frac{N_h}{N_s}}
#'
#' For the single aggregated fishery model (all gears pooled within year),
#' stratum weight is:
#' \deqn{w^{single}_{s \mid y} = \frac{C_s}{C_y}}
#'
#' and the contribution of haul \eqn{h} at length \eqn{l} is:
#' \deqn{
#' W^{single}_{h,l,(k)} =
#' p_{h,l,(k)} \cdot w_{h \mid s} \cdot w^{single}_{s \mid y}
#' }
#'
#' For the split-by-gear model, stratum weight is:
#' \deqn{w^{gear}_{s \mid y,g} = \frac{C_s}{C_{y,g}}}
#'
#' and the contribution becomes:
#' \deqn{
#' W^{gear}_{h,l,(k)} =
#' p_{h,l,(k)} \cdot w_{h \mid s} \cdot w^{gear}_{s \mid y,g}
#' }
#'
#' Final length compositions are obtained by summing the appropriate weights
#' across hauls and strata and normalizing so that proportions sum to one.
#'
#' Catch weight is converted to numbers using:
#' \deqn{
#' \hat{N} = \frac{\text{tons landed}}{\bar{w} / 1000}
#' }
#' where \eqn{\bar{w}} is the average individual weight estimated from observer
#' data using a hierarchical fallback across strata.
#'
#' Only strata with at least 30 observed length counts are retained.
#'
#' @param species Observer species code.
#' @param species_catch Catch accounting species identifier used in blend catch.
#' @param for_species_catch Foreign catch species identifier.
#' @param sp_area Stock assessment area (e.g., "BS", "GOA", "AI").
#' @param ly Final year to include.
#' @param SEX Logical; if \code{TRUE}, length compositions are sex-specific.
#' @param PORT Logical; if \code{TRUE}, port sampling data are included.
#' @param use_foreign Logical; if \code{TRUE}, foreign fishery data are included.
#'
#' @return
#' A list with two elements:
#' \describe{
#'   \item{aggregated}{Annual length compositions for a single aggregated fishery.}
#'   \item{by_gear}{Annual length compositions split by gear.}
#' }
#'
#' @references
#' Alaska Fisheries Science Center. Stock assessment length composition
#' construction methods used in recent North Pacific groundfish assessments.
#'
#' @export
LENGTH_BY_CATCH_short <- function(con_akfin,
                                  con_afsc,
                                  species,
                                  species_catch,
                                  for_species_catch,
                                  sp_area,
                                  ly,
                                  SEX = TRUE,
                                  PORT = TRUE,
                                  use_foreign = TRUE) {

  # ---- package/import checks (lightweight) ----
  for (pkg in c("data.table", "dplyr", "lubridate")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Package '", pkg, "' is required.", call. = FALSE)
    }
  }

  if (missing(con_akfin) || is.null(con_akfin)) stop("`con_akfin` is required.", call. = FALSE)
  if (missing(con_afsc) || is.null(con_afsc)) stop("`con_afsc` is required.", call. = FALSE)

  if (missing(species) || length(species) != 1L || !is.numeric(species)) {
    stop("`species` must be a single numeric code.", call. = FALSE)
  }
  if (missing(species_catch) || length(species_catch) != 1L) stop("`species_catch` is required.", call. = FALSE)
  if (missing(for_species_catch) || length(for_species_catch) != 1L) stop("`for_species_catch` is required.", call. = FALSE)
  if (missing(sp_area) || length(sp_area) != 1L) stop("`sp_area` is required.", call. = FALSE)
  if (missing(ly) || length(ly) != 1L || !is.numeric(ly)) stop("`ly` must be a single numeric year.", call. = FALSE)

  sp_area <- toupper(sp_area)

  region <- switch(
    sp_area,
    "AI"  = c(540:544),
    "GOA" = c(600:699),
    "BS"  = c(500:539),
    "BSWGOA" =c(500:539,610,620),
    stop("Invalid `sp_area` (use BS, AI, GOA, BSWGOA).", call. = FALSE)
  )

  # --------------------------
  # Helper: add AREA2 + QUARTER (preserve your exact rules)
  # --------------------------
  add_area2_quarter <- function(DT) {
    DT <- data.table::as.data.table(DT)
    DT[, MONTH := as.numeric(MONTH)]
    DT[, QUARTER := floor((as.numeric(MONTH) / 3) - 0.3) + 1]
    DT[, AREA2 := trunc(AREA / 10) * 10]
    DT[AREA2 == 500, AREA2 := 510]
    DT
  }

  # --------------------------
  # Helper: merge AVWT tables + compute AVEWT using a caller-specified priority
  # (priority differs among your blocks; we keep those differences to preserve outputs)
  # --------------------------
  assign_avewt <- function(DT, joins, priority) {
    DT <- data.table::as.data.table(DT)
    for (j in joins) {
      DT <- merge(DT, j$table, all.x = TRUE, by = j$by)
    }
    DT[, AVEWT := get(priority[1])]
    if (length(priority) > 1) {
      for (nm in priority[-1]) {
        DT[is.na(AVEWT), AVEWT := get(nm)]
      }
    }
    DT
  }

  # ---- Pull domestic length/species-comp (AKFIN) ----
  lfreq <- sql_reader("dom_length.sql")
  lfreq <- sql_filter(sql_precode = "IN", x = species, sql_code = lfreq, flag = "-- insert species",value_type = c("numeric"))
  lfreq <- sql_filter(sql_precode = "IN", x = species, sql_code = lfreq, flag = "-- insert spec",value_type = c("numeric"))
  lfreq <- sql_filter(sql_precode ="IN", x = region, sql_code = lfreq, flag = "-- insert region",value_type = c("numeric"))

  Dspcomp <- sql_run(con_akfin, lfreq) |>
    data.table::as.data.table()
  data.table::setnames(Dspcomp, toupper(names(Dspcomp)))
  Dspcomp <- Dspcomp[EXTRAPOLATED_WEIGHT > 0 & NUMB > 0]
  Dspcomp[, QUARTER := floor((as.numeric(MONTH) / 3) - 0.3) + 1]

  # ---- Pull foreign length/species-comp (AFSC) ----
  if(use_foreign){
  Flfreq <- sql_reader("for_length.sql")

  Flfreq <- sql_filter(sql_precode = "IN", x = species, sql_code = Flfreq, flag = "-- insert species",value_type = c("numeric"))
  Flfreq <- sql_filter(sql_precode ="IN", x = region, sql_code = Flfreq, flag = "-- insert region",value_type = c("numeric"))

  Fspcomp <- sql_run(con_afsc, Flfreq) |>
    data.table::as.data.table()
  data.table::setnames(Fspcomp, toupper(names(Fspcomp)))
  Fspcomp[, MONTH := as.numeric(MONTH)]
  Fspcomp[, QUARTER := floor((MONTH / 3) - 0.3) + 1]

  # ---- Combine observer sources ----
  Tspcomp <- data.table::rbindlist(list(Fspcomp, Dspcomp), use.names = TRUE, fill = TRUE)
  } else Tspcomp <- Dspcomp

  Tspcomp[, WED := WED(HDAY)]
  Tspcomp[, MONTH_WED := lubridate::month(WED)]
  Tspcomp[, MONTH := as.numeric(MONTH)]
  Tspcomp[, AREA2 := trunc(AREA / 10) * 10]
  Tspcomp[AREA2 == 500, AREA2 := 510]
  Tspcomp <- Tspcomp[EXTRAPOLATED_WEIGHT > 0 & NUMB > 0]

  OBS_DATA <- Tspcomp[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER, CRUISE, VES_AKR_ADFG,
                          HAUL_JOIN, SEX, LENGTH, SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]

  # ---- AVEWT tables (as in your original) ----
  YAGM_AVWT <- Tspcomp[, .(YAGM_AVE_WT = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, AREA2, MONTH, GEAR)]
  YAM_AVWT  <- Tspcomp[, .(YAM_AVE_WT  = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, AREA2, MONTH)]
  YGM_AVWT  <- Tspcomp[, .(YGM_AVE_WT  = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, GEAR, MONTH)]
  YGQ_AVWT  <- Tspcomp[, .(YGQ_AVE_WT  = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, GEAR, QUARTER)]
  YAQ_AVWT  <- Tspcomp[, .(YAQ_AVE_WT  = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, AREA2, QUARTER)]
  YG_AVWT   <- Tspcomp[, .(YG_AVE_WT   = sum(EXTRAPOLATED_WEIGHT) / sum(NUMB)), by = .(YEAR, GEAR)]

  # enforce numeric types like your original
  for (DT in list(Tspcomp, YAGM_AVWT, YAM_AVWT, YGM_AVWT)) {
    DT[, YEAR := as.numeric(YEAR)]
    DT[, MONTH := as.numeric(MONTH)]
  }
  for (DT in list(YAQ_AVWT, YGQ_AVWT)) {
    DT[, YEAR := as.numeric(YEAR)]
    DT[, QUARTER := as.numeric(QUARTER)]
  }
  YG_AVWT[, YEAR := as.numeric(YEAR)]

  # joins spec used by assign_avewt()
  avwt_joins_full <- list(
    list(table = YAGM_AVWT, by = c("YEAR", "AREA2", "MONTH", "GEAR")),
    list(table = YGM_AVWT,  by = c("YEAR", "GEAR", "MONTH")),
    list(table = YGQ_AVWT,  by = c("YEAR", "GEAR", "QUARTER")),
    list(table = YAM_AVWT,  by = c("YEAR", "AREA2", "MONTH")),
    list(table = YAQ_AVWT,  by = c("YEAR", "AREA2", "QUARTER")),
    list(table = YG_AVWT,   by = c("YEAR", "GEAR"))
  )

  # ---- PORT handling (preserve your era logic) ----
  if (isTRUE(PORT)) {

    # ---- A: 1990–1998 ----
    PAlfreq <- sql_reader("dom_length_port_A.sql")
    PAlfreq <- sql_filter(sql_precode = "IN", x=species, sql_code = PAlfreq, flag = "-- insert species",value_type = c("numeric"))
    PAlfreq <- sql_filter(sql_precode = "IN", x=species, sql_code = PAlfreq, flag = "-- insert spec",value_type = c("numeric"))
    PAlfreq <- sql_filter(sql_precode ="IN", x = region, sql_code = PAlfreq, flag = "-- insert region",value_type = c("numeric"))

    PADspcomp <- sql_run(con_afsc, PAlfreq) |>
      data.table::as.data.table()
    data.table::setnames(PADspcomp, toupper(names(PADspcomp)))
    PADspcomp <- PADspcomp[!is.na(EXTRAPOLATED_WEIGHT) & !is.na(GEAR)]
    PADspcomp <- add_area2_quarter(PADspcomp)

    PADspcomp2 <- assign_avewt(
      PADspcomp,
      joins = avwt_joins_full,
      priority = c("YAGM_AVE_WT", "YAM_AVE_WT", "YAQ_AVE_WT", "YGM_AVE_WT", "YGQ_AVE_WT", "YG_AVE_WT")
    )
    data.table::setorder(PADspcomp2, HDAY, CRUISE, PERMIT, HAUL_JOIN, SEX, LENGTH)
    data.table::setorder(PADspcomp,  HDAY, CRUISE, PERMIT, HAUL_JOIN, SEX, LENGTH)

    PADspcomp[, NUMB := PADspcomp2$EXTRAPOLATED_WEIGHT / PADspcomp2$AVEWT]
    PORTAL <- PADspcomp[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER, CRUISE, VES_AKR_ADFG,
                            HAUL_JOIN, SEX, LENGTH, SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]

    # ---- B: 1999–2007 ----
    PBlfreq <- sql_reader("dom_length_port_B.sql")
    PBlfreq <- sql_filter("IN", species, PBlfreq, flag = "-- insert species",value_type = c("numeric"))
    PBlfreq <- sql_filter(sql_precode ="IN", x = region, sql_code = PBlfreq, flag = "-- insert region",value_type = c("numeric"))

    PBLFREQ <- sql_run(con_afsc, PBlfreq) |>
      data.table::as.data.table()
    data.table::setnames(PBLFREQ, toupper(names(PBLFREQ)))

    PBftckt <- sql_reader("fish_ticket.sql")
    PBftckt <- sql_filter("IN", species_catch, PBftckt, flag = "-- insert species", value_type = c("character"))
    PBFTCKT <- sql_run(con_akfin, PBftckt) |>
      data.table::as.data.table()
    data.table::setnames(PBFTCKT, toupper(names(PBFTCKT)))

    PBLFREQ[, DELIVERING_VESSEL := as.numeric(DELIVERING_VESSEL)]
    PBFTCKT[, DELIVERING_VESSEL := as.numeric(DELIVERING_VESSEL)]

    PBFTCKT2 <- PBFTCKT[, .(
      TONS_LANDED = sum(TONS),
      AREA_NUMBER = length(REPORTING_AREA_CODE),
      ALL_AREA = paste(unique(REPORTING_AREA_CODE), collapse = ",")
    ), by = .(AKFIN_SPECIES_CODE, AKFIN_YEAR, DELIVERY_DATE, WEEK_END_DATE, SPECIES_NAME,
              VESSEL_ID, FMP_SUBAREA, FMP_GEAR, VES_AKR_CG_NUM, VES_AKR_NAME,
              DELIVERING_VESSEL)]

    PBFTCKT3 <- merge(PBFTCKT, PBFTCKT2, all.x = TRUE)

    PBLFREQ[, DELIVERY_DATE := format(as.Date(DELIVERY_DATE, format = "%Y-%m-%d", origin = "1970-01-01"))]
    PBFTCKT3[, DELIVERY_DATE := format(as.Date(DELIVERY_DATE, format = "%Y%m%d", origin = "1970-01-01"))]

    PBCOMB <- merge(PBLFREQ, PBFTCKT3, by = "FISH_TICKET_NO", all.x = TRUE)
    data.table::setnames(PBCOMB, c("DELIVERY_DATE.x", "DELIVERING_VESSEL.x"), c("DELIVERY_DATE", "DELIVERING_VESSEL"))

    NAPBCOMB <- data.table::as.data.table(PBCOMB[is.na(TONS), 1:16])
    data.table::setnames(NAPBCOMB, names(PBLFREQ))

    PBCOMB2 <- PBCOMB[!is.na(TONS)]
    PBCOMB2[, `:=`(FISH_TICKET_NO.y = FISH_TICKET_NO, date_diff = Sys.Date() - Sys.Date())]

    PBCOMB3 <- fuzzy_dates(length_data = as.data.frame(NAPBCOMB), Fish_ticket = PBFTCKT3, ndays = 7)
    PBCOMB3 <- data.table::as.data.table(PBCOMB3)
    PBCOMB3 <- PBCOMB3[, names(PBCOMB2), with = FALSE]

    PBCOMB4 <- data.table::rbindlist(list(PBCOMB2, PBCOMB3[!is.na(TONS)]), use.names = TRUE, fill = TRUE)
    PBCOMB4[, MONTH := as.numeric(lubridate::month(DELIVERY_DATE))]
    PBCOMB4[, QUARTER := trunc((MONTH / 3) - 0.3) + 1]
    PBCOMB4[, AREA2 := trunc(AREA / 10) * 10]
    PBCOMB4[AREA2 == 500, AREA2 := 510]

    # NOTE: keep your original PB priority (even though it repeats YGQ and omits YAQ)
    PBCOMB5 <- assign_avewt(
      PBCOMB4,
      joins = list(
        list(table = YAGM_AVWT, by = c("YEAR", "AREA2", "MONTH", "GEAR")),
        list(table = YGM_AVWT,  by = c("YEAR", "GEAR", "MONTH")),
        list(table = YGQ_AVWT,  by = c("YEAR", "GEAR", "QUARTER")),
        list(table = YAM_AVWT,  by = c("YEAR", "AREA2", "MONTH")),
        list(table = YAQ_AVWT,  by = c("YEAR", "AREA2", "QUARTER")),
        list(table = YG_AVWT,   by = c("YEAR", "GEAR"))
      ),
      priority = c("YAGM_AVE_WT", "YAM_AVE_WT", "YGQ_AVE_WT", "YGM_AVE_WT", "YGQ_AVE_WT", "YG_AVE_WT")
    )

    data.table::setorder(PBCOMB5, DELIVERY_DATE, CRUISE, DELIVERING_VESSEL, HAUL_JOIN, SPECIES, SEX, LENGTH)
    data.table::setorder(PBCOMB4, DELIVERY_DATE, CRUISE, DELIVERING_VESSEL, HAUL_JOIN, SPECIES, SEX, LENGTH)

    PBCOMB4[, NUMB := PBCOMB5$TONS_LANDED / (PBCOMB5$AVEWT / 1000)]
    data.table::setnames(PBCOMB4, "DELIVERING_VESSEL", "VES_AKR_ADFG")
    PBCOMB4[, EXTRAPOLATED_WEIGHT := TONS_LANDED * 1000]

    PORTBL <- PBCOMB4[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER, CRUISE, VES_AKR_ADFG,
                          HAUL_JOIN, SEX, LENGTH, SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]

     # ---- C: 2011–present ----
    PClfreq <- sql_reader("dom_length_port_C.sql")
    PClfreq <- sql_filter("IN", species, PClfreq, flag = "-- insert species",value_type = c("numeric"))
    PClfreq <- sql_filter("IN", species_catch, PClfreq, flag = "-- insert catch_species",value_type = c("character"))
    PClfreq <- sql_filter(sql_precode ="IN", x = region, sql_code = PClfreq, flag = "-- insert region",value_type = c("numeric"))

    PCDspcomp <- sql_run(con_afsc, PClfreq) |>
      data.table::as.data.table()
    data.table::setnames(PCDspcomp, toupper(names(PCDspcomp)))
    PCDspcomp <- PCDspcomp[!is.na(TONS_LANDED)]
    PCDspcomp <- add_area2_quarter(PCDspcomp)

    PCDspcomp2 <- assign_avewt(
      PCDspcomp,
      joins = avwt_joins_full,
      priority = c("YAGM_AVE_WT", "YAM_AVE_WT", "YAQ_AVE_WT", "YGM_AVE_WT", "YGQ_AVE_WT", "YG_AVE_WT")
    )

    data.table::setorder(PCDspcomp2, HDAY, CRUISE, PERMIT, HAUL_JOIN, SEX, LENGTH)
    data.table::setorder(PCDspcomp,  HDAY, CRUISE, PERMIT, HAUL_JOIN, SEX, LENGTH)

    PCDspcomp[, NUMB := PCDspcomp2$TONS_LANDED / (PCDspcomp2$AVEWT / 1000)]
    data.table::setnames(PCDspcomp, "PERMIT", "VES_AKR_ADFG")
    PCDspcomp[, EXTRAPOLATED_WEIGHT := TONS_LANDED * 1000]

    PORTCL <- PCDspcomp[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER, CRUISE, VES_AKR_ADFG,
                            HAUL_JOIN, SEX, LENGTH, SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]

    # ---- D: 2008–2010 ----
    PDlfreq <- sql_reader("dom_length_port_D.sql")
    PDlfreq <- sql_filter("IN", species, PDlfreq, flag = "-- insert species",value_type = c("numeric"))
    PDlfreq <- sql_filter(sql_precode ="IN", x = region, sql_code = PDlfreq, flag = "-- insert region",value_type = c("numeric"))
    
    PDLFREQ <- sql_run(con_afsc, PDlfreq) |>
      data.table::as.data.table()
    data.table::setnames(PDLFREQ, toupper(names(PDLFREQ)))
    PDLFREQ[, DELIVERING_VESSEL := as.numeric(DELIVERING_VESSEL)]

    # fuzzy match against PBFTCKT3 (created above in Era B)
    PDCOMB <- fuzzy_dates(length_data = as.data.frame(PDLFREQ), Fish_ticket = PBFTCKT3, ndays = 7) |>
      data.table::as.data.table()

    PDCOMB[, MONTH := as.numeric(lubridate::month(DELIVERY_DATE))]
    PDCOMB[, QUARTER := trunc((MONTH / 3) - 0.3) + 1]
    PDCOMB[, AREA2 := trunc(AREA / 10) * 10]
    PDCOMB[AREA2 == 500, AREA2 := 510]

    # NOTE: keep your original PD priority (includes duplicated YGQ and no YAQ/YAM)
    PDCOMB2 <- assign_avewt(
      PDCOMB,
      joins = list(
        list(table = YAGM_AVWT, by = c("YEAR", "AREA2", "MONTH", "GEAR")),
        list(table = YGM_AVWT,  by = c("YEAR", "GEAR", "MONTH")),
        list(table = YGQ_AVWT,  by = c("YEAR", "GEAR", "QUARTER")),
        list(table = YAM_AVWT,  by = c("YEAR", "AREA2", "MONTH")),
        list(table = YG_AVWT,   by = c("YEAR", "GEAR"))
      ),
      priority = c("YAGM_AVE_WT", "YGQ_AVE_WT", "YGM_AVE_WT", "YGQ_AVE_WT", "YG_AVE_WT")
    )

    data.table::setorder(PDCOMB2, DELIVERY_DATE, CRUISE, DELIVERING_VESSEL, HAUL_JOIN, SPECIES, SEX, LENGTH)
    data.table::setorder(PDCOMB,  DELIVERY_DATE, CRUISE, DELIVERING_VESSEL, HAUL_JOIN, SPECIES, SEX, LENGTH)

    PDCOMB[, NUMB := PDCOMB2$TONS_LANDED / (PDCOMB2$AVEWT / 1000)]
    data.table::setnames(PDCOMB, "DELIVERING_VESSEL", "VES_AKR_ADFG")
    PDCOMB[, EXTRAPOLATED_WEIGHT := TONS_LANDED * 1000]

    PORTDL <- PDCOMB[, .(SPECIES, YEAR, GEAR, AREA2, AREA, MONTH, QUARTER, CRUISE, VES_AKR_ADFG,
                          HAUL_JOIN, SEX, LENGTH, SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB, SOURCE)]

    PORT_DATA <- data.table::as.data.table(data.table::rbindlist(list(PORTAL, PORTBL, PORTCL, PORTDL), use.names = TRUE, fill = TRUE))
    ALL_DATA  <- data.table::rbindlist(list(OBS_DATA, PORT_DATA), use.names = TRUE, fill = TRUE)

  } else {
    ALL_DATA <- OBS_DATA
  }

  # ---- blend domestic catch (AKFIN) ----
  catch <- sql_reader("dom_catch.sql")
  catch <- sql_filter("<=", ly, catch, flag = "-- insert year",value_type = c("numeric"))
  catch <- sql_filter("IN", species_catch, catch, flag = "-- insert species_catch",value_type = c("character"))
  catch <- sql_filter("IN", sp_area, catch, flag = "-- insert subarea",value_type = c("character"))

  CATCH <- sql_run(con_akfin, catch) |>
    data.table::as.data.table()
  data.table::setnames(CATCH, toupper(names(CATCH)))
 if(use_foreign){
  # ---- foreign historical catch (AFSC) ----
  foreign_catch_area <- if (sp_area == "BS") "LIKE '%BERING%'" else
    if (sp_area == "AI") "LIKE '%ALEUTIANS%'" else
      "IN ('KODIAK','YAKUTAT', 'SHUMAGIN','S.E. ALASKA', 'SHELIKOF STR.','CHIRIKOF')"

  fcatch <- sql_reader("for_catch.sql")
  fcatch <- sql_filter("IN", for_species_catch, fcatch, flag = "-- insert species_catch",value_type = c("character"))
  fcatch <- sql_add(foreign_catch_area, fcatch, flag = "-- insert subarea")

  FCATCH <- sql_run(con_afsc, fcatch) |>
    data.table::as.data.table()
  data.table::setnames(FCATCH, toupper(names(FCATCH)))
  FCATCH <- data.table::as.data.table(FCATCH)
  FCATCH[AREA < 100, AREA := AREA * 10]

  # ---- combine catch sources ----
  CATCHT <- data.table::rbindlist(list(FCATCH, CATCH), use.names = TRUE, fill = TRUE)
  } else CATCHT<-CATCH

  CATCHT <- CATCHT[TONS > 0]
  CATCHT[, AREA := as.numeric(AREA)]
  CATCHT[, MONTH := as.numeric(MONTH_WED)]
  CATCHT[, QUARTER := trunc((MONTH) / 3 - 0.3) + 1]
  CATCHT[, AREA2 := trunc(AREA / 10) * 10]
  CATCHT[AREA2 == 500, AREA2 := 510]
  CATCHT <- CATCHT[GEAR %in% c("POT", "TRW", "HAL")]
  CATCHT[, YEAR := as.numeric(YEAR)]

  # ---- estimate number of fish caught using AVEWT (keep original priority) ----
  CATCHT2 <- assign_avewt(
    CATCHT,
    joins = list(
      list(table = YAGM_AVWT, by = c("YEAR", "AREA2", "MONTH", "GEAR")),
      list(table = YGM_AVWT,  by = c("YEAR", "GEAR", "MONTH")),
      list(table = YGQ_AVWT,  by = c("YEAR", "GEAR", "QUARTER")),
      list(table = YAM_AVWT,  by = c("YEAR", "AREA2", "MONTH")),
      list(table = YG_AVWT,   by = c("YEAR", "GEAR"))
    ),
    priority = c("YAGM_AVE_WT", "YGM_AVE_WT", "YGQ_AVE_WT", "YAM_AVE_WT", "YG_AVE_WT")
  )

  CATCHT3 <- data.table::as.data.table(as.data.frame(CATCHT2)[, names(CATCHT), drop = FALSE])
  CATCHT3[, NUMBER := TONS / (CATCHT2$AVEWT / 1000)]
  CATCHT3[, SPECIES := as.numeric(species)]
  CATCHT3 <- CATCHT3[!is.na(NUMBER)]

  CATCHT4 <- CATCHT3[, .(YAGM_TONS = sum(TONS), YAGM_TNUM = sum(NUMBER)),
                     by = .(SPECIES, YEAR, GEAR, AREA2, MONTH)]

  xt_YAG <- CATCHT4[, .(YAG_TONS = sum(YAGM_TONS), YAG_TNUM = sum(YAGM_TNUM)), by = .(AREA2, GEAR, YEAR)]
  CATCHT4 <- merge(CATCHT4, xt_YAG, by = c("YEAR", "AREA2", "GEAR"), all.x = TRUE)

  xt_YG <- CATCHT4[, .(YG_TONS = sum(YAGM_TONS), YG_TNUM = sum(YAGM_TNUM)), by = .(GEAR, YEAR)]
  CATCHT4 <- merge(CATCHT4, xt_YG, by = c("YEAR", "GEAR"), all.x = TRUE)

  xt_Y <- CATCHT4[, .(Y_TONS = sum(YAGM_TONS), Y_TNUM = sum(YAGM_TNUM)), by = .(YEAR)]
  CATCHT4 <- merge(CATCHT4, xt_Y, by = "YEAR", all.x = TRUE)

  # ---- length data prep ----
  Length <- ALL_DATA[GEAR %in% c("POT", "TRW", "HAL")]
  Length[, YEAR := as.numeric(YEAR)]
  Length[, MONTH := as.numeric(MONTH)]
  Length[, YAGMH_STONS := EXTRAPOLATED_WEIGHT / 1000]
  Length[, YAGMH_SNUM := NUMB]

  # ------------------------------------------------------------------
  # The rest of the function is your original weighting + ESS logic,
  # preserved with minimal structural edits (data.table throughout).
  # ------------------------------------------------------------------

  if (!isTRUE(SEX)) {

    Length <- Length[, .(SUM_FREQUENCY = sum(SUM_FREQUENCY)),
                     by = .(SPECIES, YEAR, AREA2, GEAR, MONTH, CRUISE, VES_AKR_ADFG, HAUL_JOIN,
                            LENGTH, YAGMH_STONS, YAGMH_SNUM)]

    L_YAGMH <- Length[, .(YAGMH_SFREQ = sum(SUM_FREQUENCY)),
                      by = .(CRUISE, VES_AKR_ADFG, HAUL_JOIN)]
    Length <- merge(Length, L_YAGMH, by = c("CRUISE", "VES_AKR_ADFG", "HAUL_JOIN"), all.x = TRUE)

    L_YAGM <- Length[, .(YAGM_STONS = sum(YAGMH_STONS), YAGM_SNUM = sum(YAGMH_SNUM), YAGM_SFREQ = sum(SUM_FREQUENCY)),
                     by = .(AREA2, GEAR, MONTH, YEAR)]
    Length <- merge(Length, L_YAGM, by = c("YEAR", "AREA2", "GEAR", "MONTH"), all.x = TRUE)

    L_YAG <- Length[, .(YAG_STONS = sum(YAGMH_STONS), YAG_SNUM = sum(YAGMH_SNUM), YAG_SFREQ = sum(SUM_FREQUENCY)),
                    by = .(AREA2, GEAR, YEAR)]
    Length <- merge(Length, L_YAG, by = c("YEAR", "AREA2", "GEAR"), all.x = TRUE)

    L_YG <- Length[, .(YG_STONS = sum(YAGMH_STONS), YG_SNUM = sum(YAGMH_SNUM), YG_SFREQ = sum(SUM_FREQUENCY)),
                   by = .(GEAR, YEAR)]
    Length <- merge(Length, L_YG, by = c("YEAR", "GEAR"), all.x = TRUE)

    L_Y <- Length[, .(Y_STONS = sum(YAGMH_STONS), Y_SNUM = sum(YAGMH_SNUM), Y_SFREQ = sum(SUM_FREQUENCY)),
                  by = .(YEAR)]
    Length <- merge(Length, L_Y, by = "YEAR", all.x = TRUE)

    Length$SPECIES<-as.numeric(Length$SPECIES)
    CATCHT4$SPECIES<-as.numeric(CATCHT4$SPECIES)

    x <- merge(Length, CATCHT4, by = c("SPECIES", "YEAR", "AREA2", "GEAR", "MONTH"), all.x = TRUE)
    y2 <- x[!is.na(YAGM_TNUM)]

    y2[, WEIGHT1 := SUM_FREQUENCY / YAGMH_SFREQ]
    y2[, WEIGHT2 := YAGMH_SNUM / YAGM_SNUM]
    y2[, WEIGHT3 := YAGM_TNUM / YG_TNUM]
    y2[, WEIGHT4 := YAGM_TNUM / Y_TNUM]

    y3 <- y2[, .(YEAR, GEAR, AREA2, MONTH, CRUISE, HAUL_JOIN, LENGTH, SUM_FREQUENCY,
                 YAGMH_SNUM, YAGMH_SFREQ, YAGM_SFREQ, YG_SFREQ, Y_SFREQ,
                 YAGM_TNUM, YG_TNUM, Y_TNUM, YAGM_SNUM, YG_SNUM, Y_SNUM,
                 WEIGHT1, WEIGHT2, WEIGHT3, WEIGHT4)]

    y3[, WEIGHTX := WEIGHT1 * WEIGHT2 * WEIGHT4]
    y3[, WEIGHTX_GEAR := WEIGHT1 * WEIGHT2 * WEIGHT3]
    y3[, STRATA := paste(AREA2, MONTH, GEAR, sep = "_")]
    y3[, STRATA1 := as.numeric(as.factor(STRATA))]
    y3[, HAUL_JOIN1 := as.numeric(as.factor(HAUL_JOIN))]

    y4   <- y3[YAGM_SFREQ > 30, .(WEIGHT = sum(WEIGHTX)), by = .(LENGTH, YEAR)]
    y4.1 <- y3[YAGM_SFREQ > 30, .(WEIGHT_GEAR = sum(WEIGHTX_GEAR)), by = .(LENGTH, GEAR, YEAR)]

    y5 <- y4[, .(TWEIGHT = sum(WEIGHT)), by = .(YEAR)]
    y5 <- merge(y4, y5, by = "YEAR")
    y5[, FREQ := WEIGHT / TWEIGHT]
    y6 <- y5[, .(YEAR, LENGTH, FREQ)]

    grid <- data.table::as.data.table(expand.grid(YEAR = unique(y5$YEAR), LENGTH = 1:max(y5$LENGTH)))
    y7 <- merge(grid, y6, all.x = TRUE, by = c("YEAR", "LENGTH"))
    y7[is.na(FREQ), FREQ := 0]

    y5.1 <- y4.1[, .(TWEIGHT = sum(WEIGHT_GEAR)), by = .(GEAR, YEAR)]
    y5.1 <- merge(y4.1, y5.1, by = c("GEAR", "YEAR"))
    y5.1[, FREQ := WEIGHT_GEAR / TWEIGHT]
    y6.1 <- y5.1[, .(YEAR, GEAR, LENGTH, FREQ)]

    grid <- data.table::as.data.table(expand.grid(YEAR = unique(y6.1$YEAR), GEAR = unique(y6.1$GEAR), LENGTH = 1:max(y6.1$LENGTH)))
    y7.1 <- merge(grid, y6.1, all.x = TRUE, by = c("YEAR", "GEAR", "LENGTH"))
    y7.1[is.na(FREQ), FREQ := 0]

    # ESS outputs preserved (BootESS and df remain NA)
    y3.1 <- y3[, .(YEAR, HAUL_JOIN1, STRATA1, LENGTH, SUM_FREQUENCY, YAGMH_SFREQ, YAGMH_SNUM,
                   YAGM_SFREQ, YG_SFREQ, Y_SFREQ, YAGM_TNUM, YG_TNUM, Y_TNUM,
                   YAGM_SNUM, YG_SNUM, Y_SNUM)]
    y3.1 <- y3.1[YAGM_SFREQ > 30]

    years <- unique(y3.1$YEAR)
    ESS <- vector("list", length(years))
    for (i in seq_along(years)) {
      data <- y3.1[YEAR == years[i]]
      N <- sum(data$SUM_FREQUENCY)
      H <- length(unique(data$HAUL_JOIN1))
      S <- length(unique(data$STRATA1))
      ESS[[i]] <- data.table::data.table(YEAR = years[i], BootESS = NA, df = NA, NSAMP = N, NHAUL = H, NSTRATA = S)
    }
    ESS <- data.table::rbindlist(ESS)

    y3.2 <- y3[, .(YEAR, GEAR, HAUL_JOIN1, STRATA1, LENGTH, SUM_FREQUENCY, YAGMH_SFREQ,
                   YAGM_SFREQ, YG_SFREQ, YAGM_TNUM, YG_TNUM, YAGMH_SNUM, YAGM_SNUM, YG_SNUM)]
    y3.2 <- y3.2[YAGM_SFREQ > 30]

    years <- unique(y3.2$YEAR)
    gears <- unique(y3.2$GEAR)
    ESS.1 <- vector("list", length(years) * length(gears))
    b <- 1
    for (j in seq_along(gears)) {
      for (i in seq_along(years)) {
        data <- y3.2[YEAR == years[i] & GEAR == gears[[j]]]
        if (nrow(data) > 0) {
          N <- sum(data$SUM_FREQUENCY)
          H <- length(unique(data$HAUL_JOIN1))
          S <- length(unique(data$STRATA1))
        } else {
          N <- 0; H <- 0; S <- 0
        }
        ESS.1[[b]] <- data.table::data.table(YEAR = years[i], GEAR = gears[[j]], BootESS = NA, df = NA, NSAMP = N, NHAUL = H, NSTRATA = S)
        b <- b + 1
      }
    }
    ESS.1 <- data.table::rbindlist(ESS.1)

    LF1   <- merge(y7, ESS, by = "YEAR")
    LF1.1 <- merge(y7.1, ESS.1, by = c("YEAR", "GEAR"))
    LF1.1 <- LF1.1[NSAMP > 0]

    return(list(LF1, LF1.1))
  }

  # ---- SEX == TRUE branch (kept functionally identical; minimal edits) ----
  Length <- Length[, .(SUM_FREQUENCY = sum(SUM_FREQUENCY)),
                   by = .(SPECIES, YEAR, AREA2, GEAR, MONTH, CRUISE, VES_AKR_ADFG, HAUL_JOIN, SEX,
                          LENGTH, YAGMH_STONS, YAGMH_SNUM)]

  L_YAGMH <- Length[, .(YAGMH_SFREQ = sum(SUM_FREQUENCY)),
                    by = .(CRUISE, VES_AKR_ADFG, HAUL_JOIN)]
  Length <- merge(Length, L_YAGMH, by = c("CRUISE", "VES_AKR_ADFG", "HAUL_JOIN"), all.x = TRUE)

  L_YAGM <- Length[, .(YAGM_STONS = sum(YAGMH_STONS), YAGM_SNUM = sum(YAGMH_SNUM), YAGM_SFREQ = sum(SUM_FREQUENCY)),
                   by = .(AREA2, GEAR, MONTH, YEAR)]
  Length <- merge(Length, L_YAGM, by = c("YEAR", "AREA2", "GEAR", "MONTH"), all.x = TRUE)

  L_YAG <- Length[, .(YAG_STONS = sum(YAGMH_STONS), YAG_SNUM = sum(YAGMH_SNUM), YAG_SFREQ = sum(SUM_FREQUENCY)),
                  by = .(AREA2, GEAR, YEAR)]
  Length <- merge(Length, L_YAG, by = c("YEAR", "AREA2", "GEAR"), all.x = TRUE)

  L_YG <- Length[, .(YG_STONS = sum(YAGMH_STONS), YG_SNUM = sum(YAGMH_SNUM), YG_SFREQ = sum(SUM_FREQUENCY)),
                 by = .(GEAR, YEAR)]
  Length <- merge(Length, L_YG, by = c("YEAR", "GEAR"), all.x = TRUE)

  L_Y <- Length[, .(Y_STONS = sum(YAGMH_STONS), Y_SNUM = sum(YAGMH_SNUM), Y_SFREQ = sum(SUM_FREQUENCY)),
                by = .(YEAR)]
  Length <- merge(Length, L_Y, by = "YEAR", all.x = TRUE)

  Length$SPECIES<-as.numeric(Length$SPECIES)
  CATCHT4$SPECIES<-as.numeric(CATCHT4$SPECIES)

  x <- merge(Length, CATCHT4, by = c("SPECIES", "YEAR", "AREA2", "GEAR", "MONTH"), all.x = TRUE)
  y2 <- x[!is.na(YAGM_TNUM)]

  y2[, WEIGHT1 := SUM_FREQUENCY / YAGMH_SFREQ]
  y2[, WEIGHT2 := YAGMH_SNUM / YAGM_SNUM]
  y2[, WEIGHT3 := YAGM_TNUM / YG_TNUM]
  y2[, WEIGHT4 := YAGM_TNUM / Y_TNUM]

  y3 <- y2[, .(YEAR, GEAR, AREA2, MONTH, CRUISE, HAUL_JOIN, SEX, LENGTH, SUM_FREQUENCY,
               YAGMH_SNUM, YAGMH_SFREQ, YAGM_SFREQ, YG_SFREQ, Y_SFREQ,
               YAGM_TNUM, YG_TNUM, Y_TNUM, YAGM_SNUM, YG_SNUM, Y_SNUM,
               WEIGHT1, WEIGHT2, WEIGHT3, WEIGHT4)]

  y3[, WEIGHTX := WEIGHT1 * WEIGHT2 * WEIGHT4]
  y3[, WEIGHTX_GEAR := WEIGHT1 * WEIGHT2 * WEIGHT3]
  y3 <- y3[WEIGHTX > 0]
  # keep your original STRATA definition as written (uses MONTH_WED, though it is not created in this branch)
  # NOTE: In your original code this likely relies on MONTH_WED existing earlier in ALL_DATA; preserved as-is.
  y3[, STRATA := paste(AREA2, MONTH_WED, GEAR, sep = "_")]
  y3[, STRATA1 := as.numeric(as.factor(STRATA))]
  y3[, HAUL_JOIN1 := as.numeric(as.factor(HAUL_JOIN))]

  y4   <- y3[YAGM_SFREQ > 30, .(WEIGHT = sum(WEIGHTX)), by = .(SEX, LENGTH, YEAR, Y_TNUM)]
  y4.1 <- y3[YAGM_SFREQ > 30, .(WEIGHT_GEAR = sum(WEIGHTX_GEAR)), by = .(SEX, LENGTH, GEAR, YEAR)]

  y5 <- y4[, .(TWEIGHT = sum(WEIGHT)), by = .(YEAR)]
  y5 <- merge(y4, y5, by = "YEAR")
  y5[, FREQ := WEIGHT / TWEIGHT]
  y6 <- y5[, .(YEAR, SEX, LENGTH, FREQ)]

  grid <- data.table::as.data.table(expand.grid(YEAR = unique(y5$YEAR), SEX = as.character(1:3), LENGTH = 1:max(y5$LENGTH)))
  y7 <- merge(grid, y6, all.x = TRUE, by = c("YEAR", "SEX", "LENGTH"))
  y7[is.na(FREQ), FREQ := 0]

  y5.1 <- y4.1[, .(TWEIGHT = sum(WEIGHT_GEAR)), by = .(GEAR, YEAR)]
  y5.1 <- merge(y4.1, y5.1, by = c("GEAR", "YEAR"))
  y5.1[, FREQ := WEIGHT_GEAR / TWEIGHT]
  y6.1 <- y5.1[, .(YEAR, GEAR, SEX, LENGTH, FREQ)]

  grid <- data.table::as.data.table(expand.grid(YEAR = unique(y6.1$YEAR), GEAR = unique(y6.1$GEAR), SEX = as.character(1:3), LENGTH = 1:max(y6.1$LENGTH)))
  y7.1 <- merge(grid, y6.1, all.x = TRUE, by = c("YEAR", "GEAR", "SEX", "LENGTH"))
  y7.1[is.na(FREQ), FREQ := 0]

  # ESS outputs preserved (BootESS and df remain NA)
  y3.1 <- y3[, .(YEAR, HAUL_JOIN1, STRATA1, SEX, LENGTH, SUM_FREQUENCY, YAGMH_SFREQ, YAGMH_SNUM,
                 YAGM_SFREQ, YG_SFREQ, Y_SFREQ, YAGM_TNUM, YG_TNUM, Y_TNUM,
                 YAGM_SNUM, YG_SNUM, Y_SNUM)]
  y3.1 <- y3.1[YAGM_SFREQ > 30]

  years <- unique(y3.1$YEAR)
  sexs <- as.character(1:3)

  ESS <- vector("list", length(years) * length(sexs))
  b <- 1
  for (i in seq_along(years)) {
    for (k in seq_along(sexs)) {
      data <- y3.1[YEAR == years[i] & SEX == sexs[k]]
      N <- sum(data$SUM_FREQUENCY)
      H <- length(unique(data$HAUL_JOIN1))
      S <- length(unique(data$STRATA1))
      ESS[[b]] <- data.table::data.table(YEAR = years[i], SEX = sexs[k], BootESS = NA, df = NA, NSAMP = N, NHAUL = H, NSTRATA = S)
      b <- b + 1
    }
  }
  ESS <- data.table::rbindlist(ESS)

  y3.2 <- y3[, .(YEAR, GEAR, HAUL_JOIN1, STRATA1, SEX, LENGTH, SUM_FREQUENCY, YAGMH_SFREQ,
                 YAGM_SFREQ, YG_SFREQ, YAGM_TNUM, YG_TNUM, YAGMH_SNUM, YAGM_SNUM, YG_SNUM)]
  y3.2 <- y3.2[YAGM_SFREQ > 30]

  years <- unique(y3.2$YEAR)
  gears <- unique(y3.2$GEAR)

  ESS.1 <- vector("list", length(years) * length(gears) * length(sexs))
  b <- 1
  for (j in seq_along(gears)) {
    for (i in seq_along(years)) {
      for (k in seq_along(sexs)) {
        data <- y3.2[YEAR == years[i] & GEAR == gears[j] & SEX == sexs[k]]
        if (nrow(data) > 0) {
          N <- sum(data$SUM_FREQUENCY)
          H <- length(unique(data$HAUL_JOIN1))
          S <- length(unique(data$STRATA1))
        } else {
          N <- 0; H <- 0; S <- 0
        }
        ESS.1[[b]] <- data.table::data.table(YEAR = years[i], SEX = sexs[k], GEAR = gears[j], BootESS = NA, df = NA, NSAMP = N, NHAUL = H, NSTRATA = S)
        b <- b + 1
      }
    }
  }
  ESS.1 <- data.table::rbindlist(ESS.1)

  LF1   <- merge(y7, ESS, by = c("YEAR", "SEX"))
  LF1.1 <- merge(y7.1, ESS.1, by = c("YEAR", "SEX", "GEAR"))
  LF1.1 <- LF1.1[NSAMP > 0]

  list(LF1, LF1.1)
}
