#' Build empirical length-at-age distributions (by quarter/gear/sex as requested)
#'
#' @param fish_data data.table from get_fishery_age_wt_data()
#' @param len_bins numeric vector of bin edges (e.g., seq(10, 120, 2))
#' @param maxage plus group age
#' @param area_region "regtion" or "area'
#' @param gear_mode "combined" or "by_gear"
#' @param sex_mode "combined" or "split"
#' @param min_strata_n minimum N within each (YEAR,SEASON,AGE,...) required; else strata dropped later if you want
#'
#' @return data.table with YEAR, QUARTER, AGE_YRS, (GEAR2), (SEX), BIN, N, P_LEN
#' @export
build_empirical_length_at_age <- function(fish_data,
                                          len_bins,
                                          maxage = 10,
                                          area_region = c("region","area"),
                                          gear_mode = c("combined","by_gear"),
                                          sex_mode  = c("combined","split"),
                                          min_strata_n = 1) {

  if (!requireNamespace("data.table", quietly = TRUE)) stop("Need data.table.", call. = FALSE)

  gear_mode <- match.arg(gear_mode)
  sex_mode  <- match.arg(sex_mode)
  area_region  <- match.arg(area_region)

  dt <- data.table::as.data.table(fish_data)
  names(dt) <- toupper(names(dt))

  need <- c("YEAR","SEASON","AGE","LENGTH")
  miss <- setdiff(need, names(dt))
  if (length(miss) > 0) stop("fish_data missing: ", paste(miss, collapse = ", "), call. = FALSE)

  al <- dt[!is.na(AGE) & !is.na(LENGTH) & !is.na(YEAR) & !is.na(SEASON)]
  if (nrow(al) == 0) stop("No age-length records available.", call. = FALSE)

  al[, AGE_YRS := as.integer(round(as.numeric(AGE)))]
  al[AGE_YRS > maxage, AGE_YRS := maxage]

  if (gear_mode == "combined") {
    if ("GEAR2" %in% names(al)) al[, GEAR2 := NULL]
  } else {
    if (!"GEAR2" %in% names(al)) stop("gear_mode='by_gear' requires GEAR2.", call. = FALSE)
    al[, GEAR2 := as.integer(as.character(GEAR2))]
  }

  if (sex_mode == "combined") {
    if ("SEX" %in% names(al)) al[, SEX := NULL]
  } else {
    if (!"SEX" %in% names(al)) stop("sex_mode='split' requires SEX.", call. = FALSE)
    al[, SEX := toupper(trimws(as.character(SEX)))]
    al <- al[SEX %in% c("F","M")]
  }

  if (area_region == "region") {
    if ("AREA" %in% names(al)) al[, AREA := NULL]
  } else {
    if (!"REGION_GRP" %in% names(al)) stop("area_region='region' requires REGION_GRP.", call. = FALSE)
    }

  if (area_region == "area") {
    if ("REGION_GRP" %in% names(al)) al[, REGION_GRP := NULL]
  } else {
    if (!"AREA" %in% names(al)) stop("area_region='area' requires AREA.", call. = FALSE)
    }

  # --- length binning with clamping and within-bin mean length ---

  # clamp lengths into bin range
  Lmin <- min(len_bins)
  Lmax <- max(len_bins)

  al[LENGTH < Lmin, LENGTH := Lmin]
  al[LENGTH >= Lmax, LENGTH := Lmax - 1e-6]  # keep inside last bin

  # assign bins
  al[, BIN := cut(LENGTH, breaks = len_bins, include.lowest = TRUE, right = FALSE)]
  al[, BIN_ID := as.integer(BIN)]

  # build empirical AL:
  # count AND mean length per bin
  by_cols <- c("YEAR","SEASON","AGE_YRS")
  if (gear_mode == "by_gear") by_cols <- c(by_cols, "GEAR2")
  if (sex_mode  == "split")  by_cols <- c(by_cols, "SEX")
  if (area_region=="area" && "AREA" %in% names(al)) {
  # keep AREA available for downstream LW prediction if desired
  by_cols <- c(by_cols, "AREA")
  } else {by_cols <- c(by_cols, "REGION_GRP")}

  out <- al[, .(
    N = .N,
    L_BAR = mean(LENGTH, na.rm = TRUE)
  ), by = c(by_cols, "BIN_ID")]

  # proportions within age stratum
  out[, P_LEN := N / sum(N), by = by_cols]
  out[!is.finite(P_LEN), P_LEN := 0]


  # proportions within each stratum (summing over bins)
  out[, P_LEN := N / sum(N), by = by_cols]
  out[!is.finite(P_LEN) | is.na(P_LEN), P_LEN := 0]

  # optional: enforce minimum samples per stratum (drop strata entirely)
  if (is.numeric(min_strata_n) && min_strata_n > 1) {
    strata_n <- out[, .(N_STRATA = sum(N)), by = by_cols]
    out <- merge(out, strata_n, by = by_cols, all.x = TRUE)
    out <- out[N_STRATA >= min_strata_n]
    out[, N_STRATA := NULL]
  }

  data.table::setorder(out, YEAR, SEASON, AGE_YRS)
  out
}
