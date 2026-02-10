#' Build a pooled age–length key (ALK) from fishery aged samples
#'
#' @description
#' Constructs an age–length key (ALK), i.e. probabilities of age conditional on length,
#' using aged fish from `get_fishery_age_wt_data()`. The ALK is pooled across years
#' (and quarters) to ensure support at all lengths, but may be stratified by gear (GEAR2)
#' and/or sex depending on `gear_mode` and `sex_mode`.
#'
#' Lengths are discretized to integer cm (consistent with LENGTH_BY_CATCH_short output).
#' Ages are plus-grouped at `maxage`.
#'
#' @param fish_raw data.table/data.frame from `get_fishery_age_wt_data()`.
#'   Must include LENGTH and AGE and (optionally) GEAR2/SEX.
#' @param maxage Plus-group age.
#' @param gear_mode "combined" or "by_gear" (if by_gear, require GEAR2).
#' @param sex_mode "combined" or "split" (if split, require SEX coded F/M).
#' @param len_range Integer vector of lengths to build the key on. If NULL, uses
#'   1:max(observed_length_rounded).
#' @param min_n_len Minimum aged fish required for a given length to keep its own ALK.
#'   Lengths with fewer than this are filled from the pooled ALK for that stratum.
#'
#' @return data.table ALK with columns:
#'   LENGTH (int), AGE_YRS (int), P_AGE (numeric),
#'   and optional GEAR2 / SEX columns depending on modes.
#' @export
build_fishery_alk <- function(fish_raw,
                              maxage = 12,
                              gear_mode = c("combined","by_gear"),
                              sex_mode  = c("combined","split"),
                              len_range = NULL,
                              min_n_len = 5L) {
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  gear_mode <- match.arg(gear_mode)
  sex_mode  <- match.arg(sex_mode)

  DT <- data.table::as.data.table
  d  <- DT(fish_raw)
  names(d) <- toupper(names(d))

  req <- c("LENGTH","AGE")
  miss <- setdiff(req, names(d))
  if (length(miss) > 0) stop("fish_raw missing: ", paste(miss, collapse=", "), call. = FALSE)

  # keep aged fish only
  suppressWarnings({
    d[, LENGTH := as.numeric(LENGTH)]
    d[, AGE    := as.integer(as.character(AGE))]
  })
  d <- d[is.finite(LENGTH) & is.finite(AGE)]
  d[, AGE_YRS := pmin(pmax(AGE, 0L), as.integer(maxage))]
  d[, LEN_INT := as.integer(round(LENGTH))]

  # modes
  by <- c("LEN_INT")
  if (gear_mode == "by_gear") {
    if (!"GEAR2" %in% names(d)) stop("gear_mode='by_gear' requires GEAR2 in fish_raw.", call. = FALSE)
    suppressWarnings(d[, GEAR2 := as.integer(as.character(GEAR2))])
    d <- d[is.finite(GEAR2)]
    by <- c(by, "GEAR2")
  }
  if (sex_mode == "split") {
    if (!"SEX" %in% names(d)) stop("sex_mode='split' requires SEX in fish_raw.", call. = FALSE)
    d[, SEX := toupper(trimws(as.character(SEX)))]
    d <- d[SEX %in% c("F","M")]
    by <- c(by, "SEX")
  }

  # define length grid
  if (is.null(len_range)) {
    maxL <- suppressWarnings(max(d$LEN_INT, na.rm = TRUE))
    if (!is.finite(maxL) || maxL < 1) stop("No valid lengths to build ALK.", call. = FALSE)
    len_range <- 1:maxL
  } else {
    len_range <- as.integer(len_range)
    len_range <- len_range[is.finite(len_range)]
    if (length(len_range) == 0) stop("len_range is empty after coercion.", call. = FALSE)
  }

  # counts by length x age (and strata)
  cnt <- d[, .N, by = c(by, "AGE_YRS")]

  # total aged per length (and strata)
  tot_len <- cnt[, .(N_LEN = sum(N)), by = by]

  # raw ALK: P(age|len) within strata
  alk <- merge(cnt, tot_len, by = by, all.x = TRUE)
  alk[, P_AGE := fifelse(N_LEN > 0, N / N_LEN, 0)]
  alk[, c("N","N_LEN") := NULL]
  data.table::setnames(alk, "LEN_INT", "LENGTH")

  # pooled ALK by strata (ignoring length) used as fallback for sparse lengths
  strata_keys <- setdiff(by, "LEN_INT")
  if (length(strata_keys) == 0) {
    pooled <- d[, .N, by = .(AGE_YRS)]
    pooled[, P_POOL := N / sum(N)]
    pooled[, N := NULL]
  } else {
    pooled <- d[, .N, by = c(strata_keys, "AGE_YRS")]
    pooled[, P_POOL := N / sum(N), by = strata_keys]
    pooled[, N := NULL]
  }

  # identify sparse lengths and fill them with pooled ALK
  # build a complete LENGTH x (strata) grid first
  if (length(strata_keys) == 0) {
    grid <- data.table::data.table(LENGTH = len_range)
    grid[, dummy := 1L]
  } else {
    strata_vals <- unique(alk[, ..strata_keys])
    grid <- merge(
      strata_vals,
      data.table::data.table(LENGTH = len_range),
      by = NULL,
      allow.cartesian = TRUE
    )
  }

  # attach N at length (to know which are sparse)
  nlen <- d[, .N, by = c(strata_keys, "LEN_INT")]
  data.table::setnames(nlen, "LEN_INT", "LENGTH")
  grid <- merge(grid, nlen, by = c(strata_keys, "LENGTH"), all.x = TRUE)
  grid[is.na(N), N := 0L]

  # expand grid to AGE rows, then fill P_AGE:
  ages <- data.table::data.table(AGE_YRS = 0:as.integer(maxage))
  grid2 <- merge(grid, ages, by = NULL, allow.cartesian = TRUE)

  # merge observed alk and pooled fallback
  grid2 <- merge(grid2, alk, by = c(strata_keys, "LENGTH", "AGE_YRS"), all.x = TRUE)
  grid2 <- merge(grid2, pooled, by = c(strata_keys, "AGE_YRS"), all.x = TRUE)

  # fill rule:
  # - if length has enough aged fish AND P_AGE present: use it
  # - otherwise use pooled P_POOL
  grid2[, P_AGE := fifelse(N >= as.integer(min_n_len) & !is.na(P_AGE), P_AGE, P_POOL)]
  grid2[, P_POOL := NULL]

  # normalize within each LENGTH (and strata) to ensure sum=1
  norm_keys <- c(strata_keys, "LENGTH")
  grid2[, P_AGE := fifelse(sum(P_AGE) > 0, P_AGE / sum(P_AGE), 0), by = norm_keys]
  grid2[, N := NULL]

  # return only columns needed
  out_cols <- c(strata_keys, "LENGTH", "AGE_YRS", "P_AGE")
  grid2[, ..out_cols]
}
