#' Fit delta-length (quarter-to-quarter) growth translation model
#'
#' Estimates mean length-at-age by quarter (optionally by gear/sex), then computes
#' delta length to translate observed quarter lengths to a target quarter q*:
#'   delta_L(age, q -> q*) = meanL(age, q*) - meanL(age, q)
#'
#' Includes simple, robust shrinkage for sparse cells by blending cell means toward
#' broader means (age+quarter pooled), weighted by sample size.
#'
#' @param fish_al Fish-level (row-level) age-length data.
#'   Must include YEAR, QUARTER, AGE, LENGTH.
#'   Optionally includes GEAR2 and/or SEX.
#' @param target_qtr Integer 1..4. Quarter to translate TO (q*).
#' @param maxage Plus-group age; ages > maxage are pooled to maxage.
#' @param gear_mode "combined" or "by_gear". If "by_gear", requires GEAR2.
#' @param sex_mode "combined" or "split". If "split", requires SEX coded F/M.
#' @param min_n_cell Minimum n for a (YEAR,AGE,QUARTER,...) cell to use mostly its own mean.
#'   Smaller cells will be shrunk toward broader mean.
#' @param shrink_k Nonnegative shrinkage constant. Larger => more shrinkage when n is small.
#'   Weight on cell mean is n/(n+shrink_k).
#'
#' @return A list with:
#'   - deltas: data.table with keys YEAR, AGE, QUARTER (+ optional GEAR2/SEX),
#'       and columns MEANL_Q, MEANL_QSTAR, DELTA_L, N_Q
#'   - target_qtr: the target quarter used
#'   - gear_mode, sex_mode, maxage
#'
#' @export
fit_quarter_growth_model <- function(fish_al,
                                    target_qtr = 1L,
                                    maxage = 10L,
                                    gear_mode = c("combined","by_gear"),
                                    sex_mode  = c("combined","split"),
                                    min_n_cell = 10L,
                                    shrink_k = 30) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  DT <- data.table::as.data.table
  gear_mode <- match.arg(gear_mode)
  sex_mode  <- match.arg(sex_mode)

  data.table::setnames(fish_al,"AGE","AGE_YRS",skip_absent=TRUE)

  target_qtr <- as.integer(target_qtr)
  if (!target_qtr %in% 1:4) stop("target_qtr must be 1..4.", call. = FALSE)

  d <- DT(fish_al)
  names(d) <- toupper(names(d))

  req <- c("YEAR","QUARTER","AGE_YRS","LENGTH")
  miss <- setdiff(req, names(d))
  if (length(miss) > 0) stop("fish_al missing: ", paste(miss, collapse=", "), call. = FALSE)

  suppressWarnings({
    d[, YEAR    := as.integer(as.character(YEAR))]
    d[, QUARTER := as.integer(as.character(QUARTER))]
    d[, AGE_YRS := as.integer(as.character(AGE_YRS))]
    d[, LENGTH  := as.numeric(LENGTH)]
  })

  d <- d[is.finite(YEAR) & is.finite(QUARTER) & is.finite(AGE_YRS) & is.finite(LENGTH)]
  d <- d[QUARTER %in% 1:4]
  d[AGE_YRS > maxage, AGE_YRS := maxage]

  # modes
  if (gear_mode == "by_gear") {
    if (!"GEAR2" %in% names(d)) stop("gear_mode='by_gear' requires GEAR2 in fish_al.", call. = FALSE)
    suppressWarnings(d[, GEAR2 := as.integer(as.character(GEAR2))])
    d <- d[is.finite(GEAR2)]
  } else {
    if ("GEAR2" %in% names(d)) d[, GEAR2 := NULL]
  }

  if (sex_mode == "split") {
    if (!"SEX" %in% names(d)) stop("sex_mode='split' requires SEX in fish_al.", call. = FALSE)
    d[, SEX := toupper(trimws(as.character(SEX)))]
    d <- d[SEX %in% c("F","M")]
  } else {
    if ("SEX" %in% names(d)) d[, SEX := NULL]
  }

  # grouping keys
  grp <- c("YEAR","AGE_YRS","QUARTER")
  if (gear_mode == "by_gear") grp <- c(grp, "GEAR2")
  if (sex_mode  == "split")  grp <- c(grp, "SEX")

  # cell means by year/AGE_YRS/qtr/(gear/sex)
  cell <- d[, .(
    N_Q = .N,
    MEANL_RAW = mean(LENGTH, na.rm = TRUE)
  ), by = grp]

  # broader mean for shrinkage: (YEAR, AGE_YRS, QUARTER) pooled over gear/sex if present
  broad_keys <- c("YEAR","AGE_YRS","QUARTER")
  broad <- d[, .(
    N_BROAD = .N,
    MEANL_BROAD = mean(LENGTH, na.rm = TRUE)
  ), by = broad_keys]

  cell <- merge(cell, broad, by = broad_keys, all.x = TRUE)

  # shrinkage weight (more shrinkage when N small)
  # w = N/(N + shrink_k)
  cell[, W_CELL := data.table::fifelse(N_Q > 0, N_Q/(N_Q + shrink_k), 0)]
  # If N is very small, you can enforce extra shrinkage via min_n_cell:
  cell[N_Q < min_n_cell, W_CELL := N_Q/(N_Q + shrink_k)]

  # shrunk mean length in quarter
  cell[, MEANL_Q := W_CELL * MEANL_RAW + (1 - W_CELL) * MEANL_BROAD]

  # build MEANL at target quarter, same keys except QUARTER
  tgt <- cell[QUARTER == target_qtr]
  if (nrow(tgt) == 0) {
    stop("No data available in target_qtr=", target_qtr, " to define mean length-at-age.", call. = FALSE)
  }

  # keys to match quarter-specific rows to target quarter rows
  match_keys <- setdiff(grp, "QUARTER")

  tgt2 <- tgt[, c(match_keys, "MEANL_Q"), with = FALSE]
  data.table::setnames(tgt2, "MEANL_Q", "MEANL_QSTAR")

  deltas <- merge(cell, tgt2, by = match_keys, all.x = TRUE)

  # If MEANL_QSTAR missing for some strata (e.g., some gear/sex absent in target quarter),
  # fallback to broader target quarter mean ignoring that missing factor level:
  if (any(is.na(deltas$MEANL_QSTAR))) {
    # build fallback target means at target_qtr on progressively reduced keys
    # 1) drop GEAR2 if present
    if (gear_mode == "by_gear") {
      drop_keys <- setdiff(match_keys, "GEAR2")
      tgt_drop <- cell[QUARTER == target_qtr, .(MEANL_QSTAR = mean(MEANL_Q, na.rm = TRUE)), by = drop_keys]
      deltas <- merge(deltas, tgt_drop, by = drop_keys, all.x = TRUE, suffixes = c("", ".fb1"))
      deltas[is.na(MEANL_QSTAR) & !is.na(MEANL_QSTAR.fb1), MEANL_QSTAR := MEANL_QSTAR.fb1]
      deltas[, MEANL_QSTAR.fb1 := NULL]
    }
    # 2) drop SEX if present
    if (sex_mode == "split") {
      drop_keys <- setdiff(match_keys, "SEX")
      tgt_drop <- cell[QUARTER == target_qtr, .(MEANL_QSTAR = mean(MEANL_Q, na.rm = TRUE)), by = drop_keys]
      deltas <- merge(deltas, tgt_drop, by = drop_keys, all.x = TRUE, suffixes = c("", ".fb2"))
      deltas[is.na(MEANL_QSTAR) & !is.na(MEANL_QSTAR.fb2), MEANL_QSTAR := MEANL_QSTAR.fb2]
      deltas[, MEANL_QSTAR.fb2 := NULL]
    }
    # 3) final fallback: year+age only
    tgt_drop <- cell[QUARTER == target_qtr, .(MEANL_QSTAR = mean(MEANL_Q, na.rm = TRUE)), by = .(YEAR, AGE_YRS)]
    deltas <- merge(deltas, tgt_drop, by = c("YEAR","AGE_YRS"), all.x = TRUE, suffixes = c("", ".fb3"))
    deltas[is.na(MEANL_QSTAR) & !is.na(MEANL_QSTAR.fb3), MEANL_QSTAR := MEANL_QSTAR.fb3]
    deltas[, MEANL_QSTAR.fb3 := NULL]
  }

  deltas[, DELTA_L := MEANL_QSTAR - MEANL_Q]

  # Keep a clean output
  keep <- c(grp, "N_Q", "MEANL_Q", "MEANL_QSTAR", "DELTA_L")
  deltas <- deltas[, ..keep]
  data.table::setkeyv(deltas, grp)

  list(
    deltas = deltas,
    target_qtr = target_qtr,
    gear_mode = gear_mode,
    sex_mode = sex_mode,
    maxage = as.integer(maxage)
  )
}
