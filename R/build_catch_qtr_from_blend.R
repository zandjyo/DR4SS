#' Build quarterly catch weights from blend catch output
#'
#' @description
#' Converts monthly blend catch (from COUNCIL.COMPREHENSIVE_BLEND_CA) into quarterly
#' catch totals and within-year proportions suitable for catch-weighting EWAA.
#'
#' @param blend_catch A data.frame/data.table with at least YEAR, MONTH_WED, and TONS.
#'   If `by_gear=TRUE`, must also contain GEAR (e.g., "HAL","POT","TRW"/other).
#' @param by_gear Logical. If TRUE, compute catch proportions by gear group (GEAR2)
#'   within each year. Gear mapping: 1 = other/trawl, 2 = HAL, 3 = POT.
#' @param use_retained Optional character vector of RETAINED_OR_DISCARDED codes to keep
#'   (e.g., c("R") to keep retained only). Default NULL keeps all (retained+discarded).
#' @param complete_grid Logical. If TRUE, fill in missing quarters with CATCH=0 and p_q=0.
#'
#' @return A data.table with columns:
#' - YEAR (int)
#' - QUARTER (int 1-4)
#' - CATCH (numeric; summed TONS)
#' - p_q (numeric; within-year proportion)
#' - GEAR2 (int) if by_gear=TRUE
#'
#' @export
build_catch_qtr_from_blend <- function(blend_catch,
                                       by_gear = FALSE,
                                        complete_grid = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }

  dt <- data.table::as.data.table(blend_catch)
  names(dt) <- toupper(names(dt))

  need <- c("YEAR", "MONTH_WED", "TONS")
  miss <- setdiff(need, names(dt))
  if (length(miss) > 0) {
    stop("blend_catch missing required columns: ", paste(miss, collapse = ", "), call. = FALSE)
  }

  suppressWarnings({
    dt[, YEAR  := as.integer(as.character(YEAR))]
    dt[, MONTH := as.integer(as.character(MONTH_WED))]
    dt[, TONS  := as.numeric(TONS)]
  })

  dt <- dt[!is.na(YEAR) & !is.na(MONTH) & MONTH %in% 1:12]
  dt <- dt[!is.na(TONS) & is.finite(TONS)]

  dt[, QUARTER := data.table::fifelse(
    MONTH <= 3, 1L,
    data.table::fifelse(MONTH <= 6, 2L,
      data.table::fifelse(MONTH <= 9, 3L, 4L)
    )
  )]

  if (isTRUE(by_gear)) {
    if (!"GEAR" %in% names(dt)) {
      stop("by_gear=TRUE requires a GEAR column in blend_catch.", call. = FALSE)
    }
    dt[, GEAR := toupper(trimws(as.character(GEAR)))]
    dt[, GEAR2 := 1L]
    dt[GEAR == "HAL", GEAR2 := 2L]
    dt[GEAR == "POT", GEAR2 := 3L]
  }

  # Aggregate to quarter totals
  if (isTRUE(by_gear)) {
    out <- dt[, .(CATCH = sum(TONS, na.rm = TRUE)), by = .(YEAR, QUARTER, GEAR2)]
  } else {
    out <- dt[, .(CATCH = sum(TONS, na.rm = TRUE)), by = .(YEAR, QUARTER)]
  }

  # Optionally complete missing quarters (and gear2 combos)
  if (isTRUE(complete_grid)) {
    if (isTRUE(by_gear)) {
      grid <- data.table::CJ(
        YEAR = sort(unique(out$YEAR)),
        QUARTER = 1:4,
        GEAR2 = sort(unique(out$GEAR2))
      )
      out <- merge(grid, out, by = c("YEAR", "QUARTER", "GEAR2"), all.x = TRUE)
    } else {
      grid <- data.table::CJ(
        YEAR = sort(unique(out$YEAR)),
        QUARTER = 1:4
      )
      out <- merge(grid, out, by = c("YEAR", "QUARTER"), all.x = TRUE)
    }
    out[is.na(CATCH), CATCH := 0]
  }

  # ✅ FIXED: compute p_q safely (vector-returning j)
  if (isTRUE(by_gear)) {
    out[, p_q := {
      tot <- sum(CATCH, na.rm = TRUE)
      if (is.finite(tot) && tot > 0) CATCH / tot else rep(0, .N)
    }, by = .(YEAR, GEAR2)]
  } else {
    out[, p_q := {
      tot <- sum(CATCH, na.rm = TRUE)
      if (is.finite(tot) && tot > 0) CATCH / tot else rep(0, .N)
    }, by = .(YEAR)]
  }

  data.table::setorder(out, YEAR, QUARTER)
  out
}
