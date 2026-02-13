#' Estimate effective multinomial sample size (Neff) from haul-level compositions
#'
#' Computes an approximate multinomial effective sample size (Neff) using the
#' between-haul variance in composition proportions. This is useful for deriving
#' annual (or annual-by-strata) composition sample sizes for Stock Synthesis.
#'
#' @details
#' Input data should be haul-level counts by bin (length or age). The function:
#' \enumerate{
#'   \item Computes within-haul proportions for each bin: \eqn{p_{h,b} = n_{h,b} / \sum_b n_{h,b}}
#'   \item For each YEAR (and optional grouping factors), computes the mean and variance
#'         across hauls for each bin: \eqn{\bar{p}_b, \mathrm{Var}(p_b)}
#'   \item Estimates bin-specific Neff: \eqn{N_{\mathrm{eff},b} = \bar{p}_b(1-\bar{p}_b) / \mathrm{Var}(p_b)}
#'   \item Aggregates across bins (median by default) to get a single Neff per YEAR/group.
#' }
#'
#' @param haul_bin_dt A data.frame/data.table with haul-level counts.
#'   Must contain YEAR, HAUL_JOIN, COUNT, and a bin column specified by \code{bin_col}
#'   (e.g., LENGTH or AGE or BIN).
#' @param bin_col Name of the bin column in \code{haul_bin_dt}. Common values are
#'   \code{"LENGTH"}, \code{"AGE"}, or \code{"BIN"}. Default \code{"BIN"}.
#' @param group_cols Optional character vector of additional grouping columns besides YEAR
#'   (e.g., c("GEAR","SEX","AREA2")).
#' @param min_hauls Minimum unique hauls required to estimate Neff (else NA).
#' @param eps Small number to avoid division-by-zero in variance.
#' @param cap Optional cap for Neff (e.g., 500). Use NULL for no cap.
#' @param agg_fun Function to aggregate bin-specific Neff across bins; default median.
#'
#' @return data.table with YEAR (+group_cols), N_HAUL, and NEFF.
#' @export
estimate_neff_from_hauls <- function(haul_bin_dt,
                                     bin_col = c("BIN", "LENGTH", "AGE"),
                                     count_col = "FREQ",        # <-- NEW
                                     group_cols = character(0),
                                     min_hauls = 5L,
                                     eps = 1e-10,
                                     cap = 500,
                                     agg_fun = stats::median) {
  stopifnot(requireNamespace("data.table", quietly = TRUE))
  DT <- data.table::as.data.table(haul_bin_dt)

  bin_col <- match.arg(bin_col)

  key_cols <- c("YEAR", group_cols)

  # --- handle COUNT ---
  if (is.null(count_col)) {
    # user says counts are implicit: one record = one fish
    DT[, COUNT := 1]
  } else {
    # require provided count column
    need <- c(key_cols, "HAUL_JOIN", bin_col, count_col)
    miss <- setdiff(need, names(DT))
    if (length(miss) > 0) {
      stop("Missing columns: ", paste(miss, collapse = ", "), call. = FALSE)
    }

    # if user passed a different column name, rename it to COUNT internally
    if (count_col != "COUNT") {
      data.table::setnames(DT, count_col, "COUNT")
    }
  }

  # --- validate remaining required cols (after possible renames) ---
  need2 <- c(key_cols, "HAUL_JOIN", bin_col, "COUNT")
  miss2 <- setdiff(need2, names(DT))
  if (length(miss2) > 0) {
    stop("Missing columns: ", paste(miss2, collapse = ", "), call. = FALSE)
  }

  # standardize types
  suppressWarnings({
    DT[, YEAR := as.integer(as.character(YEAR))]
    DT[, COUNT := as.numeric(COUNT)]
  })

  # rename selected bin column to BIN internally (avoid branching everywhere)
  if (bin_col != "BIN") {
    data.table::setnames(DT, bin_col, "BIN")
  }

  # remove junk
  DT <- DT[is.finite(COUNT) & COUNT >= 0]
  DT <- DT[!is.na(YEAR) & !is.na(HAUL_JOIN) & !is.na(BIN)]

  # haul totals and within-haul proportions
  DT[, HAUL_N := sum(COUNT, na.rm = TRUE), by = c(key_cols, "HAUL_JOIN")]
  DT <- DT[HAUL_N > 0]
  DT[, P := COUNT / HAUL_N]

  # mean and variance across hauls for each bin
  summ <- DT[, .(
    N_HAUL = data.table::uniqueN(HAUL_JOIN),
    P_BAR  = mean(P, na.rm = TRUE),
    P_VAR  = stats::var(P, na.rm = TRUE)
  ), by = c(key_cols, "BIN")]

  # bin-specific Neff
  summ[, NEFF_BIN := (P_BAR * (1 - P_BAR)) / pmax(P_VAR, eps)]

  # drop bins that cannot inform Neff (P=0/1 or non-finite)
  summ <- summ[is.finite(NEFF_BIN) & is.finite(P_BAR) & P_BAR > 0 & P_BAR < 1]

  # aggregate Neff across bins
  out <- summ[, .(
    N_HAUL = max(N_HAUL),
    NEFF   = if (max(N_HAUL) < min_hauls || .N == 0) NA_real_ else agg_fun(NEFF_BIN, na.rm = TRUE)
  ), by = key_cols]

  # cap / floor
  out[, NEFF := ifelse(is.na(NEFF), NA_real_, pmax(1, NEFF))]
  if (!is.null(cap)) out[, NEFF := pmin(NEFF, cap)]

  out[]
}