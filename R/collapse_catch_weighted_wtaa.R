#' Collapse quarter-specific predicted weights-at-age to annual catch-weighted WAA
#'
#' @description
#' Computes quarter-specific mean predicted weight-at-age from aged observations and
#' collapses to an annual mean weight-at-age using quarterly catch proportions from
#' blend catch. Supports output by gear (GEAR2) and/or sex (SEX) or combined.
#'
#' Catch-weighted annual mean for a given grouping is:
#' \deqn{w_{y,a} = \sum_q p_{y,q} \mu_{y,a,q}}
#' where \eqn{\mu_{y,a,q}} is the mean predicted weight in quarter q and
#' \eqn{p_{y,q}} is the catch proportion in that quarter (from blend catch).
#'
#' If some quarters have no observations for a given year-age-group, you can either:
#' - renormalize p_q over available quarters (default), or
#' - treat missing quarters as contributing nothing (renormalize = FALSE).
#'
#' @param pred data.table/data.frame from `predict_weights_for_aged()` with columns
#'   YEAR, QUARTER, AGE_YRS, WEIGHT_KG, and optionally SEX, GEAR2.
#' @param catch_qtr data.table/data.frame from `build_catch_qtr_from_blend()` with columns
#'   YEAR, QUARTER, p_q and optionally GEAR2 (if catch weights are gear-specific).
#' @param gear_mode Character: "combined" or "by_gear".
#' @param sex_mode Character: "combined" or "split".
#' @param renormalize Logical. If TRUE (default), renormalize p_q within each
#'   (YEAR, AGE, GEAR2, SEX) over quarters that have non-missing means.
#' @param require_catch_years Logical. If TRUE (default), error if any YEAR in `pred`
#'   is missing from `catch_qtr`. If FALSE, missing years get p_q = 0.
#'
#' @return A data.table with columns:
#' \describe{
#'   \item{YEAR}{Integer year}
#'   \item{AGE_YRS}{Integer age (plus-grouped already in `pred`)}
#'   \item{GEAR2}{Integer gear group if gear_mode="by_gear", otherwise absent}
#'   \item{SEX}{"F"/"M" if sex_mode="split", otherwise absent}
#'   \item{WAA}{Catch-weighted annual mean weight-at-age}
#'   \item{N_AGED}{Number of aged observations contributing across all quarters}
#'   \item{P_SUM}{Sum of p_q used in the collapse (after filtering/renormalization)}
#' }
#'
#' @export
collapse_catch_weighted_wtaa <- function(pred,
                                        catch_qtr,
                                        gear_mode = c("combined", "by_gear"),
                                        sex_mode  = c("combined", "split"),
                                        renormalize = TRUE,
                                        require_catch_years = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }

  gear_mode <- match.arg(gear_mode)
  sex_mode  <- match.arg(sex_mode)

  DT <- data.table::as.data.table

  P <- DT(pred)
  C <- DT(catch_qtr)
  names(P) <- toupper(names(P))
  names(C) <- toupper(names(C))

  # ---- validate required columns ----
  need_p <- c("YEAR", "QUARTER", "AGE_YRS", "WEIGHT_KG")
  miss_p <- setdiff(need_p, names(P))
  if (length(miss_p) > 0) stop("pred missing columns: ", paste(miss_p, collapse = ", "), call. = FALSE)

  need_c <- c("YEAR", "QUARTER", "P_Q")
  miss_c <- setdiff(need_c, names(C))
  if (length(miss_c) > 0) stop("catch_qtr missing columns: ", paste(miss_c, collapse = ", "), call. = FALSE)

  # ---- coerce core types ----
  suppressWarnings({
    P[, YEAR := as.integer(as.character(YEAR))]
    P[, QUARTER := as.integer(as.character(QUARTER))]
    P[, AGE_YRS := as.integer(as.character(AGE_YRS))]
    P[, WEIGHT_KG := as.numeric(WEIGHT_KG)]

    C[, YEAR := as.integer(as.character(YEAR))]
    C[, QUARTER := as.integer(as.character(QUARTER))]
    C[, P_Q := as.numeric(P_Q)]
  })

  P <- P[is.finite(WEIGHT_KG) & !is.na(WEIGHT_KG)]
  P <- P[QUARTER %in% 1:4 & !is.na(YEAR) & !is.na(AGE_YRS)]
  C <- C[QUARTER %in% 1:4 & !is.na(YEAR) & is.finite(P_Q) & !is.na(P_Q)]

  if (nrow(P) == 0) stop("pred has 0 usable rows after filtering.", call. = FALSE)
  if (nrow(C) == 0) stop("catch_qtr has 0 usable rows after filtering.", call. = FALSE)

  # ---- enforce modes (collapse dimensions if requested) ----
  if (gear_mode == "combined") {
    # ignore gear in both pred and catch weights
    if ("GEAR2" %in% names(P)) P[, GEAR2 := NULL]
  } else {
    if (!"GEAR2" %in% names(P)) stop("gear_mode='by_gear' requires GEAR2 in pred.", call. = FALSE)
    P[, GEAR2 := as.integer(as.character(GEAR2))]
    # catch weights may or may not be gear-specific; if missing, will be treated as overall weights
    if ("GEAR2" %in% names(C)) C[, GEAR2 := as.integer(as.character(GEAR2))]
  }

  if (sex_mode == "combined") {
    if ("SEX" %in% names(P)) P[, SEX := NULL]
  } else {
    if (!"SEX" %in% names(P)) stop("sex_mode='split' requires SEX in pred.", call. = FALSE)
    P[, SEX := toupper(trimws(as.character(SEX)))]
    P <- P[SEX %in% c("F", "M")]
  }

  # ---- check catch years coverage ----
  pred_years <- sort(unique(P$YEAR))
  catch_years <- sort(unique(C$YEAR))
  missing_years <- setdiff(pred_years, catch_years)

  if (length(missing_years) > 0 && isTRUE(require_catch_years)) {
    stop("catch_qtr is missing YEARS present in pred: ",
         paste(missing_years, collapse = ", "),
         call. = FALSE)
  }

  # ---- compute quarter mean weight-at-age (mu) and counts ----
  by_mu <- c("YEAR", "QUARTER", "AGE_YRS")
  if (gear_mode == "by_gear") by_mu <- c(by_mu, "GEAR2")
  if (sex_mode  == "split")  by_mu <- c(by_mu, "SEX")

  mu <- P[, .(
    MU = mean(WEIGHT_KG, na.rm = TRUE),
    N  = .N
  ), by = by_mu]

  # ---- join catch weights ----
  by_c <- c("YEAR", "QUARTER")
  if (gear_mode == "by_gear" && "GEAR2" %in% names(C)) {
    by_c <- c(by_c, "GEAR2")
  }

  # join: if catch weights are not gear-specific, merge by YEAR+QUARTER and apply to all gears
  if (gear_mode == "by_gear" && !"GEAR2" %in% names(C)) {
    mu <- merge(mu, C[, .(YEAR, QUARTER, P_Q)], by = c("YEAR", "QUARTER"), all.x = TRUE)
  } else {
    mu <- merge(mu, C[, c(by_c, "P_Q"), with = FALSE], by = by_c, all.x = TRUE)
  }

  mu[is.na(P_Q) | !is.finite(P_Q), P_Q := 0]

  # ---- collapse to annual catch-weighted mean ----
  by_y <- setdiff(by_mu, "QUARTER")

  if (isTRUE(renormalize)) {
    # Renormalize p_q over quarters available for each group
    out <- mu[, .(
      WAA = {
        psum <- sum(P_Q, na.rm = TRUE)
        if (psum > 0) sum(P_Q * MU, na.rm = TRUE) / psum else NA_real_
      },
      N_AGED = sum(N, na.rm = TRUE),
      P_SUM  = sum(P_Q, na.rm = TRUE)
    ), by = by_y]
  } else {
    # Treat missing quarters as zero contribution (no renormalization)
    out <- mu[, .(
      WAA = sum(P_Q * MU, na.rm = TRUE),
      N_AGED = sum(N, na.rm = TRUE),
      P_SUM  = sum(P_Q, na.rm = TRUE)
    ), by = by_y]
  }

  data.table::setorder(out, YEAR, AGE_YRS)
  out
}
