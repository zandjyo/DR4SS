#' Collapse season-specific predicted weights-at-age to annual catch-weighted WAA
#'
#' @description
#' Computes season-specific mean predicted weight-at-age from aged observations and
#' collapses to an annual mean weight-at-age using seasonal catch proportions from
#' blend catch. Supports output combined or split by gear (\code{GEAR2}) and/or sex (\code{SEX}),
#' and optionally by region (\code{REGION_GRP}) when present in both inputs.
#'
#' @details
#' For a given grouping (e.g., year-age, optionally gear/sex/region), the catch-weighted
#' annual mean is:
#' \deqn{w_{y,a} = \sum_s p_{y,s} \mu_{y,a,s}}
#' where \eqn{\mu_{y,a,s}} is the mean predicted weight in season \eqn{s} and
#' \eqn{p_{y,s}} is the catch proportion in that season (from blend catch).
#'
#' If some seasons have no observations for a given year-age-group, you can either:
#' \itemize{
#'   \item renormalize \eqn{p_{y,s}} over seasons with available means (\code{renormalize = TRUE}, default), or
#'   \item treat missing seasons as contributing zero (\code{renormalize = FALSE}).
#' }
#'
#' Catch weights (\code{P_Q}) may be gear-specific (include \code{GEAR2} in \code{catch_seas})
#' or overall (no \code{GEAR2} column), in which case the same seasonal proportions are applied
#' to all gears in \code{pred}.
#'
#' @param pred A \code{data.table} or \code{data.frame} from \code{predict_weights_for_aged()}
#'   with columns \code{YEAR}, \code{SEASON}, \code{AGE_YRS}, \code{WEIGHT_KG}, and optionally
#'   \code{SEX}, \code{GEAR2}, \code{REGION_GRP}.
#' @param catch_seas A \code{data.table} or \code{data.frame} from \code{build_catch_qtr_from_blend()}
#'   (seasonal/quarterly catch proportions) with columns \code{YEAR}, \code{SEASON}, \code{P_Q},
#'   and optionally \code{GEAR2} and/or \code{REGION_GRP}.
#' @param gear_mode Character; either \code{"combined"} (default) to pool across gear, or
#'   \code{"by_gear"} to retain gear-specific output (requires \code{GEAR2} in \code{pred}).
#' @param sex_mode Character; either \code{"combined"} (default) to pool across sex, or
#'   \code{"split"} to retain sex-specific output (requires \code{SEX} in \code{pred}).
#' @param renormalize Logical; if TRUE (default), renormalize \code{P_Q} within each group over
#'   seasons with available means; if FALSE, do not renormalize.
#' @param require_catch_years Logical; if TRUE (default), error if any \code{YEAR} in \code{pred}
#'   is missing from \code{catch_seas}. If FALSE, missing years receive \code{P_Q = 0}.
#'
#' @return A \code{data.table} with columns:
#' \describe{
#'   \item{YEAR}{Integer year}
#'   \item{AGE_YRS}{Integer age (plus-grouping should already be applied in \code{pred})}
#'   \item{GEAR2}{Integer gear group if \code{gear_mode = "by_gear"}, otherwise omitted}
#'   \item{SEX}{\code{"F"} or \code{"M"} if \code{sex_mode = "split"}, otherwise omitted}
#'   \item{REGION_GRP}{Region label if present in both inputs, otherwise omitted}
#'   \item{WAA}{Catch-weighted annual mean weight-at-age}
#'   \item{N_AGED}{Number of aged observations contributing across all seasons}
#'   \item{P_SUM}{Sum of \code{P_Q} over seasons included in the collapse (pre-renormalization)}
#' }
#'
#' @importFrom data.table as.data.table fifelse setorder
#' @export
collapse_catch_weighted_wtaa <- function(pred,
                                        catch_seas,
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
  C <- DT(catch_seas)
  names(P) <- toupper(names(P))
  names(C) <- toupper(names(C))

  # ---- validate required columns ----
  need_p <- c("YEAR","SEASON","AGE_YRS", "WEIGHT_KG")
  miss_p <- setdiff(need_p, names(P))
  if (length(miss_p) > 0) stop("pred missing columns: ", paste(miss_p, collapse = ", "), call. = FALSE)

  need_c <- c("YEAR", "SEASON", "P_Q")
  miss_c <- setdiff(need_c, names(C))
  if (length(miss_c) > 0) stop("catch_seas missing columns: ", paste(miss_c, collapse = ", "), call. = FALSE)

  # ---- coerce core types ----
  suppressWarnings({
    P[, YEAR := as.integer(as.character(YEAR))]
    P[, AGE_YRS := as.integer(as.character(AGE_YRS))]
    P[, WEIGHT_KG := as.numeric(WEIGHT_KG)]
    P[, SEASON := as.character(SEASON)]
    C[, SEASON := as.character(SEASON)]

    C[, YEAR := as.integer(as.character(YEAR))]
    C[, P_Q := as.numeric(P_Q)]
  })

  P <- P[is.finite(WEIGHT_KG) & !is.na(WEIGHT_KG)]
  P <- P[!is.na(SEASON)  & !is.na(YEAR) & !is.na(AGE_YRS)]
  C <- C[!is.na(SEASON) & !is.na(YEAR) & is.finite(P_Q) & !is.na(P_Q)]

  if (nrow(P) == 0) stop("pred has 0 usable rows after filtering.", call. = FALSE)
  if (nrow(C) == 0) stop("catch_qtr has 0 usable rows after filtering.", call. = FALSE)


  # ---- ensure catch weights are unique at the join keys ----
  by_c <- c("YEAR", "SEASON")
  if (gear_mode == "by_gear" && "GEAR2" %in% names(C)) by_c <- c(by_c, "GEAR2")
  if ("REGION_GRP" %in% names(P) && "REGION_GRP" %in% names(C)) by_c <- c(by_c, "REGION_GRP")

  # If duplicates exist, collapse them (sum is typical for proportions built from components)
  if (C[, anyDuplicated(.SD), .SDcols = by_c] > 0) {
    C <- C[, .(P_Q = sum(P_Q, na.rm = TRUE)), by = by_c]
  }

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

  # ---- compute season mean weight-at-age (mu) and counts ----
  by_mu <- c("YEAR", "SEASON", "AGE_YRS")
  if (gear_mode == "by_gear") by_mu <- c(by_mu, "GEAR2")
  if (sex_mode  == "split")  by_mu <- c(by_mu, "SEX")
  if ("REGION_GRP" %in% names(P) && "REGION_GRP" %in% names(C)){by_mu <- c(by_mu, "REGION_GRP")}

  mu <- P[, .(
    MU = mean(WEIGHT_KG, na.rm = TRUE),
    N  = .N
  ), by = by_mu]

  # ---- join catch weights ----
  by_c <- c("YEAR", "SEASON")
  if (gear_mode == "by_gear" && "GEAR2" %in% names(C)) {
    by_c <- c(by_c, "GEAR2")
  }
  if ("REGION_GRP" %in% names(P) && "REGION_GRP" %in% names(C)){
    by_c <- c(by_c, "REGION_GRP")
  }
  

  # join: if catch weights are not gear-specific, merge by YEAR+SEASON and apply to all gears
  if (gear_mode == "by_gear" && !"GEAR2" %in% names(C)) {
    if("REGION_GRP" %in% names(C)){
      mu <- merge(mu, C[, .(YEAR, REGION_GRP, SEASON, P_Q)], by = c("YEAR", "REGION_GRP","SEASON"), all.x = TRUE)
      } else{mu <- merge(mu, C[, .(YEAR, SEASON, P_Q)], by = c("YEAR", "SEASON"), all.x = TRUE)}
    } else { mu <- merge(mu, C[, c(by_c, "P_Q"), with = FALSE], by = by_c, all.x = TRUE)
  }
    

  mu[is.na(P_Q) | !is.finite(P_Q), P_Q := 0]

  # ---- collapse to annual catch-weighted mean ----
  by_y <- setdiff(by_mu, "SEASON")

  if (isTRUE(renormalize)) {
    # Renormalize p_q over season available for each group
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
