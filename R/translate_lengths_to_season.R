#' Translate AL bin lengths to a target season using delta-length model
#'
#' Applies delta length (from fit_quarter_growth_model) to the representative length
#' for each AL bin to obtain predicted length in target quarter q*:
#'   L_QSTAR = L_BAR + DELTA_L
#'
#' @param al_emp Empirical age-length table (e.g., from build_empirical_length_at_age()).
#'   Must include YEAR, AGE_YRS, QUARTER, BIN_ID, L_BAR, P_LEN, N.
#'   Optionally includes GEAR2 and/or SEX depending on the model.
#' @param growth_fit Output list from fit_quarter_growth_model().
#' @param clamp Logical; if TRUE, clamp translated lengths to min_L, max_L.
#' @param min_L,max_L Optional numeric bounds for clamping. If NULL, taken from L_BAR range.
#'
#' @return al_emp with added columns:
#'   - DELTA_L (applied delta)
#'   - L_QSTAR (translated length)
#'
#' @export
translate_lengths_to_season <- function(al_emp,
                                        growth_fit,
                                        clamp = TRUE,
                                        min_L = NULL,
                                        max_L = NULL) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  DT <- data.table::as.data.table

  if (is.null(growth_fit$deltas)) stop("growth_fit must come from fit_seasonal_growth_model().", call. = FALSE)

  AL <- DT(al_emp)
  names(AL) <- toupper(names(AL))

  if(isTRUE(any(names(growth_fit$deltas) %in% "REGION_GRP")) && isTRUE(any(names(growth_fit$deltas)%in% "AREA"))) AL[, AREA:=NULL]

  req <- c("YEAR","AGE_YRS","SEASON","BIN_ID","L_BAR","P_LEN","N")
  miss <- setdiff(req, names(AL))
  if (length(miss) > 0) stop("al_emp missing: ", paste(miss, collapse=", "), call. = FALSE)

  suppressWarnings({
    AL[, YEAR    := as.integer(as.character(YEAR))]
    AL[, AGE_YRS := as.integer(as.character(AGE_YRS))]
    AL[, BIN_ID  := as.integer(as.character(BIN_ID))]
    AL[, L_BAR   := as.numeric(L_BAR)]
    AL[, P_LEN   := as.numeric(P_LEN)]
    AL[, N    := as.numeric(N)]
  })

  # mode consistency: only require columns that exist in growth_fit deltas
  deltas <- DT(growth_fit$deltas)
  names(deltas) <- toupper(names(deltas))

  # keys used in deltas
  key <- c("YEAR","AGE_YRS","REGION_GRP","SEASON")
  if ("GEAR2" %in% names(deltas)) {
    if (!"GEAR2" %in% names(AL)) stop("growth_fit expects GEAR2 but al_emp lacks it.", call. = FALSE)
    suppressWarnings(AL[, GEAR2 := as.integer(as.character(GEAR2))])
    key <- c(key, "GEAR2")
  }
  if ("SEX" %in% names(deltas)) {
    if (!"SEX" %in% names(AL)) stop("growth_fit expects SEX but al_emp lacks it.", call. = FALSE)
    AL[, SEX := toupper(trimws(as.character(SEX)))]
    key <- c(key, "SEX")
  }

  # join delta onto AL; note: delta defined for observed quarter q (AL$QUARTER),
  # translating from q to q*
  keep_delta <- c(key, "DELTA_L")
  AL2 <- merge(AL, deltas[, ..keep_delta], by = key, all.x = TRUE)

  # if some DELTA_L are missing (e.g., no data to define growth in that cell),
  # fallback to 0 (no translation) — conservative
  AL2[is.na(DELTA_L), DELTA_L := 0]

  # translate lengths to target quarter
  AL2[, L_QSTAR := L_BAR + DELTA_L]

  # optional clamping
  if (isTRUE(clamp)) {
    if (is.null(min_L)) min_L <- suppressWarnings(min(AL2$L_BAR, na.rm = TRUE))
    if (is.null(max_L)) max_L <- suppressWarnings(max(AL2$L_BAR, na.rm = TRUE))
    AL2[, L_QSTAR := pmin(pmax(L_QSTAR, min_L), max_L)]
  }

  AL2[]
}
