#' Fit a length–weight key using fishery length–weight data
#'
#' @param fish_data data.table from get_fishery_age_wt_data()
#' @param trim_outliers logical; if TRUE, trim extreme weights using qgam envelope on s(LENGTH)
#' @param trim_q length-2 quantiles for trimming
#' @param min_strata_n minimum N per stratum cell before retaining a factor term
#' @param allow_year,allow_quarter,allow_gear,allow_region,allow_area,allow_sex include those factor terms if supported
#'
#' @return list(model=mgcv::gam, kept_terms=character, data_used=data.table)
#' @export
fit_lw_key <- function(fish_data,
                       trim_outliers = TRUE,
                       trim_q = c(0.01, 0.99),
                       min_strata_n = 1,
                       allow_year = TRUE,
                       allow_season = TRUE,
                       allow_gear = TRUE,
                       allow_region= TRUE,
                       allow_area = FALSE,
                       allow_sex = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) stop("Need data.table.", call. = FALSE)
  if (!requireNamespace("mgcv", quietly = TRUE)) stop("Need mgcv.", call. = FALSE)
  if (isTRUE(trim_outliers) && !requireNamespace("qgam", quietly = TRUE)) {
    stop("trim_outliers=TRUE requires qgam.", call. = FALSE)
  }

  dt <- data.table::as.data.table(fish_data)
  names(dt) <- toupper(names(dt))

  need <- c("LENGTH", "WEIGHT")
  miss <- setdiff(need, names(dt))
  if (length(miss) > 0) stop("fish_data missing: ", paste(miss, collapse = ", "), call. = FALSE)

  # LW records
  dt <- dt[!is.na(LENGTH) & !is.na(WEIGHT)]
  dt <- dt[is.finite(LENGTH) & is.finite(WEIGHT)]
  if (nrow(dt) < 100) stop("Not enough LW records to fit LW key (n < 100).", call. = FALSE)

  # optional trimming in WEIGHT~s(LENGTH)
  if (isTRUE(trim_outliers)) {
    q_lo <- min(trim_q); q_hi <- max(trim_q)
    if (!(q_lo > 0 && q_hi < 1 && q_lo < q_hi)) stop("Bad trim_q.", call. = FALSE)

    q_hi_fit <- qgam::qgam(WEIGHT ~ s(LENGTH, k = 15), data = dt, qu = q_hi)
    q_lo_fit <- qgam::qgam(WEIGHT ~ s(LENGTH, k = 15), data = dt, qu = q_lo)

    dt[, W_HI := stats::predict(q_hi_fit, newdata = dt, type = "response")]
    dt[, W_LO := stats::predict(q_lo_fit, newdata = dt, type = "response")]
    dt <- dt[WEIGHT <= W_HI & WEIGHT >= W_LO]
    dt[, c("W_HI","W_LO") := NULL]
    if (nrow(dt) < 100) stop("Too many rows removed by trimming; insufficient LW data.", call. = FALSE)
  }

  # choose candidate factor terms
  candidates <- character()
  if (allow_year    && "YEAR"    %in% names(dt)) candidates <- c(candidates, "YEAR")
  if (allow_season  && "SEASON" %in% names(dt) && length(unique(dt$SEASON)) > 1 ) candidates <- c(candidates, "SEASON")
  if (allow_region  && "REGION_GRP" %in% names(dt) && length(unique(dt$REGION_GRP)) > 1 ) candidates <- c(candidates, "REGION_GRP")
  if (allow_gear    && "GEAR2"   %in% names(dt)) candidates <- c(candidates, "GEAR2")
  if (allow_area    && "AREA"    %in% names(dt)&& length(unique(dt$AREA)) > 1 ) candidates <- c(candidates, "AREA")
  if (allow_sex     && "SEX"     %in% names(dt)) candidates <- c(candidates, "SEX")

  # force factors
  for (v in candidates) dt[, (v) := droplevels(as.factor(get(v)))]

  strata_ok <- function(d, vars, min_n) {
    if (length(vars) == 0) return(TRUE)
    tmp <- d[, .N, by = vars]
    all(tmp$N >= min_n)
  }

  # drop order: gear -> region -> area -> quarter -> sex -> year (year last)
  drop_order <- c("GEAR2","REGION_GRP","AREA","SEASON","SEX","YEAR")
  kept <- intersect(drop_order, candidates)

  while (length(kept) > 0 && !strata_ok(dt, kept, min_strata_n)) {
    kept <- kept[-1]
  }
  if (nrow(dt) < 50) stop("Insufficient LW data after strata checks.", call. = FALSE)

  rhs <- c("s(LENGTH, k = 12)", kept)
  fml <- stats::as.formula(paste("WEIGHT ~", paste(rhs, collapse = " + ")))

  fit <- mgcv::gam(
    formula = fml,
    family = stats::Gamma(link = "log"),
    data = dt,
    method = "REML"
  )

  list(model = fit, kept_terms = kept, data_used = dt)
}
