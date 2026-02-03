#' Fishery EWAA WTAGE and NAGE tables (optionally split by sex)
#'
#' Pull fishery age/length/weight data using dom_age_wt2.sql from int/sql, optionally
#' trim extreme weights using quantile GAMs, fit a Gamma(log) GAM to predict weights
#' for aged fish, and return Stock Synthesis-style wide matrices for weights-at-age
#' (WTAGE) and counts-at-age (NAGE). Optionally split outputs by sex (F and M).
#'
#' Uses package SQL helpers `sql_read()`, `sql_filter()`, and `sql_run()`.
#'
#' @param con A DBI connection to the AKFIN database.
#' @param species Fishery species code (numeric/integer; e.g., 202).
#' @param region Either:
#'   (1) region code(s) among c("AI","BS","GOA","BSWGOA") (default), OR
#'   (2) a numeric vector of NMFS areas directly (will be used as-is).
#' @param maxage Plus-group age (ages > maxage are pooled to maxage).
#' @param split_sex Logical. If TRUE, fit/predict within sex and return F/M tables.
#' @param year_min Minimum YEAR to include (default 2007 like your older scripts).
#' @param trim_outliers Logical. If TRUE, use qgam 0.01/0.99 envelopes to trim weights.
#' @param trim_q Numeric length-2 quantiles used when trim_outliers=TRUE.
#'
#' @return If split_sex=FALSE: list(F_WTAGE=..., F_NAGE=...).
#'   If split_sex=TRUE: list(F=list(F_WTAGE=..., F_NAGE=...), M=list(F_WTAGE=..., F_NAGE=...)).
#' @export
fishery_ewaa <- function(con,
                         species = 202,
                         region = c("AI", "BS", "GOA", "BSWGOA"),
                         maxage = 10,
                         split_sex = FALSE,
                         year_min = 2007,
                         trim_outliers = TRUE,
                         trim_q = c(0.01, 0.99),
                         min_strata_n = 30) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required (add to Imports).", call. = FALSE)
  }
  if (!requireNamespace("mgcv", quietly = TRUE)) {
    stop("Package 'mgcv' is required (add to Imports).", call. = FALSE)
  }
  if (isTRUE(trim_outliers) && !requireNamespace("qgam", quietly = TRUE)) {
    stop("trim_outliers=TRUE requires package 'qgam' (add to Imports or set trim_outliers=FALSE).",
         call. = FALSE)
  }

  DT <- data.table::as.data.table

  # ---- region -> area mapping (unless numeric areas supplied) ----
  region_map <- list(
    AI     = 540:544,
    BS     = 500:539,
    GOA    = 600:699,
    BSWGOA = c(500:539, 610:620)
  )

  if (is.numeric(region)) {
    area <- sort(unique(as.integer(region)))
  } else {
    reg <- unique(toupper(as.character(region)))
    bad <- setdiff(reg, names(region_map))
    if (length(bad) > 0) {
      stop("Unknown region: ", paste(bad, collapse = ", "),
           ". Allowed: ", paste(names(region_map), collapse = ", "),
           " or supply numeric NMFS areas directly.", call. = FALSE)
    }
    area <- sort(unique(unlist(region_map[reg])))
  }

  # ---- read SQL from package and inject filters ----
  #sql_code= readLines('int/sql/dom_age_wt2.sql')
  sql_code <- sql_reader("dom_age_wt2.sql")
  sql_code <- sql_filter(sql_precode = "IN", x = species, sql_code = sql_code, flag = "-- insert species")
  sql_code <- sql_filter(sql_precode = "IN", x = area,    sql_code = sql_code, flag = "-- insert location")

  d <- DT(sql_run(con, sql_code))
  if (nrow(d) == 0) stop("Fishery query returned 0 rows.", call. = FALSE)

  names(d) <- toupper(names(d))

  need <- c("YEAR", "MONTH", "AGE", "LENGTH", "WEIGHT", "GEAR", "NMFS_AREA", "SEX")
  miss <- setdiff(need, names(d))
  if (length(miss) > 0) {
    stop("Fishery query missing required columns: ", paste(miss, collapse = ", "), call. = FALSE)
  }

  suppressWarnings({
    d[, YEAR   := as.integer(as.character(YEAR))]
    d[, MONTH  := as.integer(as.character(MONTH))]
    d[, AGE    := as.integer(round(as.numeric(AGE)))]
    d[, LENGTH := as.numeric(LENGTH)]
    d[, WEIGHT := as.numeric(WEIGHT)]
    d[, NMFS_AREA := as.integer(as.character(NMFS_AREA))]
  })

  d <- d[YEAR >= year_min]

  # derived AREA and QUARTER
  if(region %in% c("BS","GOA","BSWGOA")){
    d[, AREA := trunc(NMFS_AREA / 10)]
    d[AREA == 50, AREA := 51]
  }
  
  d[, AREA := as.factor(AREA)]

  d[, QUARTER := 4L]
  d[MONTH < 3, QUARTER := 1L]
  d[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
  d[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
  d[, QUARTER := as.factor(QUARTER)]

  d[, GEAR := as.factor(GEAR)]
  d[, SEX := toupper(trimws(as.character(SEX)))]

  # clean WEIGHT
  d[WEIGHT == 0 | WEIGHT > 50, WEIGHT := NA_real_]

  if (isTRUE(split_sex)) {
    d <- d[SEX %in% c("F", "M")]
  }

  # separate datasets: weights for fitting and ages for prediction
  W <- d[!is.na(WEIGHT) & !is.na(LENGTH) & !is.na(YEAR) & !is.na(MONTH)]
  A <- d[!is.na(AGE)    & !is.na(LENGTH) & !is.na(YEAR) & !is.na(MONTH)]

  if (nrow(W) < 50) stop("Not enough non-missing WEIGHT records to fit GAM (n < 50).", call. = FALSE)
  if (nrow(A) == 0) stop("No AGE records available for prediction after filtering.", call. = FALSE)

  # ---- optional outlier trimming using qgam envelopes ----
  trim_weight_data <- function(WW) {
    if (!isTRUE(trim_outliers)) return(WW)

    q_lo <- min(trim_q); q_hi <- max(trim_q)
    if (!(q_lo > 0 && q_hi < 1 && q_lo < q_hi)) {
      stop("trim_q must be two quantiles in (0,1) with trim_q[1] < trim_q[2].", call. = FALSE)
    }

    q_hi_fit <- qgam::qgam(
      WEIGHT ~ s(LENGTH, k = 15) + s(MONTH, k = 10),
      data = WW, qu = q_hi
    )
    q_lo_fit <- qgam::qgam(
      WEIGHT ~ s(LENGTH, k = 15) + s(MONTH, k = 10),
      data = WW, qu = q_lo
    )

    WW[, W_HI := stats::predict(q_hi_fit, newdata = WW, type = "response")]
    WW[, W_LO := stats::predict(q_lo_fit, newdata = WW, type = "response")]

    WW2 <- WW[WEIGHT <= W_HI & WEIGHT >= W_LO]
    WW2[, c("W_HI", "W_LO") := NULL]
    WW2
  }

  # ---- helper: check strata counts for selected factors ----
  strata_ok <- function(WW, factors, min_n) {
    if (length(factors) == 0) return(TRUE)
    # counts per interaction cell
    tmp <- WW[, .N, by = factors]
    all(tmp$N >= min_n)
  }

  # ---- fit/predict with adaptive strata dropping ----
  fit_predict <- function(WW, AA) {

    WW <- trim_weight_data(WW)

    # ensure factor classes for strata vars
    WW[, GEAR := as.factor(GEAR)]
    WW[, AREA := as.factor(AREA)]
    WW[, QUARTER := as.factor(QUARTER)]

    # drop unused levels
    WW[, GEAR := droplevels(GEAR)]
    WW[, AREA := droplevels(AREA)]
    WW[, QUARTER := droplevels(QUARTER)]

    # Determine which factor terms can be supported
    # Start with full: GEAR + AREA + QUARTER; if any cell < min_strata_n, drop in order.
    keep <- c("GEAR", "AREA", "QUARTER")
    drop_order <- c("GEAR", "AREA", "QUARTER")

    while (length(keep) > 0 && !strata_ok(WW, keep, min_strata_n)) {
      # drop the next term in the required order (if still present)
      to_drop <- drop_order[drop_order %in% keep][1]
      keep <- setdiff(keep, to_drop)
    }

    # After dropping all three, we allow model without strata terms.
    # But we still require enough rows to fit a GAM sensibly.
    if (nrow(WW) < 30) {
      stop("Not enough WEIGHT observations to fit even the reduced model (n < 30).", call. = FALSE)
    }

    # If we dropped everything because cells were too sparse, that's OK.
    # But if we *still* fail strata check (shouldn't happen), error.
    if (length(keep) > 0 && !strata_ok(WW, keep, min_strata_n)) {
      stop("Not enough data in strata even after dropping GEAR, AREA, and QUARTER.", call. = FALSE)
    }

    # Build formula with kept factors
    rhs <- c("YEAR", "s(LENGTH, k = 12)", keep)
    fml <- stats::as.formula(paste("WEIGHT ~", paste(rhs, collapse = " + ")))

    # Align AA factor levels to WW for any kept factors; drop AA rows with unseen levels
    if ("GEAR" %in% keep) {
      AA[, GEAR := factor(GEAR, levels = levels(WW$GEAR))]
    }
    if ("AREA" %in% keep) {
      AA[, AREA := factor(AREA, levels = levels(WW$AREA))]
    }
    if ("QUARTER" %in% keep) {
      AA[, QUARTER := factor(QUARTER, levels = levels(WW$QUARTER))]
    }

    # Remove rows that can't be predicted because of unseen factor levels
    if (length(keep) > 0) {
      bad <- FALSE
      for (v in keep) bad <- bad | is.na(AA[[v]])
      if (any(bad)) {
        AA <- AA[!bad]
      }
    }

    if (nrow(AA) == 0) {
      stop("After aligning factor levels, no AGE rows remain for prediction.", call. = FALSE)
    }

    fit <- mgcv::gam(
      formula = fml,
      family = stats::Gamma(link = "log"),
      data = WW,
      method = "REML"
    )

    AA[, WEIGHTP := stats::predict(fit, newdata = AA, type = "response")]
    AA[AGE > maxage, AGE := maxage]

    DT(data.table::data.table(
      Source    = "Fishery",
      Weight_kg = AA$WEIGHTP,
      Sex       = AA$SEX,
      Age_yrs   = AA$AGE,
      Length_cm = AA$LENGTH,
      Month     = AA$MONTH,
      Year      = AA$YEAR
    ))
  }

  # ---- build WTAGE + NAGE ----
  build_tables <- function(dd) {
    dd <- dd[!is.na(Age_yrs)]
    if (nrow(dd) == 0) return(list(F_WTAGE = NULL, F_NAGE = NULL))

    WT <- weight_at_age_wide(dd, value = "weight", maxage = maxage)
    NG <- weight_at_age_wide(dd, value = "count",  maxage = maxage)

    WT <- WT[order(as.integer(WT[["#Yr"]])), , drop = FALSE]
    NG <- NG[order(as.integer(NG[["#Yr"]])), , drop = FALSE]

    WT <- fill_wtage_matrix(WT, option = "row")
    NG[is.na(NG)] <- 0

    list(F_WTAGE = WT, F_NAGE = NG)
  }

  # ---- combined vs split_sex ----
  if (!isTRUE(split_sex)) {
    F_data <- fit_predict(W, A)
    return(build_tables(F_data))
  }

  out <- list()
  for (sx in c("F", "M")) {
    WW <- W[SEX == sx]
    AA <- A[SEX == sx]

    if (nrow(WW) < 50 || nrow(AA) == 0) {
      out[[sx]] <- list(F_WTAGE = NULL, F_NAGE = NULL)
      next
    }

    F_data <- fit_predict(WW, AA)
    out[[sx]] <- build_tables(F_data)
  }

  out
}

