#' Survey EWAA WTAGE and NAGE tables (optionally split by sex)
#'
#' Pull survey age/length/weight data using srv_ages.sql from int/sql, fill missing
#' weights with a GAM W~L model (default mgcv settings; fallback to LM if GAM fails),
#' and return Stock Synthesis-style wide matrices for weights-at-age (WTAGE) and
#' counts-at-age (NAGE). Optionally split outputs by sex (F and M).
#'
#' Uses package SQL helpers `sql_read()`, `sql_filter()`, and `sql_run()` from sql_utils.R.
#'
#' @param con A DBI connection to the AFSC database.
#' @param species Survey species code (numeric/integer).
#' @param region Region code(s) for the survey SQL (character; can be length > 1).
#' @param maxage Plus-group age (ages > maxage are pooled to maxage).
#' @param split_sex Logical. If TRUE, return separate tables for females (F) and males (M),
#'   and fit the GAM/LM weight-fill models within each sex. If FALSE (default), fit a single
#'   model on combined data.
#'
#' @return If split_sex=FALSE: list(S_WTAGE=..., S_NAGE=...).
#'   If split_sex=TRUE: list(F=list(S_WTAGE=..., S_NAGE=...), M=list(S_WTAGE=..., S_NAGE=...)).
#' @export
survey_ewaa <- function(con,
                        species = 21720,
                        region  = "BS",
                        maxage  = 10,
                        split_sex = FALSE) {

  if (!requireNamespace("mgcv", quietly = TRUE)) {
    stop("Package 'mgcv' is required for GAM weight filling (add to Imports).", call. = FALSE)
  }
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required (add to Imports).", call. = FALSE)
  }

  # --- read SQL from installed package (int/sql) ---
  sql_code <- sql_reader("srv_ages.sql")
  
    # --- inject species + region ---
  sql_code <- sql_filter(sql_precode = "IN", x = species, sql_code = sql_code, flag = "-- insert species")
  sql_code <- sql_filter(sql_precode = "IN", x = region,  sql_code = sql_code,
                         flag = "-- insert region", value_type = "character")

  # --- run ---
  d <- data.table::as.data.table(sql_run(con, sql_code))

  # --- validate required columns (Sex only required if split_sex=TRUE) ---
  need <- c("Year", "Age_yrs", "Length_cm", "Weight_kg")
  miss <- setdiff(need, names(d))
  if (length(miss) > 0) {
    stop("Survey query is missing required columns: ", paste(miss, collapse = ", "), call. = FALSE)
  }
  if (isTRUE(split_sex) && !("Sex" %in% names(d))) {
    stop("split_sex=TRUE but survey query did not return column 'Sex'.", call. = FALSE)
  }

  # --- stabilize types ---
  if (is.factor(d$Year)) d[, Year := as.character(Year)]
  suppressWarnings({
    y <- as.integer(d$Year)
    if (!all(is.na(y))) d[, Year := y]
  })

  suppressWarnings({
    d[, Age_yrs   := as.integer(round(as.numeric(Age_yrs)))]
    d[, Length_cm := as.numeric(Length_cm)]
    d[, Weight_kg := as.numeric(Weight_kg)]
  })

  # treat 0 weights as missing
  d[Weight_kg == 0, Weight_kg := NA_real_]

  # Provide Source for consistency with EWAA utils if not present
  if (!("Source" %in% names(d))) d[, Source := "Survey"]

  # --- helper: fit/predict missing weights on the provided dataset (i.e., within a sex if split) ---
  fill_weights_gam_lm <- function(dd) {
    obs <- dd[!is.na(Weight_kg) & !is.na(Length_cm)]
    mis <- dd[ is.na(Weight_kg) & !is.na(Length_cm)]

    if (nrow(mis) == 0) return(dd)
    if (nrow(obs) == 0) return(dd)

    # data-driven sanity checks (avoid degenerate fits)
    if (length(unique(obs$Length_cm)) < 5) return(dd)
    if (nrow(obs) < 10) return(dd)

    obs[, logW := log(Weight_kg)]
    obs[, logL := log(Length_cm)]
    mis[, logL := log(Length_cm)]

    # GAM first (default mgcv settings), fallback to LM if GAM fails
    pred <- tryCatch({
      fit <- mgcv::gam(logW ~ mgcv::s(logL), data = obs)
      exp(stats::predict(fit, newdata = mis, type = "response"))
    }, error = function(e) {
      tryCatch({
        fit <- stats::lm(logW ~ logL, data = obs)
        exp(stats::predict(fit, newdata = mis))
      }, error = function(e2) {
        rep(NA_real_, nrow(mis))
      })
    })

    mis[, Weight_kg := pred]

    obs[, c("logW", "logL") := NULL]
    mis[, "logL" := NULL]

    data.table::rbindlist(list(obs, mis), use.names = TRUE, fill = TRUE)
  }

  # --- helper: build WTAGE + NAGE ---
  build_tables <- function(dd) {
    dd <- dd[!is.na(Age_yrs)]
    if (nrow(dd) == 0) return(list(S_WTAGE = NULL, S_NAGE = NULL))

    WT <- weight_at_age_wide(dd, value = "weight", maxage = maxage)
    NG <- weight_at_age_wide(dd, value = "count",  maxage = maxage)

    WT <- WT[order(as.integer(WT[["#Yr"]])), , drop = FALSE]
    NG <- NG[order(as.integer(NG[["#Yr"]])), , drop = FALSE]

    WT <- fill_wtage_matrix(WT, option = "row")
    list(S_WTAGE = WT, S_NAGE = NG)
  }

  # --- combined vs sex-split ---
  if (!isTRUE(split_sex)) {
    d2 <- fill_weights_gam_lm(d)    # combined model
    return(build_tables(d2))
  }

  # split by sex (always F and M), and fit models within each sex
  if (is.factor(d$Sex)) d[, Sex := as.character(Sex)]
  d[, Sex := toupper(trimws(Sex))]
  d <- d[Sex %in% c("F", "M")]

  out <- list()
  for (sx in c("F", "M")) {
    dd  <- d[Sex == sx]
    dd2 <- fill_weights_gam_lm(dd)  # sex-specific model
    out[[sx]] <- build_tables(dd2)
  }

  out
}
