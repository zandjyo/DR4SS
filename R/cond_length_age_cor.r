#' Create conditional age-at-length compositions from survey ages
#'
#' Queries specimen-level survey age data (via \code{survey_age.sql}) and builds a
#' conditional age-at-length composition matrix suitable for the Stock Synthesis
#' conditional age-at-length (CAAL) format. Ages are pooled into a plus group at
#' \code{max_age}, and lengths (cm) are assigned to user-supplied Stock Synthesis
#' length bins (\code{len_bins}).
#'
#' This function produces a single matrix (returned as \code{output$norm}) with
#' one row per YEAR x LENGTH_BIN combination. Each row contains the sample size
#' (optionally reweighted) and the conditional age distribution across ages
#' 0:\code{max_age}.
#'
#' The SQL template \code{survey_age.sql} must include the placeholder flags:
#' \itemize{
#'   \item \code{-- insert survey}
#'   \item \code{-- insert start_year}
#'   \item \code{-- insert species}
#' }
#'
#' @param con_akfin A DBI connection to the AKFIN database.
#' @param species Numeric species code used in the SQL query.
#' @param area Character area identifier: one of \code{"GOA"}, \code{"AI"}, \code{"BS"}, \code{"SLOPE"}.
#' @param start_year Numeric start year (inclusive) used for the SQL query.
#' @param max_age Integer maximum age (plus group). Ages > \code{max_age} are pooled into \code{max_age}.
#' @param len_bins Numeric vector of Stock Synthesis length-bin lower edges (in cm).
#' @param wt Numeric weighting factor applied to input sample sizes (ISS).
#' @param seas Integer SS season value written into the output matrix.
#' @param flt Integer SS fleet/survey index written into the output matrix.
#' @param ageerr Integer SS age-error definition index written into the output matrix.
#'
#' @return A named list with element \code{norm}, where \code{norm} is a numeric matrix with columns:
#' \describe{
#'   \item{1}{YEAR}
#'   \item{2}{Seas}
#'   \item{3}{Flt/Svy}
#'   \item{4}{Sex (0 = combined)}
#'   \item{5}{Part (left as 0)}
#'   \item{6}{Ageerr}
#'   \item{7}{Lbin_lo}
#'   \item{8}{Lbin_hi}
#'   \item{9}{Nsamp (optionally reweighted by \code{wt})}
#'   \item{10+}{Conditional age proportions for ages 0:\code{max_age}}
#' }
#'
#' @export
cond_length_age_cor <- function(con_akfin,
                                species,
                                area,
                                start_year,
                                max_age,
                                len_bins,
                                wt = 1,
                                seas = 1,
                                flt = 2,
                                ageerr = 1) {

  if (missing(con_akfin) || is.null(con_akfin)) {
    stop("`con_akfin` must be a valid DBI connection.", call. = FALSE)
  }
  if (missing(species) || length(species) != 1L || !is.numeric(species)) {
    stop("`species` must be a single numeric species code.", call. = FALSE)
  }
  if (missing(start_year) || length(start_year) != 1L || !is.numeric(start_year)) {
    stop("`start_year` must be a single numeric year.", call. = FALSE)
  }
  if (missing(max_age) || length(max_age) != 1L || !is.numeric(max_age) || max_age < 0) {
    stop("`max_age` must be a single non-negative numeric value.", call. = FALSE)
  }
  max_age <- as.integer(max_age)

  if (missing(len_bins) || length(len_bins) < 2L || !is.numeric(len_bins)) {
    stop("`len_bins` must be a numeric vector of length-bin lower edges (length >= 2).", call. = FALSE)
  }
  len_bins <- sort(unique(as.numeric(len_bins)))

  if (!is.numeric(wt) || length(wt) != 1L || wt <= 0) {
    stop("`wt` must be a single positive numeric value.", call. = FALSE)
  }

  area <- toupper(area)
  survey <- switch(
    area,
    "GOA"   = 47,
    "AI"    = 52,
    "BS"    = c(98, 143),
    "SLOPE" = 78,
    stop("Invalid `area`. Use GOA, AI, BS, or SLOPE.", call. = FALSE)
  )

  # ---- pull specimen ages using packaged SQL ----
  Age_sql <- sql_reader("survey_age.sql")

  Age_sql <- sql_filter("IN",  survey,     Age_sql, flag = "-- insert survey",     value_type = "numeric")
  Age_sql <- sql_filter(">=",  start_year, Age_sql, flag = "-- insert start_year", value_type = "numeric")
  Age_sql <- sql_filter("=",   species,    Age_sql, flag = "-- insert species",    value_type = "numeric")

  Age <- sql_run(con_akfin, Age_sql) |>
    data.table::as.data.table() |>
    dplyr::rename_with(toupper)

  need <- c("YEAR", "LENGTH_MM", "AGE")
  miss <- setdiff(need, names(Age))
  if (length(miss) > 0) {
    stop("survey_age.sql result missing required columns: ", paste(miss, collapse = ", "), call. = FALSE)
  }

  # ---- build length in cm and bin ----
  Age[, LENGTH := as.integer(LENGTH_MM / 10)]
  idx <- findInterval(Age$LENGTH, len_bins, rightmost.closed = FALSE, all.inside = TRUE)
  Age[, BIN := len_bins[idx]]

  end_year <- lubridate::year(Sys.Date())
  years <- seq(start_year, end_year, 1)

  dt <- Age[YEAR %in% years & LENGTH > 0 & AGE > 0]
  if (nrow(dt) == 0) {
    return(list(norm = matrix(numeric(0), nrow = 0)))
  }

  # ---- plus group ----
  dt[, AGE_FILT := AGE]
  dt[AGE_FILT > max_age, AGE_FILT := max_age]
  dt[, AGE_FILT := as.integer(AGE_FILT)]

  # ---- counts by YEAR x BIN x AGE ----
  counts <- dt[, .N, by = .(YEAR, BIN, AGE_FILT)]

  # ensure complete YEAR x BIN x AGE grid (so missing ages become 0)
  year_bins <- unique(counts[, .(YEAR, BIN)])
  data.table::setorder(year_bins, YEAR, BIN)

  age_levels <- 0:max_age
  full <- data.table::CJ(
    YEAR = year_bins$YEAR,
    BIN = year_bins$BIN,
    AGE_FILT = age_levels,
    unique = TRUE
  )

  counts_full <- merge(full, counts, by = c("YEAR", "BIN", "AGE_FILT"), all.x = TRUE)
  counts_full[is.na(N), N := 0L]

  # Nsamp per YEAR x BIN
  nsamp_dt <- counts_full[, .(NSAMP = sum(N)), by = .(YEAR, BIN)]
  nsamp_dt[, NSAMP := as.numeric(NSAMP) * wt]

  # wide matrix: one row per YEAR x BIN, columns AGE_FILT
  wide <- data.table::dcast(
    counts_full,
    YEAR + BIN ~ AGE_FILT,
    value.var = "N",
    fill = 0
  )
  data.table::setorder(wide, YEAR, BIN)

  # conditional proportions
  age_cols <- as.character(age_levels)
  denom <- rowSums(as.matrix(wide[, ..age_cols]))
  denom[denom == 0] <- NA_real_

  props <- as.matrix(wide[, ..age_cols]) / denom
  props[is.na(props)] <- 0

  # align Nsamp to wide (YEAR,BIN) order deterministically
  nsamp_dt <- merge(wide[, .(YEAR, BIN)], nsamp_dt, by = c("YEAR", "BIN"), all.x = TRUE)
  nsamp_dt[is.na(NSAMP), NSAMP := 0]

  # ---- assemble SS matrix ----
  num_rows <- nrow(wide)
  nsamples_col <- 9L
  num_ages <- length(age_levels)
  num_cols <- nsamples_col + num_ages

  Agecomp_obs <- matrix(0, nrow = num_rows, ncol = num_cols)

  Agecomp_obs[, 1] <- wide$YEAR
  Agecomp_obs[, 2] <- seas
  Agecomp_obs[, 3] <- flt
  Agecomp_obs[, 4] <- 0
  Agecomp_obs[, 5] <- 0
  Agecomp_obs[, 6] <- ageerr
  Agecomp_obs[, 7] <- wide$BIN
  Agecomp_obs[, 8] <- wide$BIN
  Agecomp_obs[, 9] <- nsamp_dt$NSAMP

  Agecomp_obs[, 10:num_cols] <- props

  list(norm = Agecomp_obs)
}
