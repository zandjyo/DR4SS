#' Get survey age compositions and format for Stock Synthesis
#'
#' Pulls survey age composition data from the AKFIN database (default) and formats
#' it into the Stock Synthesis age-composition matrix layout. Optionally, age
#' compositions can be sourced from a provided VAST age-composition table instead
#' of querying AKFIN.
#'
#' This function preserves legacy behavior from historical assessment scripts:
#' \itemize{
#'   \item Ages greater than or equal to \code{max_age} are pooled into a plus group.
#'   \item For Bering Sea (\code{area = "BS"}), an additional AKFIN query is run for
#'         \code{area_id = 99901}, \code{survey = 98}, and \code{YEAR <= 1986}, and
#'         appended to the main result set.
#'   \item When \code{use_vast = TRUE}, records are filtered to \code{Region == "Both"}
#'         and \code{Year != 2020} to match legacy processing.
#'   \item When \code{split_sex = TRUE}, sex-specific age compositions are returned in
#'         Stock Synthesis split-sex format: one row per year with male age bins first
#'         (\code{M0...Mmax_age}) and female age bins second (\code{F0...Fmax_age}).
#'   \item Unsexed fish (\code{SEX == 3}) are split 50:50 between males
#'         (\code{SEX == 1}) and females (\code{SEX == 2}) before final normalization.
#'   \item When \code{split_sex = FALSE}, sexes are pooled across all records.
#' }
#'
#' @param con_akfin A DBI connection to the AKFIN database. Required when
#'   \code{use_vast = FALSE}.
#' @param use_vast Logical; if TRUE, use \code{vast_agecomp} instead of querying AKFIN.
#' @param vast_agecomp A data.frame/data.table containing VAST age compositions.
#'   Expected columns include \code{Region}, \code{Year}, and age columns (0,1,2,...).
#'   If \code{split_sex = TRUE}, it must also contain a sex column named \code{SEX}
#'   (or \code{Sex}/\code{sex}).
#' @param species Numeric species code.
#' @param start_yr Numeric start year (used when querying AKFIN: \code{YEAR >= start_yr}).
#' @param area Character; one of \code{"GOA"}, \code{"AI"}, \code{"BS"}, \code{"SLOPE"}.
#' @param max_age Integer maximum age (plus group). Ages >= \code{max_age} are pooled.
#' @param seas Integer SS season.
#' @param flt Integer SS fleet/survey index.
#' @param gender Integer SS gender code used when \code{split_sex = FALSE}.
#' @param split_sex Logical; if \code{TRUE}, return split-sex age compositions with
#'   one row per year and male bins first, then female bins. Unsexed fish
#'   (\code{SEX == 3}) are split 50:50 between sexes.
#' @param part Integer SS partition code.
#' @param ageerr Integer SS age error definition index.
#' @param lgin_lo Integer SS Lbin_lo (legacy field in SS agecomp block).
#' @param lgin_hi Integer SS Lbin_hi (legacy field in SS agecomp block).
#'
#' @return A data.frame formatted for the Stock Synthesis age-composition data block.
#'   If \code{split_sex = FALSE}, columns are:
#'   YEAR, Seas, FltSvy, Gender, Part, Ageerr, Lgin_lo, Lgin_hi, Nsamp, F0..Fmax_age.
#'   If \code{split_sex = TRUE}, columns are:
#'   YEAR, Seas, FltSvy, Gender, Part, Ageerr, Lgin_lo, Lgin_hi, Nsamp,
#'   M0..Mmax_age, F0..Fmax_age.
#'
#' @export
GET_SURVEY_ACOMP <- function(con_akfin = NULL,
                             use_vast = FALSE,
                             vast_agecomp = NULL,
                             species,
                             start_yr,
                             area = "BS",
                             max_age = 12,
                             seas = 1,
                             flt = 2,
                             gender = 1,
                             split_sex = FALSE,
                             part = 0,
                             ageerr = 0,
                             lgin_lo = 1,
                             lgin_hi = 120) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package `data.table` is required.", call. = FALSE)
  }

  if (!isTRUE(use_vast)) {
    if (is.null(con_akfin)) stop("`con_akfin` must be provided when use_vast = FALSE.", call. = FALSE)
  } else {
    if (is.null(vast_agecomp)) stop("`vast_agecomp` must be provided when use_vast = TRUE.", call. = FALSE)
  }
  if (missing(species) || length(species) != 1L || !is.numeric(species)) {
    stop("`species` must be a single numeric species code.", call. = FALSE)
  }
  if (missing(start_yr) || length(start_yr) != 1L || !is.numeric(start_yr)) {
    stop("`start_yr` must be a single numeric year.", call. = FALSE)
  }
  if (length(max_age) != 1L || !is.numeric(max_age) || max_age < 0) {
    stop("`max_age` must be a single non-negative numeric value.", call. = FALSE)
  }
  if (!is.logical(split_sex) || length(split_sex) != 1L) {
    stop("`split_sex` must be TRUE or FALSE.", call. = FALSE)
  }

  max_age <- as.integer(max_age)

  # ---- area mapping (numeric IDs) ----
  area <- toupper(area)
  area_map <- switch(
    area,
    "GOA"   = list(survey = 47,         area_id = 99903),
    "AI"    = list(survey = 52,         area_id = 99904),
    "BS"    = list(survey = c(98, 143), area_id = c(99900, 99902)),
    "SLOPE" = list(survey = 78,         area_id = 99905),
    stop("Unknown `area`: ", area, ". Use GOA, AI, BS, or SLOPE.", call. = FALSE)
  )

  if (area == "SLOPE") {
    stop("There are no age compositions worked up for SLOPE in the database.", call. = FALSE)
  }

  # ---- helpers ----
  .standardize_sex_col <- function(dt) {
    dt <- data.table::as.data.table(dt)
    sex_candidates <- c("SEX", "Sex", "sex")
    sex_hit <- intersect(sex_candidates, names(dt))
    if (length(sex_hit) == 0L) {
      stop("Age composition data must contain a SEX column when `split_sex = TRUE`.", call. = FALSE)
    }
    if (sex_hit[1] != "SEX") data.table::setnames(dt, sex_hit[1], "SEX")
    dt[, SEX := suppressWarnings(as.integer(as.character(SEX)))]
    dt
  }

  .split_unsexed_agecomp <- function(dt) {
    dt <- data.table::as.data.table(dt)

    if (!"SEX" %in% names(dt)) {
      stop("Expected column `SEX` not found.", call. = FALSE)
    }
    if (!all(c("YEAR", "AGE", "AGEPOP") %in% names(dt))) {
      stop("Agecomp table must contain YEAR, AGE, and AGEPOP.", call. = FALSE)
    }

    dt[, SEX := suppressWarnings(as.integer(as.character(SEX)))]

    known <- dt[SEX %in% c(1L, 2L)]

    unk <- dt[SEX == 3L]
    if (nrow(unk) > 0L) {
      unk_m <- data.table::copy(unk)
      unk_f <- data.table::copy(unk)
      unk_m[, `:=`(SEX = 1L, AGEPOP = AGEPOP / 2)]
      unk_f[, `:=`(SEX = 2L, AGEPOP = AGEPOP / 2)]
      dt <- data.table::rbindlist(list(known, unk_m, unk_f), use.names = TRUE, fill = TRUE)
    } else {
      dt <- known
    }

    dt <- dt[SEX %in% c(1L, 2L)]
    dt <- dt[, .(AGEPOP = sum(AGEPOP, na.rm = TRUE)), by = .(YEAR, SEX, AGE)]
    data.table::setorder(dt, YEAR, SEX, AGE)
    dt
  }

  .apply_plus_group <- function(dt, max_age, split_sex = FALSE) {
    dt <- data.table::as.data.table(dt)

    needed <- c("YEAR", "AGE", "AGEPOP")
    if (split_sex) needed <- c(needed, "SEX")
    if (!all(needed %in% names(dt))) {
      stop("Agecomp table is missing required columns: ",
           paste(setdiff(needed, names(dt)), collapse = ", "), call. = FALSE)
    }

    by_cols <- if (split_sex) c("YEAR", "SEX", "AGE") else c("YEAR", "AGE")
    base_cols <- if (split_sex) c("YEAR", "SEX") else c("YEAR")

    dt1 <- dt[AGE < max_age]
    dt2 <- dt[AGE >= max_age]

    if (nrow(dt2) > 0) {
      dt2[, AGE := max_age]
      dt2 <- dt2[, .(AGEPOP = sum(AGEPOP, na.rm = TRUE)), by = by_cols]
      dt <- data.table::rbindlist(list(dt1, dt2), use.names = TRUE)
    } else {
      dt <- dt1
    }

    dt <- dt[, .(AGEPOP = sum(AGEPOP, na.rm = TRUE)), by = by_cols]
    data.table::setorderv(dt, c(base_cols, "AGE"))
    dt
  }

  .normalize_rows <- function(df, start_col = 10L) {
    num <- as.matrix(df[, start_col:ncol(df), drop = FALSE])
    rs <- rowSums(num, na.rm = TRUE)
    ok <- rs > 0
    if (any(ok)) {
      num[ok, ] <- num[ok, , drop = FALSE] / rs[ok]
    }
    if (any(!ok)) {
      num[!ok, ] <- 0
    }
    df[, start_col:ncol(df)] <- num
    df
  }

  # ---- Nsamp (haul/sample counts) from AKFIN ----
  Count_sql <- sql_reader("AKFIN_count_AKFIN.sql")
  Count_sql <- sql_filter("IN", species, Count_sql, flag = "-- insert species", value_type = "numeric")
  Count_sql <- sql_filter("IN", area_map$survey, Count_sql, flag = "-- insert survey", value_type = "numeric")

  Count <- sql_run(con_akfin, Count_sql) |>
    data.table::as.data.table() |>
    dplyr::rename_with(toupper)

  if (!all(c("YEAR", "HAULS") %in% names(Count))) {
    stop("count_AKFIN.sql must return YEAR and HAULS.", call. = FALSE)
  }

  # ---- build Acomp either from AKFIN query or VAST ----
  if (!isTRUE(use_vast)) {

    Age_sql <- sql_reader("survey_agecomp_AKFIN.sql")
    Age_sql <- sql_filter("IN", area_map$area_id, Age_sql, flag = "-- insert area_id", value_type = "numeric")
    Age_sql <- sql_filter("=", species, Age_sql, flag = "-- insert species", value_type = "numeric")
    Age_sql <- sql_filter(">=", start_yr, Age_sql, flag = "-- insert start_year", value_type = "numeric")

    Acomp <- sql_run(con_akfin, Age_sql) |>
      data.table::as.data.table() |>
      dplyr::rename_with(toupper)

    need_cols <- c("YEAR", "AGE", "AGEPOP")
    if (isTRUE(split_sex)) need_cols <- c(need_cols, "SEX")
    if (!all(need_cols %in% names(Acomp))) {
      stop("survey_agecomp_AKFIN.sql must return: ",
           paste(need_cols, collapse = ", "), call. = FALSE)
    }

    if (area == "BS") {
      Age_sql2 <- sql_reader("survey_agecomp_AKFIN.sql")
      Age_sql2 <- sql_filter("IN", 99901, Age_sql2, flag = "-- insert area_id", value_type = "numeric")
      Age_sql2 <- sql_filter("=", species, Age_sql2, flag = "-- insert species", value_type = "numeric")
      Age_sql2 <- sql_filter("<=", 1986, Age_sql2, flag = "-- insert start_year", value_type = "numeric")

      Acomp2 <- sql_run(con_akfin, Age_sql2) |>
        data.table::as.data.table() |>
        dplyr::rename_with(toupper)

      if (nrow(Acomp2) > 0) {
        Acomp <- data.table::rbindlist(list(Acomp2, Acomp), use.names = TRUE, fill = TRUE)
      }
    }

    if (isTRUE(split_sex)) {
      Acomp <- .standardize_sex_col(Acomp)
      Acomp <- .split_unsexed_agecomp(Acomp)
    } else {
      Acomp <- Acomp[, .(AGEPOP = sum(AGEPOP, na.rm = TRUE)), by = .(YEAR, AGE)]
    }

    Acomp <- .apply_plus_group(Acomp, max_age, split_sex = split_sex)

    YR <- sort(unique(Acomp$YEAR))
    if (isTRUE(split_sex)) {
      grid <- data.table::CJ(YEAR = YR, SEX = 1:2, AGE = 0:max_age)
      Acomp <- merge(grid, Acomp, by = c("YEAR", "SEX", "AGE"), all.x = TRUE)
      Acomp[is.na(AGEPOP), AGEPOP := 0]
      data.table::setorder(Acomp, YEAR, SEX, AGE)
    } else {
      grid <- data.table::as.data.table(expand.grid(AGE = 0:max_age, YEAR = YR))
      Acomp <- merge(grid, Acomp, by = c("YEAR", "AGE"), all.x = TRUE)
      Acomp[is.na(AGEPOP), AGEPOP := 0]
      data.table::setorder(Acomp, YEAR, AGE)
    }

  } else {

    Proportions <- data.table::as.data.table(vast_agecomp)

    if (all(c("Region", "Year") %in% names(Proportions))) {
      Proportions <- Proportions[tolower(Region) == "both" & Year != 2020]
    } else {
      stop("vast_agecomp must include columns Region and Year (legacy expectation).", call. = FALSE)
    }

    if (isTRUE(split_sex)) {
      Proportions <- .standardize_sex_col(Proportions)
      Proportions <- Proportions[SEX %in% c(1L, 2L, 3L)]
    }

    age_cols <- setdiff(names(Proportions), c("Region", "Year", "SEX"))
    age_num <- suppressWarnings(as.integer(age_cols))
    keep_age <- !is.na(age_num)

    if (!any(keep_age)) {
      stop("vast_agecomp must contain age columns named 0,1,2,...", call. = FALSE)
    }

    age_cols <- age_cols[keep_age]
    age_num <- age_num[keep_age]
    ord <- order(age_num)
    age_cols <- age_cols[ord]
    age_num <- age_num[ord]

    max_available <- max(age_num, na.rm = TRUE)
    if (max_age > max_available) max_age <- max_available

    long <- data.table::melt(
      Proportions,
      id.vars = intersect(c("Region", "Year", "SEX"), names(Proportions)),
      measure.vars = age_cols,
      variable.name = "AGE",
      value.name = "AGEPOP"
    )
    long[, AGE := as.integer(as.character(AGE))]
    long[, YEAR := as.integer(Year)]
    long[is.na(AGEPOP), AGEPOP := 0]

    if (isTRUE(split_sex)) {
      Acomp <- long[, .(YEAR, SEX, AGE, AGEPOP)]
      Acomp <- .split_unsexed_agecomp(Acomp)
    } else {
      Acomp <- long[, .(AGEPOP = sum(AGEPOP, na.rm = TRUE)), by = .(YEAR, AGE)]
    }

    Acomp <- .apply_plus_group(Acomp, max_age, split_sex = split_sex)

    YR <- sort(unique(Acomp$YEAR))
    if (isTRUE(split_sex)) {
      grid <- data.table::CJ(YEAR = YR, SEX = 1:2, AGE = 0:max_age)
      Acomp <- merge(grid, Acomp, by = c("YEAR", "SEX", "AGE"), all.x = TRUE)
      Acomp[is.na(AGEPOP), AGEPOP := 0]
      data.table::setorder(Acomp, YEAR, SEX, AGE)
    } else {
      grid <- data.table::as.data.table(expand.grid(AGE = 0:max_age, YEAR = YR))
      Acomp <- merge(grid, Acomp, by = c("YEAR", "AGE"), all.x = TRUE)
      Acomp[is.na(AGEPOP), AGEPOP := 0]
      data.table::setorder(Acomp, YEAR, AGE)
    }
  }

  # ---- build SS output ----
  years <- sort(unique(Acomp$YEAR))

  Nsamp_df <- merge(
    data.frame(YEAR = years),
    data.frame(YEAR = Count$YEAR, HAULS = Count$HAULS),
    by = "YEAR",
    all.x = TRUE
  )
  Nsamp <- Nsamp_df$HAULS
  Nsamp[is.na(Nsamp)] <- 0

  if (!isTRUE(split_sex)) {

    FIN <- (max_age + 1L) + 9L
    y <- matrix(ncol = FIN, nrow = length(years))
    SS_out <- as.data.frame(y)

    names(SS_out) <- c(
      "YEAR", "Seas", "FltSvy", "Gender", "Part", "Ageerr", "Lgin_lo", "Lgin_hi", "Nsamp",
      paste0("F", 0:max_age)
    )

    SS_out$YEAR <- years
    SS_out$Seas <- seas
    SS_out$FltSvy <- flt
    SS_out$Gender <- gender
    SS_out$Part <- part
    SS_out$Ageerr <- ageerr
    SS_out$Lgin_lo <- lgin_lo
    SS_out$Lgin_hi <- lgin_hi
    SS_out$Nsamp <- Nsamp

    for (i in seq_along(years)) {
      vals <- Acomp$AGEPOP[Acomp$YEAR == years[i]]
      if (length(vals) != (max_age + 1L)) {
        stop("Unexpected number of age bins for YEAR = ", years[i], call. = FALSE)
      }
      SS_out[i, 10:FIN] <- vals
    }

    SS_out <- .normalize_rows(SS_out, start_col = 10L)
    return(SS_out)

    } else {

    # split-sex SS format: one row per year with M bins followed by F bins
    FIN <- 9L + 2L * (max_age + 1L)
    y <- matrix(ncol = FIN, nrow = length(years))
    SS_out <- as.data.frame(y)

    names(SS_out) <- c(
      "YEAR", "Seas", "FltSvy", "Gender", "Part", "Ageerr", "Lgin_lo", "Lgin_hi", "Nsamp",
      paste0("F", 0:max_age),
      paste0("M", 0:max_age)
    )

    SS_out$YEAR <- years
    SS_out$Seas <- seas
    SS_out$FltSvy <- flt
    SS_out$Gender <- 3
    SS_out$Part <- part
    SS_out$Ageerr <- ageerr
    SS_out$Lgin_lo <- lgin_lo
    SS_out$Lgin_hi <- lgin_hi
    SS_out$Nsamp <- Nsamp

    for (i in seq_along(years)) {
      yr <- years[i]

      # SEX==1 male, SEX==2 female
      m_vals <- Acomp$AGEPOP[Acomp$YEAR == yr & Acomp$SEX == 1L]
      f_vals <- Acomp$AGEPOP[Acomp$YEAR == yr & Acomp$SEX == 2L]

      if (length(m_vals) != (max_age + 1L)) {
        stop("Unexpected number of male age bins for YEAR = ", yr, call. = FALSE)
      }
      if (length(f_vals) != (max_age + 1L)) {
        stop("Unexpected number of female age bins for YEAR = ", yr, call. = FALSE)
      }

      # M bins first, then F bins
      SS_out[i, 10:(9 + max_age + 1L)] <- f_vals
      SS_out[i, (10 + max_age + 1L):FIN] <- m_vals
    }

    # Normalize across BOTH sexes together so the full row sums to 1
    SS_out <- .normalize_rows(SS_out, start_col = 10L)

    SS_out <- SS_out[order(SS_out$YEAR), ]
    rownames(SS_out) <- NULL
    return(SS_out)
  }
}

# Backward-compatible alias
#' @export
GET_ACOMP <- GET_SURVEY_ACOMP