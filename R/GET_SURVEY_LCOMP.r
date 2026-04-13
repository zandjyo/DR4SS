#' Get survey length compositions from AKFIN and format for Stock Synthesis
#'
#' Pulls survey length composition data and associated haul/sample counts from
#' the AKFIN database for a specified region and species. Length data may be
#' binned into Stock Synthesis (SS) length bins and optionally formatted into
#' the SS3 length-composition matrix layout (one row per year).
#'
#' Sex handling:
#' If sex=TRUE, returns sex-specific compositions with females first then males,
#' and splits unknown sex (SEX==3) 50:50 between females (2) and males (1).
#' When sex=TRUE, proportions are normalized within YEAR across both sexes
#' combined, so the full row sums to 1.
#'
#' @param con_akfin A DBI connection to the AKFIN database.
#' @param species Numeric observer species code.
#' @param bins Numeric vector of SS length-bin lower edges (e.g., seq(3.5,119.5,1)).
#' @param bin Logical; if TRUE, bin integer lengths into `bins` using BIN_LEN_DATA().
#' @param area Character area identifier: one of "GOA", "AI", "BS", "SLOPE".
#' @param sex Logical; if FALSE, aggregate across sex. If TRUE, return female+male with unknowns split 50/50.
#' @param SS Logical; if TRUE, format output as an SS3 length-comp matrix.
#' @param seas Integer SS season.
#' @param flt Integer SS fleet/survey index.
#' @param gender Integer SS gender code (typical: 1=female, 2=male, 3=both).
#'               If sex=TRUE and SS=TRUE, this will be forced to 3 unless you override intentionally.
#' @param part Integer SS partition code.
#'
#' @return If SS=TRUE, a numeric matrix formatted for Stock Synthesis.
#'   Otherwise, a data.table of compositions.
#'
#' @export
GET_SURVEY_LCOMP <- function(con_akfin,
                             species = 202,
                             bins = seq(4.5, 119.5, 5),
                             bin = TRUE,
                             area = "BS",
                             sex = FALSE,
                             SS = TRUE,
                             seas = 1,
                             flt = 3,
                             gender = 1,
                             part = 0) {

  if (missing(con_akfin) || is.null(con_akfin)) {
    stop("`con_akfin` must be a valid DBI connection to AKFIN.", call. = FALSE)
  }
  if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table required.", call. = FALSE)
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("dplyr required.", call. = FALSE)

  DT <- data.table::as.data.table

  if (!is.logical(sex) || length(sex) != 1L) {
    stop("`sex` must be TRUE or FALSE.", call. = FALSE)
  }

  # ---- area mapping ----
  area <- toupper(area)
  area_map <- switch(
    area,
    "GOA"   = list(survey = 47,        area_id = 99903),
    "AI"    = list(survey = 52,        area_id = 99904),
    "BS"    = list(survey = c(98,143), area_id = c(99900,99901,99902)),
    "SLOPE" = list(survey = 78,        area_id = 99905),
    stop("Unknown `area`: ", area, ". Use GOA, AI, BS, or SLOPE.", call. = FALSE)
  )
  if (area == "SLOPE") stop("There are no survey size compositions available for SLOPE.", call. = FALSE)

  # ---- haul/sample counts (Nsamp) ----
  Count_sql <- sql_reader("AKFIN_count_AKFIN.sql")
  Count_sql <- sql_filter("IN", species, Count_sql, flag = "-- insert species", value_type = "numeric")
  Count_sql <- sql_filter("IN", area_map$survey, Count_sql, flag = "-- insert survey", value_type = "numeric")

  Count <- sql_run(con_akfin, Count_sql) |>
    DT() |>
    dplyr::rename_with(toupper)

  if (!all(c("YEAR", "HAULS", "LENGTHS") %in% names(Count))) {
    stop("AKFIN_count_AKFIN.sql must return columns YEAR, HAULS, LENGTHS.", call. = FALSE)
  }

  # ---- length compositions ----
  lcomp_sql <- sql_reader("length_comp_AKFIN.sql")
  lcomp_sql <- sql_filter("IN", species, lcomp_sql, flag = "-- insert species", value_type = "numeric")
  lcomp_sql <- sql_filter("IN", area_map$area_id, lcomp_sql, flag = "-- insert area_id", value_type = "numeric")

  lcomp <- sql_run(con_akfin, lcomp_sql) |>
    DT() |>
    dplyr::rename_with(toupper)

  need <- c("YEAR", "LENGTH", "TOTAL")
  if (!all(need %in% names(lcomp))) stop("length_comp_AKFIN.sql must return YEAR, LENGTH, TOTAL.", call. = FALSE)

  if (isTRUE(sex)) {
    if (!"SEX" %in% names(lcomp)) {
      stop("sex=TRUE requires `SEX` column from length_comp_AKFIN.sql (SEX: 1=male, 2=female, 3=unknown).", call. = FALSE)
    }
    lcomp[, SEX := suppressWarnings(as.integer(as.character(SEX)))]
  }

  # ---- zero-fill YEAR x LENGTH (and SEX if needed) ----
  len <- 0:(max(bins) + 1)
  yrs <- sort(unique(lcomp$YEAR))

  if (!isTRUE(sex)) {
    # combine sexes
    lcomp <- lcomp[, .(TOTAL = sum(TOTAL, na.rm = TRUE)), by = .(YEAR, LENGTH)]
    grid <- DT(expand.grid(YEAR = yrs, LENGTH = len, stringsAsFactors = FALSE))
    lcomp <- merge(grid, lcomp, by = c("YEAR", "LENGTH"), all.x = TRUE)
    lcomp[is.na(TOTAL), TOTAL := 0]
  } else {
    # keep sex-specific; include SEX=1,2,3 so unknowns can be split
    lcomp <- lcomp[, .(TOTAL = sum(TOTAL, na.rm = TRUE)), by = .(YEAR, LENGTH, SEX)]
    grid <- DT(expand.grid(YEAR = yrs, LENGTH = len, SEX = c(1L, 2L, 3L), stringsAsFactors = FALSE))
    lcomp <- merge(grid, lcomp, by = c("YEAR", "LENGTH", "SEX"), all.x = TRUE)
    lcomp[is.na(TOTAL), TOTAL := 0]
  }

  # ---- binning ----
  if (isTRUE(bin)) {
    if (!exists("BIN_LEN_DATA", mode = "function")) {
      stop("BIN_LEN_DATA() not found. Please load/attach the package that provides it.", call. = FALSE)
    }
    lcomp <- BIN_LEN_DATA(DT(lcomp), len_bins = bins)

    if (!isTRUE(sex)) {
      lcomp <- lcomp[, .(TOTAL = sum(TOTAL, na.rm = TRUE)), by = .(YEAR, BIN)]
      grid2 <- DT(expand.grid(YEAR = sort(unique(lcomp$YEAR)), BIN = bins, stringsAsFactors = FALSE))
      lcomp <- merge(grid2, lcomp, by = c("YEAR", "BIN"), all.x = TRUE)
      lcomp[is.na(TOTAL), TOTAL := 0]
      lcomp[, T_NUMBER := sum(TOTAL), by = YEAR]
      lcomp[, PROP := data.table::fifelse(T_NUMBER > 0, TOTAL / T_NUMBER, 0)]
      lcomp <- lcomp[, .(YEAR, BIN, PROP)]
    } else {
      lcomp <- lcomp[, .(TOTAL = sum(TOTAL, na.rm = TRUE)), by = .(YEAR, BIN, SEX)]
      grid2 <- DT(expand.grid(YEAR = sort(unique(lcomp$YEAR)), BIN = bins, SEX = c(1L, 2L, 3L), stringsAsFactors = FALSE))
      lcomp <- merge(grid2, lcomp, by = c("YEAR", "BIN", "SEX"), all.x = TRUE)
      lcomp[is.na(TOTAL), TOTAL := 0]

      # Split unknown sex (3) 50/50 into female (2) and male (1), then drop 3
      u <- lcomp[SEX == 3L]
      if (nrow(u) > 0) {
        u_half <- data.table::copy(u)
        u_half[, TOTAL := TOTAL / 2]
        u_f <- data.table::copy(u_half); u_f[, SEX := 2L]
        u_m <- data.table::copy(u_half); u_m[, SEX := 1L]
        lcomp <- data.table::rbindlist(list(lcomp[SEX != 3L], u_f, u_m), use.names = TRUE, fill = TRUE)
        lcomp <- lcomp[, .(TOTAL = sum(TOTAL, na.rm = TRUE)), by = .(YEAR, BIN, SEX)]
      }

      # Normalize within YEAR across both sexes combined
      lcomp <- lcomp[SEX %in% c(1L, 2L)]
      lcomp[, T_NUMBER := sum(TOTAL), by = YEAR]
      lcomp[, PROP := data.table::fifelse(T_NUMBER > 0, TOTAL / T_NUMBER, 0)]
      lcomp <- lcomp[, .(YEAR, BIN, SEX, PROP)]
    }
  } else {
    # unbinned proportions; keep as LENGTH and TOTAL
    if (!isTRUE(sex)) {
      lcomp[, T_NUMBER := sum(TOTAL), by = YEAR]
      lcomp[, PROP := data.table::fifelse(T_NUMBER > 0, TOTAL / T_NUMBER, 0)]
      lcomp <- lcomp[, .(YEAR, LENGTH, PROP)]
    } else {
      # split unknowns at LENGTH level
      u <- lcomp[SEX == 3L]
      if (nrow(u) > 0) {
        u_half <- data.table::copy(u)
        u_half[, TOTAL := TOTAL / 2]
        u_f <- data.table::copy(u_half); u_f[, SEX := 2L]
        u_m <- data.table::copy(u_half); u_m[, SEX := 1L]
        lcomp <- data.table::rbindlist(list(lcomp[SEX != 3L], u_f, u_m), use.names = TRUE, fill = TRUE)
        lcomp <- lcomp[, .(TOTAL = sum(TOTAL, na.rm = TRUE)), by = .(YEAR, LENGTH, SEX)]
      }
      lcomp <- lcomp[SEX %in% c(1L, 2L)]
      lcomp[, T_NUMBER := sum(TOTAL), by = YEAR]
      lcomp[, PROP := data.table::fifelse(T_NUMBER > 0, TOTAL / T_NUMBER, 0)]
      lcomp <- lcomp[, .(YEAR, LENGTH, SEX, PROP)]
    }
  }

  # ---- Stock Synthesis matrix ----
  if (isTRUE(SS)) {
    years <- sort(unique(lcomp$YEAR))
    Nsamp_df <- merge(
      data.frame(YEAR = years),
      Count[, .(YEAR, HAULS)],
      by = "YEAR",
      all.x = TRUE
    )
    Nsamp <- Nsamp_df$HAULS
    Nsamp[is.na(Nsamp)] <- 0

    if (!isTRUE(sex)) {
      # combined-sex matrix (nbins + 6)
      bins_use <- if (isTRUE(bin)) bins else sort(unique(lcomp$LENGTH))
      nbin <- length(bins_use)
      nyr <- length(years)

      x <- matrix(ncol = nbin + 6, nrow = nyr)
      x[, 2] <- seas
      x[, 3] <- flt
      x[, 4] <- gender
      x[, 5] <- part
      x[, 6] <- Nsamp

      for (i in seq_len(nyr)) {
        x[i, 1] <- years[i]
        if (isTRUE(bin)) {
          x[i, 7:(nbin + 6)] <- lcomp$PROP[lcomp$YEAR == years[i]][order(match(lcomp$BIN[lcomp$YEAR == years[i]], bins_use))]
        } else {
          xi <- lcomp[lcomp$YEAR == years[i], ]
          xi <- xi[order(xi$LENGTH), ]
          x[i, 7:(nbin + 6)] <- xi$PROP
        }
      }
      return(x)
    }

    # sex-specific matrix: females first then males => 2*nbins + 6
    gender_use <- 3L

    bins_use <- if (isTRUE(bin)) bins else sort(unique(lcomp$LENGTH))
    nbin <- length(bins_use)
    nyr  <- length(years)

    x <- matrix(ncol = (2 * nbin) + 6, nrow = nyr)
    x[, 2] <- seas
    x[, 3] <- flt
    x[, 4] <- gender_use
    x[, 5] <- part
    x[, 6] <- Nsamp

    for (i in seq_len(nyr)) {
      yr <- years[i]
      x[i, 1] <- yr

      if (isTRUE(bin)) {
        f <- lcomp[YEAR == yr & SEX == 2L][order(match(BIN, bins_use))]
        m <- lcomp[YEAR == yr & SEX == 1L][order(match(BIN, bins_use))]

        if (nrow(f) != nbin) {
          f <- merge(DT(data.frame(BIN = bins_use)), f, by = "BIN", all.x = TRUE)
          f[is.na(PROP), PROP := 0]
          f <- f[order(match(BIN, bins_use))]
        }
        if (nrow(m) != nbin) {
          m <- merge(DT(data.frame(BIN = bins_use)), m, by = "BIN", all.x = TRUE)
          m[is.na(PROP), PROP := 0]
          m <- m[order(match(BIN, bins_use))]
        }

        x[i, 7:(6 + nbin)] <- f$PROP
        x[i, (7 + nbin):(6 + 2 * nbin)] <- m$PROP
      } else {
        f <- lcomp[YEAR == yr & SEX == 2L][order(LENGTH)]
        m <- lcomp[YEAR == yr & SEX == 1L][order(LENGTH)]
        x[i, 7:(6 + nbin)] <- f$PROP
        x[i, (7 + nbin):(6 + 2 * nbin)] <- m$PROP
      }
    }

    return(x)
  }

  # ---- return long data if not SS ----
  lcomp[]
}