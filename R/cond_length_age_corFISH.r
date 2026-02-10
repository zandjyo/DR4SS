#' Conditional age-at-length compositions for fishery data (catch/age table)
#'
#' @description
#' Pulls domestic fishery age/length records via SQL, bins lengths, plus-groups ages,
#' and returns Stock Synthesis age-composition observations in conditional age-at-length form
#' (rows are Year x Length-bin; cells are age proportions). Sample sizes are weighted by `wt`.
#'
#' @param con DBI connection for AKFIN.
#' @param species Numeric species code(s) used in the SQL filter (e.g., fishery species code).
#' @param area Character region selector: one of \code{"AI"}, \code{"BS"}, \code{"GOA"}.
#' @param max_age1 Maximum age (plus group).
#' @param len_bins1 Numeric vector of length bin lower edges (SS bins). The final bin is treated as plus.
#' @param one_fleet Logical. If TRUE, all fishery data are assigned to fleet 1. If FALSE, HAL=2, POT=3, TRW/other=1.
#' @param wt Numeric multiplier applied to Nsamp (column 9).
#' @param seas Integer season for SS output (default 1 for fishery conditional age-at-length tables).
#' @param ageerr Integer age-error definition index for SS output (default 1).
#'
#' @return A list with element \code{$norm}, a data.frame in SS agecomp format with columns:
#' \code{Yr, Seas, Flt, Gender, Part, Ageerr, Lbin_lo, Lbin_hi, Nsamp, a0..aMax}.
#'
#' @export
cond_length_age_corFISH <- function(con,
                                    species,
                                    area,
                                    max_age1,
                                    len_bins1,
                                    one_fleet = TRUE,
                                    wt = 1,
                                    seas = 1,
                                    ageerr = 1) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required.", call. = FALSE)
  }

  # ---- validate inputs ----
  area <- toupper(as.character(area))
  if (!area %in% c("AI", "BS", "GOA", "BSWGOA")) {
    stop("`area` must be one of 'AI', 'BS', 'GOA' or 'BSWGOA'.", call. = FALSE)
  }
  if (!is.numeric(len_bins1) || length(len_bins1) < 2) {
    stop("`len_bins1` must be a numeric vector of length bins (length >= 2).", call. = FALSE)
  }
  len_bins1 <- sort(unique(len_bins1))

  # ---- map region clause for SQL ----
  region_clause <- switch(
    area,
    AI  = c(540:544),
    BS  = c(500:539),
    GOA = c(600:699),
    BSWGOA = c(500:539,610,620)
  )

  # ---- pull data ----
  sql_code <- sql_reader("dom_age.sql")
  sql_code <- sql_filter(sql_precode="IN",x= region_clause, sql_code = sql_code, flag = "-- insert region")
  sql_code <- sql_filter(sql_precode = "IN", x = species, sql_code = sql_code, flag = "-- insert species")

  dt <- sql_run(con, sql_code) %>%
    data.table::as.data.table() %>%
    dplyr::rename_all(toupper) %>%
    data.table::as.data.table()

  if (nrow(dt) == 0L) {
    # return empty SS-format table with correct columns
    age_cols <- paste0("a", 0:max_age1)
    out <- data.frame(
      Yr = integer(0), Seas = integer(0), Flt = integer(0),
      Gender = integer(0), Part = integer(0), Ageerr = integer(0),
      Lbin_lo = numeric(0), Lbin_hi = numeric(0), Nsamp = numeric(0),
      stringsAsFactors = FALSE
    )
    out[age_cols] <- numeric(0)
    return(list(norm = out))
  }

  # ---- harmonize / clean ----
  # plus-group age
  dt[, AGE_FILT := pmin(AGE, max_age1)]

  # remove missing/non-positive age/length as in original
  dt <- dt[!is.na(AGE) & !is.na(LENGTH) & LENGTH > 0 & AGE > 0]

  if (nrow(dt) == 0L) {
    age_cols <- paste0("a", 0:max_age1)
    out <- data.frame(
      Yr = integer(0), Seas = integer(0), Flt = integer(0),
      Gender = integer(0), Part = integer(0), Ageerr = integer(0),
      Lbin_lo = numeric(0), Lbin_hi = numeric(0), Nsamp = numeric(0),
      stringsAsFactors = FALSE
    )
    out[age_cols] <- numeric(0)
    return(list(norm = out))
  }

  # HAULJOIN logic (keep your rule: if HAUL_JOIN == "H" then PORT_JOIN else HAUL_JOIN)
  dt[, HAULJOIN := HAUL_JOIN]
  dt[HAUL_JOIN == "H", HAULJOIN := PORT_JOIN]

  # fleet mapping
  if (isTRUE(one_fleet)) {
    dt[, GEAR2 := 1L]
  } else {
    dt[, GEAR2 := 1L]
    dt[GEAR == "HAL", GEAR2 := 2L]
    dt[GEAR == "POT", GEAR2 := 3L]
  }

  # length binning (fast)
  # - treat last bin as plus bin (everything >= last threshold maps to last bin)
  # - for values below first bin, assign first bin
  idx <- findInterval(dt$LENGTH, vec = len_bins1, rightmost.closed = TRUE, all.inside = FALSE)
  idx[idx < 1L] <- 1L
  idx[idx > length(len_bins1)] <- length(len_bins1)
  dt[, BIN := len_bins1[idx]]

  # ---- build SS conditional age-at-length compositions by fleet ----
  fleets <- sort(unique(dt$GEAR2))
  age_levels <- 0:max_age1
  age_cols <- paste0("a", age_levels)

  out_list <- vector("list", length(fleets))

  for (k in seq_along(fleets)) {
    flt <- fleets[k]
    dtk <- dt[GEAR2 == flt]

    if (nrow(dtk) == 0L) next

    # counts by Year x BIN x AGE_FILT
    cnt <- dtk[, .N, by = .(YEAR, BIN, AGE_FILT)]

    # Nsamp by Year x BIN
    ns <- cnt[, .(Nsamp = sum(N)), by = .(YEAR, BIN)]

    # wide age counts by Year x BIN
    wide <- data.table::dcast(
      cnt,
      YEAR + BIN ~ AGE_FILT,
      value.var = "N",
      fill = 0
    )

    # ensure all age columns exist (0..max_age1)
    missing_ages <- setdiff(as.character(age_levels), names(wide))
    for (ma in missing_ages) wide[, (ma) := 0]

    # reorder age columns
    data.table::setcolorder(wide, c("YEAR", "BIN", as.character(age_levels)))

    # convert to proportions
    wide <- merge(wide, ns, by = c("YEAR", "BIN"), all.x = TRUE)
    for (a in as.character(age_levels)) {
      wide[, (a) := data.table::fifelse(Nsamp > 0, get(a) / Nsamp, 0)]
    }

    # apply wt to Nsamp
    wide[, Nsamp := Nsamp * wt]

    # SS table columns
    # Lbin_lo == Lbin_hi == BIN in your original
    ss <- data.table::data.table(
      Yr     = as.integer(wide$YEAR),
      Seas   = as.integer(seas),
      Flt    = as.integer(flt),
      Gender = 0L,
      Part   = 0L,
      Ageerr = as.integer(ageerr),
      Lbin_lo = as.numeric(wide$BIN),
      Lbin_hi = as.numeric(wide$BIN),
      Nsamp   = as.numeric(wide$Nsamp)
    )

    # attach age columns with SS names a0..aMax
    # note: your original starts at AGE>0, but SS wants the full vector; a0 will be zeros here.
    for (a in age_levels) {
      ss[, paste0("a", a) := as.numeric(wide[[as.character(a)]])]
    }

    data.table::setorder(ss, Yr, Lbin_lo)

    out_list[[k]] <- ss
  }

  out_dt <- data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE)
  out_df <- as.data.frame(out_dt)

  return(list(norm = out_df))
}
