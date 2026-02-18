#' Fishery conditional age-at-length (CAAL) compositions in Stock Synthesis format
#'
#' Builds conditional age-at-length compositions from fishery ageing data by
#' year, season, region, and (optionally) fleet. Ages are plus-grouped at
#' \code{max_age} and lengths are assigned to user-supplied bins \code{len_bins}.
#'
#' @details
#' The function pulls fishery age/length observations via
#' \code{get_fishery_age_wt_data()} and constructs conditional age-at-length
#' proportions within each Year × LengthBin (and strata defined by season/region/fleet).
#'
#' For each stratum, year, and length bin \eqn{l}, the output proportions are:
#' \deqn{p(a \mid l) = \frac{n_{a,l}}{\sum_a n_{a,l}}}
#' where \eqn{n_{a,l}} is the number of observations at age \eqn{a} in length bin \eqn{l}.
#' Ages are filtered to \code{AGE > 0}, so the \code{a0} column (age 0) will be zero.
#'
#' Length binning uses \code{findInterval()} against \code{len_bins}. Values below the
#' first bin are assigned to the first bin; values above the last bin are assigned to
#' the last bin (a plus bin).
#'
#' The returned table follows Stock Synthesis CAAL conventions with columns:
#' \code{Yr, Seas, Flt, Gender, Part, Ageerr, Lbin_lo, Lbin_hi, Nsamp, a0..aMax}.
#' In this implementation \code{Lbin_lo == Lbin_hi == BIN} (bin labels), consistent with
#' historical single-value bin conventions; if edge-based bins are desired, modify the
#' \code{Lbin_lo/Lbin_hi} construction accordingly.
#'
#' @param con_akfin A DBI connection to the AKFIN database.
#' @param species Species code passed to \code{get_fishery_age_wt_data()}.
#' @param start_year,end_year Inclusive year range.
#' @param season_def Named list mapping season labels to months (passed through to the data pull).
#' @param region_def Named list mapping region labels to AREA codes (passed through to the data pull).
#' @param max_age Integer maximum age for plus-grouping.
#' @param len_bins Numeric vector of length bin labels (must be length >= 2). Values are sorted/unique.
#' @param drop_unmapped Logical; drop observations whose AREA does not map into \code{region_def}.
#' @param one_fleet Logical; if TRUE, pool across gears into a single fleet (fleet = 1).
#' @param wgoa_cod Logical; optional WGOA recoding logic passed to the data pull.
#' @param wt Numeric scalar multiplier applied to \code{Nsamp}.
#' @param ageerr Integer age-error definition code written to the \code{Ageerr} column.
#'
#' @return A named list with element \code{norm}, a \code{data.frame} in Stock Synthesis
#' conditional age-at-length format. If no data are available, returns an empty table with
#' the correct columns.
#'
#' @seealso \code{\link[data.table]{dcast}}, \code{get_fishery_age_wt_data}
#'
#' @importFrom data.table as.data.table dcast fifelse rbindlist setcolorder setorder
#' @export
fishery_cond_age_length <- function(con_akfin,
                                    species,
                                    start_year,
                                    end_year,
                                    season_def,
                                    region_def,
                                    max_age,
                                    len_bins,
                                    drop_unmapped = TRUE,
                                    one_fleet = TRUE,
                                    wgoa_cod=TRUE,
                                    wt = 1,
                                    ageerr = 1) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  

  # ---- validate inputs ----
 if (!is.numeric(len_bins) || length(len_bins) < 2) {
    stop("`len_bins` must be a numeric vector of length bins (length >= 2).", call. = FALSE)
  }
  len_bins <- sort(unique(len_bins))
  
  dt<-get_fishery_age_wt_data(con=con_akfin,
                              species = species,
                              season_def = season_def,
                              region_def = region_def,
                              start_year = start_year,
                              end_year = end_year,
                              wgoa_cod = wgoa_cod,
                              max_wt = 5000,
                              drop_unmapped = drop_unmapped)

  if (nrow(dt) == 0L) {
    # return empty SS-format table with correct columns
    age_cols <- paste0("a", 0:max_age)
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
  dt[, AGE_FILT := pmin(AGE, max_age)]

  # remove missing/non-positive age/length as in original
  dt <- dt[!is.na(AGE) & !is.na(LENGTH) & LENGTH > 0 & AGE > 0]

  if (nrow(dt) == 0L) {
    age_cols <- paste0("a", 0:max_age)
    out <- data.frame(
      Yr = integer(0), Seas = integer(0), Flt = integer(0),
      Gender = integer(0), Part = integer(0), Ageerr = integer(0),
      Lbin_lo = numeric(0), Lbin_hi = numeric(0), Nsamp = numeric(0),
      stringsAsFactors = FALSE
    )
    out[age_cols] <- numeric(0)
    return(list(norm = out))
  }


  # fleet mapping
  if (isTRUE(one_fleet)) {
    dt[, GEAR4 := 1L]
  } else {
    dt[, GEAR4 := GEAR3]
  }

  # length binning (fast)
  # - treat last bin as plus bin (everything >= last threshold maps to last bin)
  # - for values below first bin, assign first bin
  idx <- findInterval(dt$LENGTH, vec = len_bins, rightmost.closed = TRUE, all.inside = FALSE)
  idx[idx < 1L] <- 1L
  idx[idx > length(len_bins)] <- length(len_bins)
  dt[, BIN := len_bins[idx]]

  # ---- build SS conditional age-at-length compositions by fleet ----
  fleets <- sort(unique(dt$GEAR4))
  seasons<-sort(unique(dt$SEASON))
  regions<-sort(unique(dt$REGION_GRP))
  
  age_levels <- 0:max_age
  age_cols <- paste0("a", age_levels)

  out_list <- vector("list", length(fleets)*length(seasons)*length(regions))

  ls1<-1

  for (fl in seq_along(fleets)) {
    for(sea in seq_along(seasons)){
      for(reg in seq_along(regions)){

    
        flt <- fleets[fl]
        sea <- seasons[sea]
        reg <- regions[reg]
    
        dtk <- dt[GEAR4 == flt&SEASON==sea & REGION_GRP==reg]

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

    # ensure all age columns exist (0..max_age)
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
          Seas   = sea,
          Flt    = paste(reg,flt,sep="_"),
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

      out_list[[ls1]] <- ss
       ls1<-ls1+1
      }
    }
    
  }

  out_dt <- data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE)
  out_df <- as.data.frame(out_dt)

  return(list(norm = out_df))
}
