#' Calculate haul-level abundance at age from CPUE and ALK(s)
#'
#' Estimates abundance-at-age (numbers per km^2) at the haul level by
#' combining haul CPUE, length-frequency data, and either a single
#' age-length key (ALK) or a hierarchy of fallback ALKs.
#'
#' The function partitions total haul CPUE into sex-specific and
#' length-specific components and then applies the conditional
#' probabilities from the ALK to derive abundance at age.
#'
#' @param cpue_len_df A data.frame or data.table containing CPUE and
#'   length-frequency data with required columns:
#'   \code{YEAR}, \code{REGION}, \code{HAULJOIN}, \code{CPUE_NOKM2},
#'   \code{SPECIES_CODE}, \code{SEX}, \code{LENGTH}, \code{FREQUENCY},
#'   \code{MID_LAT}, and \code{MID_LON}.
#'
#' @param alk_df Either:
#'   \itemize{
#'     \item A single ALK data.frame/data.table with columns
#'       \code{YEAR}, \code{REGION}, \code{SPECIES_CODE}, \code{SEX},
#'       \code{LENGTH}, \code{AGE}, and \code{ALK_PROP}, or
#'     \item A named list of fallback ALKs as returned by
#'       \code{build_alk_hierarchy()}, containing:
#'       \code{alk_y_r_s}, \code{alk_r_s}, \code{alk_s}, and
#'       \code{alk_all}.
#'   }
#'
#' @param plus_age Optional numeric (default = \code{NULL}).
#'   If provided, all ages greater than or equal to \code{plus_age}
#'   are collapsed into a plus group.
#'
#' @param drop_unsexed Logical (default = \code{TRUE}).
#'   If \code{TRUE}, removes unsexed fish from both the CPUE/length data
#'   and the ALK(s) before calculation.
#'
#' @param length_bins Optional numeric vector (default = \code{NULL}).
#'   If provided, CPUE/length data are binned using these breakpoints
#'   before applying the ALK. These bins should match those used in
#'   \code{build_alk()} or \code{build_alk_hierarchy()}.
#'
#' @param return_diagnostics Logical (default = \code{FALSE}).
#'   If \code{TRUE}, returns a list containing the abundance-at-age
#'   output, ALK usage summary, and joined haul-length-at-age data.
#'
#' @return By default, a data.frame containing haul-level abundance-at-age:
#' \describe{
#'   \item{YEAR}{Integer survey year.}
#'   \item{REGION}{Survey region.}
#'   \item{HAULJOIN}{Haul identifier.}
#'   \item{SPECIES_CODE}{Species code.}
#'   \item{SEX}{Sex category.}
#'   \item{AGE}{Age or plus group (if specified).}
#'   \item{ABUNDANCE_AT_AGE}{Estimated abundance (numbers per km^2).}
#'   \item{MID_LAT}{Midpoint latitude of haul.}
#'   \item{MID_LON}{Midpoint longitude of haul.}
#' }
#'
#'   If \code{return_diagnostics = TRUE}, returns a list with:
#' \describe{
#'   \item{abundance_at_age}{Final haul-level abundance-at-age output.}
#'   \item{alk_usage}{Summary of haul-length abundance matched to each
#'     ALK level.}
#'   \item{joined_data}{Expanded haul-length-age table after ALK
#'     application.}
#' }
#'
#' @details
#' The calculation proceeds as follows:
#' \enumerate{
#'   \item Total haul CPUE is partitioned among sexes based on observed
#'         length-frequency counts.
#'   \item Within each sex, CPUE is partitioned across length bins using
#'         observed length proportions.
#'   \item A single ALK or fallback ALK hierarchy is applied to derive
#'         abundance at age.
#'   \item Abundance is summed across length bins to obtain total
#'         abundance at age for each haul.
#' }
#'
#' If a hierarchy of ALKs is supplied, matching proceeds from most
#' specific to least specific:
#' \enumerate{
#'   \item YEAR + REGION + SPECIES_CODE + SEX + LENGTH
#'   \item REGION + SPECIES_CODE + SEX + LENGTH
#'   \item SPECIES_CODE + SEX + LENGTH
#'   \item SPECIES_CODE + LENGTH
#' }
#'
#' @export
calc_station_age_abundance <- function(cpue_len_df,
                                       alk_df,
                                       plus_age = NULL,
                                       drop_unsexed = TRUE,
                                       length_bins = NULL,
                                       return_diagnostics = FALSE) {

  req1 <- c("YEAR", "REGION", "HAULJOIN", "CPUE_NOKM2",
            "SPECIES_CODE", "SEX", "LENGTH", "FREQUENCY",
            "MID_LAT", "MID_LON")

  if (!all(req1 %in% names(cpue_len_df))) {
    stop("cpue_len_df missing: ",
         paste(setdiff(req1, names(cpue_len_df)), collapse = ", "))
  }

  x <- cpue_len_df

  # ---- clean types ----
  x$YEAR        <- as.integer(x$YEAR)
  x$CPUE_NOKM2  <- as.numeric(x$CPUE_NOKM2)
  x$LENGTH      <- as.numeric(x$LENGTH)
  x$FREQUENCY   <- as.numeric(x$FREQUENCY)
  x$MID_LAT     <- as.numeric(x$MID_LAT)
  x$MID_LON     <- as.numeric(x$MID_LON)

  if (drop_unsexed) {
    x <- x[!(x$SEX %in% c("U", "UNSEXED", 3, "3")), , drop = FALSE]
  }

  if (!is.null(length_bins)) {
    x$LENGTH <- bin_length_values(x$LENGTH, length_bins = length_bins)
    x <- x[!is.na(x$LENGTH), , drop = FALSE]
  }

  if (!is.null(plus_age)) {
    plus_age <- as.numeric(plus_age)
    if (length(plus_age) != 1 || is.na(plus_age) || plus_age < 0) {
      stop("plus_age must be NULL or a single non-negative numeric value.")
    }
  }

  # ---- ensure CPUE is unique per haul ----
  cpue_check <- aggregate(
    CPUE_NOKM2 ~ YEAR + REGION + HAULJOIN + SPECIES_CODE,
    data = x,
    FUN = function(z) length(unique(z))
  )

  if (any(cpue_check$CPUE_NOKM2 > 1)) {
    stop("CPUE_NOKM2 is not unique within haul-year-species groups")
  }

  haul_cpue <- unique(
    x[, c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE",
          "CPUE_NOKM2", "MID_LAT", "MID_LON")]
  )

  # ---- summarize haul sex-length frequencies ----
  freq <- aggregate(
    FREQUENCY ~ YEAR + REGION + HAULJOIN + SPECIES_CODE + SEX + LENGTH,
    data = x,
    FUN = sum
  )

  # ---- total frequency by haul-sex ----
  sex_tot <- aggregate(
    FREQUENCY ~ YEAR + REGION + HAULJOIN + SPECIES_CODE + SEX,
    data = freq,
    FUN = sum
  )
  names(sex_tot)[names(sex_tot) == "FREQUENCY"] <- "SEX_FREQ"

  # ---- total frequency by haul ----
  haul_tot <- aggregate(
    SEX_FREQ ~ YEAR + REGION + HAULJOIN + SPECIES_CODE,
    data = sex_tot,
    FUN = sum
  )
  names(haul_tot)[names(haul_tot) == "SEX_FREQ"] <- "TOT_FREQ"

  sex_tot <- merge(
    sex_tot,
    haul_tot,
    by = c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE"),
    all.x = TRUE
  )

  sex_tot$SEX_PROP <- sex_tot$SEX_FREQ / sex_tot$TOT_FREQ

  sex_tot <- merge(
    sex_tot,
    haul_cpue,
    by = c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE"),
    all.x = TRUE
  )

  sex_tot$SEX_CPUE <- sex_tot$CPUE_NOKM2 * sex_tot$SEX_PROP

  # ---- length proportions within haul-sex ----
  len_tot <- aggregate(
    FREQUENCY ~ YEAR + REGION + HAULJOIN + SPECIES_CODE + SEX,
    data = freq,
    FUN = sum
  )
  names(len_tot)[names(len_tot) == "FREQUENCY"] <- "LEN_TOTAL"

  freq <- merge(
    freq,
    len_tot,
    by = c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE", "SEX"),
    all.x = TRUE
  )

  freq$LEN_PROP <- freq$FREQUENCY / freq$LEN_TOTAL

  # ---- attach sex-specific CPUE ----
  freq <- merge(
    freq,
    sex_tot[, c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE", "SEX", "SEX_CPUE")],
    by = c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE", "SEX"),
    all.x = TRUE
  )

  freq$N_LENGTH <- freq$SEX_CPUE * freq$LEN_PROP

  # ---- internal helper to apply ALK hierarchy ----
  apply_alk_hierarchy_internal <- function(freq_df, alk_list) {

    req_freq <- c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE", "SEX", "LENGTH", "N_LENGTH")
    if (!all(req_freq %in% names(freq_df))) {
      stop("freq_df is missing required columns: ",
           paste(setdiff(req_freq, names(freq_df)), collapse = ", "))
    }

    req_alk <- c("alk_y_r_s", "alk_r_s", "alk_s", "alk_all")
    if (!all(req_alk %in% names(alk_list))) {
      stop("alk_list must contain: ", paste(req_alk, collapse = ", "))
    }

    key_cols <- c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE", "SEX", "LENGTH")

    get_unmatched <- function(all_df, matched_keys) {
      if (nrow(matched_keys) == 0) return(all_df)
      idx_all <- do.call(paste, c(all_df[, key_cols, drop = FALSE], sep = "\r"))
      idx_use <- do.call(paste, c(matched_keys[, key_cols, drop = FALSE], sep = "\r"))
      all_df[!idx_all %in% idx_use, , drop = FALSE]
    }

    matched <- list()
    remaining <- freq_df

    # level 1
    tmp <- merge(
      remaining,
      alk_list$alk_y_r_s,
      by = c("YEAR", "REGION", "SPECIES_CODE", "SEX", "LENGTH"),
      all = FALSE
    )
    if (nrow(tmp) > 0) {
      tmp$ALK_LEVEL <- "alk_y_r_s"
      matched[[length(matched) + 1]] <- tmp
      remaining <- get_unmatched(remaining, unique(tmp[, key_cols, drop = FALSE]))
    }

    # level 2
    if (nrow(remaining) > 0) {
      tmp <- merge(
        remaining,
        alk_list$alk_r_s,
        by = c("REGION", "SPECIES_CODE", "SEX", "LENGTH"),
        all = FALSE
      )
      if (nrow(tmp) > 0) {
        tmp$ALK_LEVEL <- "alk_r_s"
        matched[[length(matched) + 1]] <- tmp
        remaining <- get_unmatched(remaining, unique(tmp[, key_cols, drop = FALSE]))
      }
    }

    # level 3
    if (nrow(remaining) > 0) {
      tmp <- merge(
        remaining,
        alk_list$alk_s,
        by = c("SPECIES_CODE", "SEX", "LENGTH"),
        all = FALSE
      )
      if (nrow(tmp) > 0) {
        tmp$ALK_LEVEL <- "alk_s"
        matched[[length(matched) + 1]] <- tmp
        remaining <- get_unmatched(remaining, unique(tmp[, key_cols, drop = FALSE]))
      }
    }

    # level 4
    if (nrow(remaining) > 0) {
      tmp <- merge(
        remaining,
        alk_list$alk_all,
        by = c("SPECIES_CODE", "LENGTH"),
        all = FALSE
      )
      if (nrow(tmp) > 0) {
        tmp$ALK_LEVEL <- "alk_all"
        matched[[length(matched) + 1]] <- tmp
        remaining <- get_unmatched(remaining, unique(tmp[, key_cols, drop = FALSE]))
      }
    }

    if (length(matched) == 0) {
      stop("No rows matched any ALK level.")
    }

    dat2 <- do.call(rbind, matched)

    if (nrow(remaining) > 0) {
      warning("Some haul-sex-length rows still had no ALK match after fallback.")
    }

    dat2
  }

  # ---- single ALK vs hierarchy ----
  is_hierarchy <- is.list(alk_df) && !is.data.frame(alk_df)

  if (is_hierarchy) {

    req_names <- c("alk_y_r_s", "alk_r_s", "alk_s", "alk_all")
    if (!all(req_names %in% names(alk_df))) {
      stop("ALK hierarchy list must contain: ",
           paste(req_names, collapse = ", "))
    }

    # clean hierarchy inputs
    clean_one_alk <- function(k) {
      k$YEAR     <- if ("YEAR" %in% names(k)) as.integer(k$YEAR) else k$YEAR
      k$LENGTH   <- as.numeric(k$LENGTH)
      k$AGE      <- as.numeric(k$AGE)
      k$ALK_PROP <- as.numeric(k$ALK_PROP)

      if (drop_unsexed && "SEX" %in% names(k)) {
        k <- k[!(k$SEX %in% c("U", "UNSEXED", 3, "3")), , drop = FALSE]
      }

      if (!is.null(plus_age)) {
        k$AGE <- ifelse(k$AGE >= plus_age, plus_age, k$AGE)

        group_cols <- intersect(
          c("YEAR", "REGION", "SPECIES_CODE", "SEX", "LENGTH", "AGE"),
          names(k)
        )

        k <- aggregate(
          k$ALK_PROP,
          by = k[group_cols],
          FUN = sum
        )
        names(k)[names(k) == "x"] <- "ALK_PROP"
      }

      k
    }

    alk_list <- lapply(alk_df, clean_one_alk)

    dat2 <- apply_alk_hierarchy_internal(
      freq_df = freq,
      alk_list = alk_list
    )

  } else {

    req2 <- c("YEAR", "REGION", "SPECIES_CODE", "SEX",
              "LENGTH", "AGE", "ALK_PROP")

    if (!all(req2 %in% names(alk_df))) {
      stop("Single alk_df is missing required columns: ",
           paste(setdiff(req2, names(alk_df)), collapse = ", "))
    }

    k <- alk_df
    k$YEAR     <- as.integer(k$YEAR)
    k$LENGTH   <- as.numeric(k$LENGTH)
    k$AGE      <- as.numeric(k$AGE)
    k$ALK_PROP <- as.numeric(k$ALK_PROP)

    if (drop_unsexed) {
      k <- k[!(k$SEX %in% c("U", "UNSEXED", 3, "3")), , drop = FALSE]
    }

    if (!is.null(plus_age)) {
      k$AGE <- ifelse(k$AGE >= plus_age, plus_age, k$AGE)
      k <- aggregate(
        ALK_PROP ~ YEAR + REGION + SPECIES_CODE + SEX + LENGTH + AGE,
        data = k,
        FUN = sum
      )
    }

    dat2 <- merge(
      freq,
      k,
      by = c("YEAR", "REGION", "SPECIES_CODE", "SEX", "LENGTH"),
      all.x = TRUE
    )

    if (any(is.na(dat2$ALK_PROP))) {
      stop("Missing ALK values for some YEAR-REGION-SEX-LENGTH combinations")
    }

    dat2$ALK_LEVEL <- "alk_single"
  }

  # ---- abundance at age ----
  dat2$ABUNDANCE_AT_AGE <- dat2$N_LENGTH * dat2$ALK_PROP

  alk_usage <- aggregate(
    N_LENGTH ~ ALK_LEVEL,
    data = dat2,
    FUN = sum
  )
  names(alk_usage)[names(alk_usage) == "N_LENGTH"] <- "TOTAL_N_LENGTH"

  # ---- sum to haul-age ----
  out <- aggregate(
    ABUNDANCE_AT_AGE ~ YEAR + REGION + HAULJOIN + SPECIES_CODE + SEX + AGE,
    data = dat2,
    FUN = sum
  )

  coords <- unique(
    x[, c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE", "MID_LAT", "MID_LON")]
  )

  out <- merge(
    out,
    coords,
    by = c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE"),
    all.x = TRUE
  )

  # ---- final plus group label ----
  if (!is.null(plus_age)) {
    out$AGE <- ifelse(out$AGE >= plus_age,
                      paste0(as.integer(plus_age)),
                      as.character(out$AGE))

    out <- aggregate(
      ABUNDANCE_AT_AGE ~ YEAR + REGION + HAULJOIN + SPECIES_CODE +
        SEX + AGE + MID_LAT + MID_LON,
      data = out,
      FUN = sum
    )
  }

  out <- out[order(out$YEAR, out$REGION, out$HAULJOIN,
                   out$SPECIES_CODE, out$SEX, out$AGE), ]

  rownames(out) <- NULL

  if (return_diagnostics) {
    return(list(
      abundance_at_age = out,
      alk_usage = alk_usage,
      joined_data = dat2
    ))
  }

  out
}