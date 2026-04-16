#' Calculate station-level abundance at age from CPUE and ALK
#'
#' Estimates abundance-at-age (numbers per km^2) at the station level
#' by combining station CPUE, length-frequency data, and an
#' age-length key (ALK).
#'
#' The method partitions total station CPUE into sex-specific and
#' length-specific components and then applies the conditional
#' probabilities from the ALK to derive abundance at age.
#'
#' @param cpue_len_df A data.frame or data.table containing CPUE and
#'   length-frequency data with required columns:
#'   \code{YEAR}, \code{REGION}, \code{HAULJOIN}, \code{CPUE_NOKM2},
#'   \code{SPECIES_CODE}, \code{SEX}, \code{LENGTH}, \code{FREQUENCY},
#'   \code{MID_LAT}, and \code{MID_LON}.
#'
#' @param alk_df A data.frame or data.table containing an age-length key
#'   with columns:
#'   \code{YEAR}, \code{REGION}, \code{SPECIES_CODE}, \code{SEX},
#'   \code{LENGTH}, \code{AGE}, and \code{ALK_PROP}.
#'
#' @param plus_age Optional numeric (default = \code{NULL}).
#'   If provided, all ages greater than or equal to \code{plus_age}
#'   are collapsed into a plus group. The plus group is applied at both
#'   the ALK level and the final output.
#'
#' @param drop_unsexed Logical (default = \code{TRUE}).
#'   If \code{TRUE}, removes observations with unsexed fish from both
#'   the CPUE/length data and the ALK prior to calculation.
#'
#' @return A data.frame containing station-level abundance-at-age with:
#' \describe{
#'   \item{YEAR}{Integer survey year.}
#'   \item{REGION}{Survey region.}
#'   \item{HAULJOIN}{Station (haul) identifier.}
#'   \item{SPECIES_CODE}{Species code.}
#'   \item{SEX}{Sex category.}
#'   \item{AGE}{Age or plus group (if specified).}
#'   \item{ABUNDANCE_AT_AGE}{Estimated abundance (numbers per km^2).}
#'   \item{MID_LAT}{Midpoint latitude of haul.}
#'   \item{MID_LON}{Midpoint longitude of haul.}
#' }
#'
#' @details
#' The calculation proceeds as follows:
#' \enumerate{
#'   \item Station-level CPUE is partitioned among sexes based on
#'         observed length-frequency counts.
#'   \item Within each sex, CPUE is partitioned across length bins
#'         using observed length proportions.
#'   \item The age-length key (\eqn{P(\text{AGE} \mid \text{LENGTH})})
#'         is applied to derive abundance at age.
#'   \item Abundance is summed across length bins to obtain total
#'         abundance at age for each station.
#' }
#'
#' If a plus group is specified, ages are collapsed prior to final
#' aggregation and reported as character values (e.g., \code{"10+"}).
#'
#' The function assumes that:
#' \itemize{
#'   \item CPUE is constant within each station-year-species group.
#'   \item Length-frequency data are representative of the station.
#'   \item The ALK is appropriate for the corresponding YEAR, REGION,
#'         SPECIES_CODE, and SEX.
#' }
#'
#' @export

calc_station_age_abundance <- function(cpue_len_df,
                                       alk_df,
                                       plus_age = NULL,
                                       drop_unsexed = TRUE) {

  # ------------------------------------------------------------
  # REQUIRED COLUMNS
  # ------------------------------------------------------------
  req1 <- c("YEAR","REGION","STATIONID","HAULJOIN","CPUE_NOKM2",
            "SPECIES_CODE","SEX","LENGTH","FREQUENCY",
            "MID_LAT","MID_LON")

  req2 <- c("YEAR","REGION","SPECIES_CODE","SEX",
            "LENGTH","AGE","ALK_PROP")

  if (!all(req1 %in% names(cpue_len_df))) {
    stop("cpue_len_df missing: ",
         paste(setdiff(req1, names(cpue_len_df)), collapse=", "))
  }

  if (!all(req2 %in% names(alk_df))) {
    stop("alk_df missing: ",
         paste(setdiff(req2, names(alk_df)), collapse=", "))
  }

  x <- cpue_len_df
  k <- alk_df

  # ------------------------------------------------------------
  # CLEAN TYPES
  # ------------------------------------------------------------
  x$YEAR        <- as.integer(x$YEAR)
  x$CPUE_NOKM2  <- as.numeric(x$CPUE_NOKM2)
  x$LENGTH      <- as.numeric(x$LENGTH)
  x$FREQUENCY   <- as.numeric(x$FREQUENCY)

  k$YEAR        <- as.integer(k$YEAR)
  k$LENGTH      <- as.numeric(k$LENGTH)
  k$AGE         <- as.numeric(k$AGE)
  k$ALK_PROP    <- as.numeric(k$ALK_PROP)

  # ------------------------------------------------------------
  # DROP UNSEXED IF REQUESTED
  # ------------------------------------------------------------
  if (drop_unsexed) {
    x <- x[!(x$SEX %in% c("U","UNSEXED",3,"3")), ]
    k <- k[!(k$SEX %in% c("U","UNSEXED",3,"3")), ]
  }

  # ------------------------------------------------------------
  # OPTIONAL PLUS GROUP (ALK LEVEL)
  # ------------------------------------------------------------
  if (!is.null(plus_age)) {
    k$AGE <- ifelse(k$AGE >= plus_age, plus_age, k$AGE)

    k <- aggregate(
      ALK_PROP ~ YEAR + REGION + SPECIES_CODE + SEX + LENGTH + AGE,
      data = k,
      sum
    )
  }

  # ------------------------------------------------------------
  # ENSURE CPUE IS UNIQUE PER STATION
  # ------------------------------------------------------------
  cpue_check <- aggregate(
    CPUE_NOKM2 ~ YEAR + REGION + HAULJOIN + SPECIES_CODE,
    data = x,
    function(z) length(unique(z))
  )

  if (any(cpue_check$CPUE_NOKM2 > 1)) {
    stop("CPUE_NOKM2 is not unique within station-year groups")
  }

  station_cpue <- unique(
    x[, c("YEAR","REGION","HAULJOIN","SPECIES_CODE",
          "CPUE_NOKM2","MID_LAT","MID_LON")]
  )

  # ------------------------------------------------------------
  # LENGTH FREQUENCIES BY SEX
  # ------------------------------------------------------------
  freq <- aggregate(
    FREQUENCY ~ YEAR + REGION + HAULJOIN +
      SPECIES_CODE + SEX + LENGTH,
    data = x, sum
  )

  sex_tot <- aggregate(
    FREQUENCY ~ YEAR + REGION + HAULJOIN +
      SPECIES_CODE + SEX,
    data = freq, sum
  )
  names(sex_tot)[6] <- "SEX_FREQ"

  station_tot <- aggregate(
    SEX_FREQ ~ YEAR + REGION + HAULJOIN + SPECIES_CODE,
    data = sex_tot, sum
  )
  names(station_tot)[5] <- "TOT_FREQ"

  sex_tot <- merge(sex_tot, station_tot)
  sex_tot$SEX_PROP <- sex_tot$SEX_FREQ / sex_tot$TOT_FREQ

  sex_tot <- merge(sex_tot, station_cpue)
  sex_tot$SEX_CPUE <- sex_tot$CPUE_NOKM2 * sex_tot$SEX_PROP

  # ------------------------------------------------------------
  # LENGTH PROPORTIONS WITHIN SEX
  # ------------------------------------------------------------
  len_tot <- aggregate(
    FREQUENCY ~ YEAR + REGION + HAULJOIN +
      SPECIES_CODE + SEX,
    data = freq, sum
  )
  names(len_tot)[6] <- "LEN_TOTAL"

  freq <- merge(freq, len_tot)
  freq$LEN_PROP <- freq$FREQUENCY / freq$LEN_TOTAL

  # attach CPUE by sex
  freq <- merge(
    freq,
    sex_tot[,c("YEAR","REGION","HAULJOIN","SPECIES_CODE","SEX","SEX_CPUE")]
  )

  freq$N_LENGTH <- freq$SEX_CPUE * freq$LEN_PROP

  # ------------------------------------------------------------
  # APPLY ALK
  # ------------------------------------------------------------
  dat2 <- merge(
    freq,
    k,
    by = c("YEAR","REGION","SPECIES_CODE","SEX","LENGTH"),
    all.x = TRUE
  )

  if (any(is.na(dat2$ALK_PROP))) {
    stop("Missing ALK values for some YEAR-REGION-SEX-LENGTH combinations")
  }

  dat2$ABUNDANCE_AT_AGE <- dat2$N_LENGTH * dat2$ALK_PROP

  # ------------------------------------------------------------
  # SUM TO AGE
  # ------------------------------------------------------------
  out <- aggregate(
    ABUNDANCE_AT_AGE ~ YEAR + REGION + HAULJOIN +
      SPECIES_CODE + SEX + AGE,
    data = dat2, sum
  )

  coords <- unique(
    x[,c("YEAR","REGION","HAULJOIN","SPECIES_CODE",
         "MID_LAT","MID_LON")]
  )

  out <- merge(out, coords)

  # ------------------------------------------------------------
  # FINAL PLUS GROUP LABEL
  # ------------------------------------------------------------
  if (!is.null(plus_age)) {
    out$AGE <- ifelse(out$AGE >= plus_age,
                      paste0(plus_age,"+"),
                      as.character(out$AGE))

    out <- aggregate(
      ABUNDANCE_AT_AGE ~ YEAR + REGION + HAULJOIN +
        SPECIES_CODE + SEX + AGE + MID_LAT + MID_LON,
      data = out, sum
    )
  }

  out <- out[order(out$YEAR, out$REGION, out$HAULJOIN,
                   out$SPECIES_CODE, out$SEX, out$AGE), ]

  rownames(out) <- NULL
  return(out)
}