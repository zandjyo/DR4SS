#' Format mean length-at-age or mean weight-at-age data for Stock Synthesis
#'
#' Computes annual mean length-at-age or mean weight-at-age from individual
#' age-sample observations and formats the results into the matrix layout
#' required by the Stock Synthesis (SS) data file.
#'
#' When \code{type = "L"}, the function calculates mean length-at-age (in cm);
#' when \code{type = "W"}, it calculates mean weight-at-age (in kg). For each
#' year, output includes both the mean value and the corresponding sample size
#' by age.
#'
#' This function retains legacy behavior used in historical AFSC workflows,
#' including: (1) filtering to \code{AGE1 > 0}, (2) filling missing age-year
#' combinations with zeros, and (3) producing the legacy SS matrix layout with
#' constant columns and a 999 terminator field.
#'
#' @param srv_age_samples A data frame containing individual age observations,
#'   including at minimum the columns \code{YEAR}, \code{AGE1}, and either
#'   \code{LENGTH} (for \code{type = "L"}) or \code{WEIGHT} (for \code{type = "W"}).
#'   Lengths are assumed to be in millimeters and weights in grams.
#' @param max_age Integer maximum age to include in the output (age bins will
#'   span \code{0:max_age}).
#' @param type Character string indicating output type:
#'   \code{"L"} for mean length-at-age or \code{"W"} for mean weight-at-age.
#' @param seas Integer season number to write to the SS data file.
#' @param flt Integer fleet index to write to the SS data file.
#' @param gender Integer gender code for SS (typically 0 = combined).
#' @param part Integer partition code for SS (typically 0 = combined).
#' @param birthseas Integer birth season (not currently used in the active code
#'   path; retained for legacy compatibility).
#' @param growpattern Integer growth pattern (not currently used in the active
#'   code path; retained for legacy compatibility).
#'
#' @return A numeric matrix formatted for direct inclusion in a Stock Synthesis
#'   data file. Each row corresponds to a year, and columns are ordered as:
#' \describe{
#'   \item{1}{Year}
#'   \item{2}{Season}
#'   \item{3}{Fleet}
#'   \item{4}{Gender}
#'   \item{5}{Partition}
#'   \item{6}{Data type indicator (1 = mean length-at-age, -1 = mean weight-at-age)}
#'   \item{7}{Unused / terminator column (set to 999)}
#'   \item{8..}{Mean values by age (ages 0 to \code{max_age})}
#'   \item{Remaining}{Sample sizes by age (ages 0 to \code{max_age})}
#' }
#'
#' @details
#' Records with missing or invalid length/weight values (\code{NA} or < 1) are
#' removed with a warning. Mean length-at-age values are converted from
#' millimeters to centimeters (divide by 10), and mean weight-at-age values are
#' converted from grams to kilograms (divide by 1000).
#'
#' Missing age–year combinations are filled with zeros in both mean values and
#' sample sizes to meet Stock Synthesis formatting requirements.
#'
#' @examples
#' \dontrun{
#' # Mean length-at-age
#' Lmat <- FORMAT_AGE_MEANS1(age_df, max_age = 12, type = "L", seas = 1, flt = 2)
#'
#' # Mean weight-at-age
#' Wmat <- FORMAT_AGE_MEANS1(age_df, max_age = 12, type = "W", seas = 1, flt = 2)
#' }
#'
#' @export
FORMAT_AGE_MEANS1 <- function(srv_age_samples = NULL,
                              max_age = 12,
                              type = "L",
                              seas = 1,
                              flt = 2,
                              gender = 0,
                              part = 0,
                              birthseas = 1,
                              growpattern = 1) {

  # Internal helper: shared logic for mean-at-age formatting (SS matrix layout)
  .format_age_means_core <- function(Age_All,
                                     max_age,
                                     value_col = c("LENGTH", "WEIGHT"),
                                     value_divisor,
                                     seas, flt, gender, part,
                                     type_indicator) {
    value_col <- match.arg(value_col)

    # print warning if there are any blank or negative values (legacy behavior)
    bad_vals <- max(
      dim(subset(Age_All, is.na(Age_All[[value_col]]) == TRUE))[1],
      dim(subset(Age_All, Age_All[[value_col]] < 1))[1]
    )

    if (bad_vals > 0) {
      # Keep legacy warning text exactly (even for weights) to avoid behavior diffs
      print(paste(
        "Warning in FORMAT_AGE_MEANS: ",
        bad_vals,
        "database records with bad length values"
      ))
      Age_All <- subset(Age_All, is.na(Age_All[[value_col]]) == FALSE)
    }

    # keep legacy AGE1 > 0 filter (same as original)
    Age_All <- subset(Age_All, Age_All$AGE1 > 0)

    # Compute mean values and sample sizes by (AGE1, YEAR)
    Ave_AgeM <- aggregate(
      list(VALUE = Age_All[[value_col]] / value_divisor),
      by = list(AGE = Age_All$AGE1, YEAR = Age_All$YEAR),
      FUN = mean
    )

    Ave_Age_M <- aggregate(
      list(SAMPLES = Age_All[[value_col]] / value_divisor),
      by = list(AGE = Age_All$AGE1, YEAR = Age_All$YEAR),
      FUN = length
    )

    # Attach the mean values to the sample-size frame (as in original)
    Ave_Age_M$VALUE <- Ave_AgeM$VALUE

    # Create complete AGE x YEAR grid (ages 0:max_age)
    AGE1 <- c(0:max_age)
    YEAR <- sort(as.numeric(unique(Age_All$YEAR)))
    AGE1 <- expand.grid(AGE1, YEAR)
    names(AGE1) <- c("AGE", "YEAR")

    # Merge to fill missing age-year combos, set missing to -9 then to 0 (legacy)
    Ave_AgeM <- merge(Ave_Age_M, AGE1, all = TRUE)
    Ave_AgeM$VALUE[is.na(Ave_AgeM$VALUE) == TRUE] <- -9
    Ave_AgeM$SAMPLES[is.na(Ave_AgeM$SAMPLES) == TRUE] <- -9

    # IMPORTANT: preserve legacy partial-match behavior: Ave_AgeM$SAMPLE -> SAMPLES
    AGE_FRAME <- data.frame(
      YEAR   = Ave_AgeM$YEAR,
      Age    = Ave_AgeM$AGE,
      Sample = Ave_AgeM$SAMPLE,
      VALUE  = Ave_AgeM$VALUE
    )
    AGE_FRAME <- AGE_FRAME[order(AGE_FRAME$YEAR, AGE_FRAME$Age), ]

    # set all "-9" values set above to 0
    AGE_FRAME$Sample[which(AGE_FRAME$Sample == -9, arr.ind = TRUE)] <- 0
    AGE_FRAME$VALUE[which(AGE_FRAME$VALUE == -9, arr.ind = TRUE)] <- 0

    years <- unique(AGE_FRAME$YEAR)
    bins  <- unique(AGE_FRAME$Age)

    nbin <- length(bins)
    nyr  <- length(years)

    x <- matrix(ncol = ((2 * nbin) + 7), nrow = nyr)

    x[, 2] <- seas
    x[, 3] <- flt
    x[, 4] <- gender
    x[, 5] <- part
    x[, 6] <- type_indicator
    x[, 7] <- 999

    for (i in 1:nyr) {
      x[i, 1] <- years[i]
      x[i, 8:(nbin + 7)] <- AGE_FRAME$VALUE[AGE_FRAME$YEAR == years[i]]
      x[i, (nbin + 8):ncol(x)] <- AGE_FRAME$Sample[AGE_FRAME$YEAR == years[i]]
    }

    x
  }

  Age_All <- subset(srv_age_samples, !is.na(srv_age_samples$AGE))

  if (toupper(type) == "L") {
    # LENGTH in mm -> cm, so divide by 10; SS type indicator = 1
    return(.format_age_means_core(
      Age_All = Age_All,
      max_age = max_age,
      value_col = "LENGTH",
      value_divisor = 10,
      seas = seas, flt = flt, gender = gender, part = part,
      type_indicator = 1
    ))
  }

  if (toupper(type) == "W") {
    # WEIGHT in g -> kg, so divide by 1000; SS type indicator = -1
    return(.format_age_means_core(
      Age_All = Age_All,
      max_age = max_age,
      value_col = "WEIGHT",
      value_divisor = 1000.0,
      seas = seas, flt = flt, gender = gender, part = part,
      type_indicator = -1
    ))
  }

  NULL
}
