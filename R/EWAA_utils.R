# EWAA_utils.R
# Utilities for Empirical Weight-at-Age (EWAA) data workflows
#
# This file contains helper functions to:
#  - read and screen raw weight-at-age data,
#  - reshape long-format data to Stock Synthesis-style wide matrices,
#  - optionally interpolate/extrapolate missing values,
#  - generate diagnostic heatmap-style figures with values or sample sizes,
#  - write Stock Synthesis wtatage input files (including maturity*fecundity rows).
#
# NOTE: These utilities assume "ages" are integer years and that a plus-group is
#       defined by maxage (all ages >= maxage are binned to maxage).
#
# Suggested Imports (package-level):
#  - dplyr, tidyr, ggplot2, grDevices, cli, glue, cowplot
#
# Author: Adapted and cleaned up from EWAA_TOOLS.r

#' Validate long-format EWAA input
#' @keywords internal
.ewaa_validate_long <- function(dat) {
  req <- c("Source","Weight_kg","Age_yrs","Length_cm","Month","Year")
  miss <- setdiff(req, names(dat))
  if (length(miss) > 0) {
    stop("EWAA long data is missing required columns: ", paste(miss, collapse = ", "))
  }
  dat
}

#' Identify age columns in a wide EWAA matrix
#' @keywords internal
.ewaa_agecols <- function(wide, maxage) {
  grep(paste0("^a(", paste0(0:maxage, collapse="|"), ")$"), names(wide))
}

#' Turn a long data frame of weight-at-age data into a wide data frame by age
#'
#' Take long-format (raw) weight-at-age data and create a Stock Synthesis-style
#' wide matrix with columns a0..amaxage and the required SS metadata columns.
#'
#' @param dat A data frame created from [weight_at_age_read()] or one that has the
#'   following column names: Source, Weight_kg, Age_yrs, Length_cm, Month, Year.
#' @param value A character value specifying which type of data you are
#'   interested in. The options include `"weight"`, `"length"`, and `"count"`,
#'   where the latter returns sample sizes.
#' @param maxage The age of the plus-group bin. Default is 10.
#' @param fleet_id Integer fleet code to attach to all rows (typical usage when
#'   writing a single aggregated series). Default is 1.
#' @param seas,gender,GP,bseas Stock Synthesis metadata fields (defaults 1).
#'
#' @return A data frame with columns `#Yr, seas, gender, GP, bseas, fleet, a0..amaxage`.
#'   Ages with no samples are `NA`. If `value="count"`, values are sample sizes.
#' @export
weight_at_age_wide <- function(dat,
                               value = c("weight", "length", "count"),
                               maxage = 10,
                               fleet_id = 1,
                               seas = 1,
                               gender = 1,
                               GP = 1,
                               bseas = 1) {

  dat <- .ewaa_validate_long(dat)
  value <- match.arg(value)
  # Make Year stable (handles factor, character, numeric)
if ("Year" %in% names(dat)) {
  if (is.factor(dat$Year)) dat$Year <- as.character(dat$Year)
  # if it's numeric-like text, standardize to integer
  suppressWarnings({
    y_num <- as.integer(dat$Year)
    if (!all(is.na(y_num))) dat$Year <- y_num
  })
}

  # Coerce age to integer-ish where possible (SS bins are integer ages)
  dat$Age_yrs <- suppressWarnings(as.integer(round(dat$Age_yrs)))
  dat$Year    <- suppressWarnings(as.integer(dat$Year))

  # Ensure at least one age-0 column exists (common SS expectation)
  if (min(dat$Age_yrs, na.rm = TRUE) > 0) {

  # start with a 1-row template with ALL columns in dat
  add_0 <- as.data.frame(lapply(dat, function(x) NA))[1, , drop = FALSE]

  # then fill only the fields we know about (if they exist)
  add_0$Source    <- "Fishery"
  add_0$Weight_kg <- 0.012
  add_0$Sex       <- "M"
  add_0$Age_yrs   <- 0
  add_0$Length_cm <- 15
  add_0$Month     <- 1
  add_0$Year      <- min(as.integer(as.character(dat$Year)), na.rm = TRUE)

  # bind in a way that preserves all columns
  dat <- data.table::rbindlist(list(data.table::as.data.table(dat),
                                   data.table::as.data.table(add_0)),
                               use.names = TRUE, fill = TRUE)
}

  dat_filtered <- dat |>
    dplyr::mutate(
      fleet = as.integer(fleet_id),
      Age_yrs = ifelse(Age_yrs <= maxage, Age_yrs, maxage)
    ) |>
    dplyr::filter(!is.na(Weight_kg), !is.na(Age_yrs), !is.na(Year)) |>
    dplyr::group_by(Age_yrs, Year, fleet)

  dat_grouped <- switch(
    value,
    weight = dplyr::summarise(dat_filtered, to_summarize = mean(Weight_kg, na.rm = TRUE), .groups = "drop"),
    length = dplyr::summarise(dat_filtered, to_summarize = mean(Length_cm, na.rm = TRUE), .groups = "drop"),
    count  = dplyr::summarise(dat_filtered, to_summarize = dplyr::n(), .groups = "drop")
  )

  out <- dat_grouped |>
    tidyr::pivot_wider(
      values_from = to_summarize,
      names_from = Age_yrs,
      names_prefix = "a"
    ) |>
    dplyr::relocate(a0, .before = dplyr::any_of("a1")) |>
    dplyr::full_join(
      y = data.frame(
        Year = unique(dat_grouped$Year),
        seas = seas,
        gender = gender,
        GP = GP,
        bseas = bseas
      ),
      by = "Year"
    ) |>
    dplyr::rename("#Yr" = Year) |>
    dplyr::mutate(fleet = as.integer(fleet_id)) |>
    dplyr::arrange(`#Yr`)

  out
}

#' Fill NA values in a wtatage matrix via adjacent averaging
#'
#' Fills missing age values by averaging the nearest non-missing values in the
#' specified direction (across years or across ages).
#'
#' @param wtage A data frame created by [weight_at_age_wide()].
#' @param option Either `"row"` (interpolate across years within each age column)
#'   or `"age"` (interpolate across ages within each year/row).
#' @return A data frame with NA values replaced and a `Note` column appended.
#' @export
fill_wtage_matrix <- function(wtage, option = c("row", "age")) {
  option <- paste0("i", match.arg(option, several.ok = FALSE))
  if (!"Note" %in% colnames(wtage)) wtage$Note <- ""
  nages <- ncol(wtage) - ifelse("Note" %in% colnames(wtage), 1, 0) - 6

  for (irow in seq_len(nrow(wtage))) {
    isNA <- (1:nages)[is.na(wtage[irow, -(1:6)])]
    if (length(isNA) > 0) {
      wtage$Note[irow] <- paste(
        wtage$Note[irow],
        "# interpolated ages", paste(isNA - 1, collapse = ",")
      )
      for (iage in isNA) {
        if (get(option) > 1) {
          if (option == "irow") earliervals <- wtage[1:(irow - 1), iage + 6]
          if (option == "iage") earliervals <- wtage[irow, (1:(iage - 1)) + 6]
        } else {
          earliervals <- NA
        }

        if (get(option) < ifelse(option == "irow", nrow(wtage), nages)) {
          if (option == "irow") latervals <- wtage[(irow + 1):nrow(wtage), iage + 6]
          if (option == "iage") latervals <- wtage[irow, ((iage + 1):nages) + 6]
        } else {
          latervals <- NA
        }

        lastearlier <- rev(earliervals[!is.na(earliervals)])[1]
        firstlater  <- latervals[!is.na(latervals)][1]
        if (is.na(lastearlier)) lastearlier <- firstlater
        if (is.na(firstlater))  firstlater  <- lastearlier
        wtage[irow, iage + 6] <- mean(c(lastearlier, firstlater), na.rm = TRUE)
      }
    }
  }

  wtage
}

#' A compact high-contrast palette
#' @keywords internal
rich.colors.short <- function(n, alpha = 1) {
  x <- seq(0, 1, length.out = n)
  r <- 1 / (1 + exp(20 - 35 * x))
  g <- pmin(pmax(0, -0.8 + 6 * x - 5 * x^2), 1)
  b <- stats::dnorm(x, 0.25, 0.15) / max(stats::dnorm(x, 0.25, 0.15))
  rgb.m <- cbind(r, g, b)
  apply(rgb.m, 1, function(v) grDevices::rgb(v[1], v[2], v[3], alpha = alpha))
}

#' Heatmap-style plot of a year-by-age matrix with optional text
#'
#' Creates an image plot with ages on the x-axis and years on the y-axis and
#' optionally overlays values (e.g., weights) or sample sizes.
#'
#' @param agevec Vector of ages (default 0:10)
#' @param yrvec Vector of years (default 1990:2022)
#' @param mat Matrix of values by age (rows) and year (columns) **excluding**
#'   the mean vector. See Details in [make_wtatage_plots()].
#' @param meanvec Vector of mean values by age (across years).
#' @param Ntext If TRUE, print sample sizes instead of values.
#' @param Nsamp.mat Matrix of sample sizes aligned with `mat`.
#' @param Nsamp.meanvec Vector of sample sizes aligned with `meanvec`.
#' @param interpmat Optional matrix indicating which values were interpolated
#'   (used for bolding/shading).
#' @param main Plot title.
#' @param dofont Use bold font for interpolated values (if interpmat supplied).
#' @param dorect Shade interpolated cells (if interpmat supplied).
#' @param margins Plot margins.
#'
#' @export
makeimage <- function(agevec = 0:10, yrvec = 1990:2022,
                      mat,
                      meanvec = NULL,
                      Ntext = FALSE,
                      Nsamp.mat = NULL,
                      Nsamp.meanvec = NULL,
                      interpmat = NULL,
                      main = "",
                      dofont = TRUE,
                      dorect = FALSE,
                      margins = c(4.2, 4.2, 4, 1) + .1) {

  # If meanvec not provided, assume first "column" of mat is mean; this is legacy behavior.
  if (is.null(meanvec)) {
    meanvec <- mat[, 1]
    mat <- mat[, c(agevec + 7)]
    if (Ntext) {
      Nsamp.meanvec <- Nsamp.mat[, 1]
      Nsamp.mat <- Nsamp.mat[, -1]
    }
  }

  graphics::par(mar = margins)

  # Include a "mean" row label by inserting dummy years 1973/1974 as in legacy function
  yrvec2 <- c(1973, 1974, yrvec)
  mat2 <- cbind(meanvec, NA, mat)

  if (Ntext) {
    if (is.null(Nsamp.mat) || is.null(Nsamp.meanvec)) {
      stop("If Ntext=TRUE, supply Nsamp.mat and Nsamp.meanvec.")
    }
    Nsamp.mat2 <- cbind(Nsamp.meanvec, NA, Nsamp.mat)
  }

  # Guess breaks based on magnitude
  if (max(mat, na.rm = TRUE) < 4) { # assume weights in kg
    breaks <- seq(0, 4, length.out = 51)
    digits <- 2
  } else { # assume length in cm
    breaks <- seq(10, 80, length.out = 51)
    digits <- 1
  }

  graphics::image(
    x = agevec, y = yrvec2, z = mat2, axes = FALSE,
    xlab = "Age", ylab = "Year",
    col = grDevices::rainbow(60)[1:50],
    main = main, breaks = breaks
  )

  # Overlay text
  zdataframe <- expand.grid(yr = yrvec2, age = agevec)
  zdataframe$z <- c(t(mat2))
  if (Ntext) zdataframe$Nsamp <- c(t(Nsamp.mat2))

  if (!is.null(interpmat)) {
    interpmat2 <- cbind(meanvec, NA, interpmat)
    zdataframe$interp <- c(t(interpmat2))
  } else {
    zdataframe$interp <- 0
  }

  zdataframe$font <- 1
  if (dofont) zdataframe$font <- ifelse(is.na(zdataframe$interp), 2, 1)

  if (!Ntext) {
    ztext <- format(round(zdataframe$z, digits))
    ztext[ztext %in% c("  NA","   NA"," NA")] <- ""
    graphics::text(x = zdataframe$age, y = zdataframe$yr,
                   labels = ztext, font = zdataframe$font, cex = .7)
  } else {
    graphics::text(x = zdataframe$age, y = zdataframe$yr,
                   labels = zdataframe$Nsamp, font = zdataframe$font, cex = .7)
  }

  interp <- zdataframe[is.na(zdataframe$interp) & zdataframe$yr != 1974, ]
  if (dorect && nrow(interp) > 0) {
    graphics::rect(
      interp$age - .5, interp$yr - .5,
      interp$age + .5, interp$yr + .5,
      col = grDevices::rgb(0, 0, 0, .3), density = 20
    )
  }

  graphics::axis(1, at = agevec, cex.axis = .7)
  graphics::axis(2, at = c(1973, yrvec),
                 labels = c("mean", yrvec), las = 1, cex.axis = .7)
}

#' Simple interpolation across years for each age column
#'
#' Uses linear interpolation (approx) to fill missing values within each column.
#'
#' @param df A data frame whose age columns are numeric.
#' @param skipcols Columns to skip (defaults to SS metadata cols 1:6).
#' @return A data frame with interpolated columns.
#' @export
dointerpSimple <- function(df, skipcols = 1:6) {
  # Drop trailing non-numeric Note column if present
  if ("Note" %in% names(df)) {
    note <- df$Note
    df$Note <- NULL
  } else {
    note <- NULL
  }

  cols <- setdiff(seq_len(ncol(df)), skipcols)
  n <- nrow(df)

  for (icol in cols) {
    y <- suppressWarnings(as.numeric(df[[icol]]))
    df[[icol]] <- stats::approx(x = seq_len(n), xout = seq_len(n), y = y, rule = 2)$y
  }

  if (!is.null(note)) df$Note <- note
  df
}

#' Write EWAA plots to disk
#'
#' Produces up to six plots:
#'  1) raw (no interpolation) heatmap + optional numbers plot,
#'  2) interpolated heatmap,
#'  3) interpolated + extrapolated heatmap,
#'  4) same as (3) with shaded interpolations,
#'  5) same as (3) with bold interpolations,
#'  6) length-at-age heatmap (+ optional numbers) if `lengths` supplied.
#'
#' @param plots Integer vector specifying which plots to create (default 1:6).
#' @param data Wide EWAA data (weights) with first row as age-mean (see Details).
#' @param counts Wide EWAA counts aligned with `data`.
#' @param lengths Optional wide EWAA lengths aligned with `data`.
#' @param dir Output directory (default getwd()).
#' @param year Year label used in filenames (default current year).
#' @param maxage Plus-group age.
#'
#' @details This function expects `data` and `counts` to include an **extra first row**
#' representing the mean across years (as used in some legacy EBS PCOD workflows).
#' If your `weight_at_age_wide()` output does not include this mean row, prepend it
#' before calling, or adapt the plotting call to pass `meanvec` explicitly.
#'
#' @export
make_wtatage_plots <- function(plots = 1:6, data, counts, lengths = NULL,
                               dir = getwd(),
                               year = format(Sys.Date(), "%Y"),
                               maxage = 15) {

  on.exit(grDevices::graphics.off(), add = TRUE)

  # Remove any Note columns
  data   <- data[, !grepl("Note", names(data), ignore.case = TRUE), drop = FALSE]
  counts <- counts[, !grepl("Note", names(counts), ignore.case = TRUE), drop = FALSE]
  if (!is.null(lengths)) {
    lengths <- lengths[, !grepl("Note", names(lengths), ignore.case = TRUE), drop = FALSE]
  }

  agecols <- .ewaa_agecols(data, maxage)

  meanvec <- as.numeric(data[1, agecols])
  mat     <- t(as.matrix(data[-1, agecols, drop = FALSE]))

  wt1  <- dointerpSimple(data[-1, , drop = FALSE])
  temp <- fill_wtage_matrix(wt1[, c(1:6, agecols), drop = FALSE])
  mat2 <- t(as.matrix(temp[, agecols, drop = FALSE]))

  Nsamp.meanvec <- as.numeric(counts[1, agecols])
  Nsamp.mat     <- t(as.matrix(counts[-1, agecols, drop = FALSE]))

  yrvec_raw <- (as.integer(year) - ncol(mat) + 1):as.integer(year)

  # 1) no interpolation (and numbers)
  if (1 %in% plots) {
    fileplot <- file.path(dir, paste0("empirical_wtatage_", year, "_alldata_1_nointerp.png"))
    grDevices::png(fileplot, width = 7, height = 9, units = "in", res = 400)
    makeimage(mat = mat, meanvec = meanvec,
              main = "Mean weight at age (all data)",
              yrvec = yrvec_raw, agevec = 0:maxage)
    grDevices::dev.off()

    fileplot <- file.path(dir, paste0("empirical_wtatage_", year, "_alldata_1B_nointerp_numbers.png"))
    grDevices::png(fileplot, width = 7, height = 9, units = "in", res = 400)
    makeimage(mat = mat, meanvec = meanvec, Ntext = TRUE,
              Nsamp.meanvec = Nsamp.meanvec, Nsamp.mat = Nsamp.mat,
              main = "Mean weight at age (colors) with sample sizes (numbers)",
              yrvec = yrvec_raw, agevec = 0:maxage)
    grDevices::dev.off()
  }

  # 2) interpolation only
  if (2 %in% plots) {
    mat1 <- t(as.matrix(wt1[, agecols, drop = FALSE]))
    fileplot <- file.path(dir, paste0("empirical_wtatage_", year, "_alldata_2_interp.png"))
    grDevices::png(fileplot, width = 7, height = 9, units = "in", res = 400)
    makeimage(mat = mat1, meanvec = meanvec,
              main = "Mean weight at age with interpolation (all data)",
              yrvec = yrvec_raw, agevec = 0:maxage)
    grDevices::dev.off()
  }

  # 3) interpolation + extrapolation
  if (3 %in% plots) {
    fileplot <- file.path(dir, paste0("empirical_wtatage_", year, "_alldata_3_interp_extrap.png"))
    grDevices::png(fileplot, width = 7, height = 9, units = "in", res = 400)
    makeimage(mat = mat2, meanvec = meanvec,
              main = "Mean weight at age with interpolation & extrapolation (all data)",
              yrvec = yrvec_raw, agevec = 0:maxage)
    grDevices::dev.off()
  }

  # 4) shaded interpolations
  if (4 %in% plots) {
    fileplot <- file.path(dir, paste0("empirical_wtatage_", year, "_alldata_4_interp_extrap_shade.png"))
    grDevices::png(fileplot, width = 7, height = 9, units = "in", res = 400)
    makeimage(mat = mat2, interpmat = mat, dofont = FALSE, dorect = TRUE,
              meanvec = meanvec,
              main = "Mean weight at age with interpolation & extrapolation (all data)",
              yrvec = yrvec_raw, agevec = 0:maxage)
    grDevices::dev.off()
  }

  # 5) bold interpolations
  if (5 %in% plots) {
    fileplot <- file.path(dir, paste0("empirical_wtatage_", year, "_alldata_5_interp_extrap_bold.png"))
    grDevices::png(fileplot, width = 7, height = 9, units = "in", res = 400)
    makeimage(mat = mat2, interpmat = mat, dofont = TRUE, dorect = FALSE,
              meanvec = meanvec,
              main = "Mean weight at age with interpolation & extrapolation (all data)",
              yrvec = yrvec_raw, agevec = 0:maxage)
    grDevices::dev.off()
  }

  # 6) length-at-age
  if (6 %in% plots && !is.null(lengths)) {
    len.meanvec <- as.numeric(lengths[1, agecols])
    len.mat     <- t(as.matrix(lengths[-1, agecols, drop = FALSE]))

    fileplot <- file.path(dir, paste0("empirical_lenatage_", year, "_alldata_6_nointerp.png"))
    grDevices::png(fileplot, width = 7, height = 9, units = "in", res = 400)
    makeimage(mat = len.mat, meanvec = len.meanvec,
              main = "Mean length at age (all data, cm)",
              yrvec = yrvec_raw, agevec = 0:maxage)
    grDevices::dev.off()

    fileplot <- file.path(dir, paste0("empirical_lenatage_", year, "_6B_nointerp_numbers.png"))
    grDevices::png(fileplot, width = 7, height = 9, units = "in", res = 400)
    makeimage(mat = len.mat, meanvec = len.meanvec, Ntext = TRUE,
              Nsamp.meanvec = Nsamp.meanvec, Nsamp.mat = Nsamp.mat,
              main = "Mean length at age (colors) with sample sizes (numbers)",
              yrvec = yrvec_raw, agevec = 0:maxage)
    grDevices::dev.off()
  }

  invisible(TRUE)
}

#' Write Stock Synthesis weight-at-age input file
#'
#' Writes a `.ss`-style file with fleet blocks for:
#'  - fleet = -2: maturity*fecundity (maturity * wtatage from fleet 0 rows),
#'  - fleet = -1: mid-year population,
#'  - fleet = 0: beginning-of-year population,
#'  - fleet > 0: fleets (fishery/survey as supplied).
#'
#' @param file Output filename (default includes timestamp).
#' @param data Wide wtatage matrix with columns `#Yr, seas, gender, GP, bseas, fleet, a0..amaxage`.
#' @param maturity Numeric vector of maturity-at-age aligned with age columns.
#'
#' @export
write_wtatage_file <- function(
  file = paste0("wtatage_", format(Sys.time(), "%Y"), "created_", format(Sys.time(), "%d-%b-%Y_%H.%M"), ".ss"),
  data,
  maturity
) {
  # Ensure the fleet column is named "fleet"
  names(data)[grep("^fleet$", names(data), ignore.case = TRUE)] <- "fleet"

  agecols <- grep("^a\\d+$", names(data))
  if (length(agecols) == 0) stop("No age columns found (expected names like a0, a1, ...).")
  if (length(maturity) != length(agecols)) {
    stop("Length of maturity (", length(maturity), ") must match number of age columns (", length(agecols), ").")
  }

  # Remove file if exists (avoid appending)
  if (file.exists(file)) file.remove(file)

  # Helper to print data frame with SS-style header hash
  printdf <- function(dataframe) {
    names(dataframe)[1] <- paste("#_", names(dataframe)[1], sep = "")
    print(dataframe, row.names = FALSE, strip.white = TRUE)
  }

  oldwidth <- options()$width
  oldmax.print <- options()$max.print
  on.exit(options(width = oldwidth, max.print = oldmax.print), add = TRUE)
  options(width = 5000, max.print = 9999999)

  zz <- file(file, open = "at")
  on.exit(close(zz), add = TRUE)
  sink(zz)
  on.exit({ if (sink.number() > 0) sink() }, add = TRUE)

  header <- c(
    "# empirical weight-at-age Stock Synthesis input file",
    "# created by EWAA_utils.R",
    paste("# creation date:", Sys.time()),
    "###################################################",
    paste0(max(as.integer(sub("^a","",names(data)[agecols])), na.rm = TRUE), " # Maximum age"),
    "",
    "#Maturity x Fecundity: Fleet = -2",
    "#Maturity x Fecundity values are maturity * wtatage from fleet 0 rows",
    ""
  )
  writeLines(header)

  # Fleet = -2: maturity * wtatage for rows where fleet==0 (population at beginning of year)
  if (!any(data$fleet == 0, na.rm = TRUE)) {
    stop("To compute fleet=-2, data must contain at least one row with fleet==0.")
  }

  fleet0 <- data[data$fleet == 0, , drop = FALSE]
  fleetn2 <- cbind(fleet0[, 1:6, drop = FALSE],
                   t(apply(fleet0[, agecols, drop = FALSE], 1, function(x) as.numeric(x) * maturity)))
  fleetn2$fleet <- -2
  printdf(fleetn2)
  writeLines("#")

  # Write blocks for each requested fleet code
  for (ifleet in -1:3) {
    block <- data
    block$fleet <- ifleet

    note <- if (ifleet == -1) {
      "#Weight at age for population in middle of the year: Fleet = -1"
    } else if (ifleet == 0) {
      "#Weight at age for population at beginning of the year: Fleet = 0"
    } else if (ifleet == 1) {
      "#Weight at age for Fishery: Fleet = 1"
    } else {
      paste0("#Weight at age for Survey or other fleet: Fleet = ", ifleet)
    }

    writeLines(c("", note))
    printdf(block)
  }

  # Terminator line
  terminator <- fleetn2[1, , drop = FALSE]
  terminator[,] <- 0
  terminator[1, 1] <- -9999
  terminator[1, "fleet"] <- 2
  writeLines(c("", "# terminator line"))
  printdf(terminator)

  writeLines("# End of wtatage.ss file")
}

#' Identify weight-at-age outliers
#'
#' Flags outliers using simple screening rules:
#'  * September 2003: fish < 0.01 kg and length > 130 cm,
#'  * weight-length envelope: outside [2e-6*L^3, 20e-6*L^3],
#'  * ages >= 20 (often coded ages like 99/99+ or miscoded).
#'
#' @param data A long-format EWAA data frame.
#' @param filter If TRUE, remove outliers; if FALSE, retain all rows.
#' @param drop If TRUE, drop the `outlier` column before returning (when filter=TRUE).
#' @return A data frame with outliers flagged/removed.
#' @export
weight_at_age_outlier <- function(data, filter = TRUE, drop = TRUE) {
  data <- .ewaa_validate_long(data)

  out <- data |>
    dplyr::mutate(
      outlier = dplyr::case_when(
        Weight_kg <= 0.01 & Length_cm > 130 & Year == 2003 & Month == 9 ~ TRUE,
        Weight_kg > (20e-6) * Length_cm^3 ~ TRUE,
        Weight_kg < (2e-6) * Length_cm^3 ~ TRUE,
        Age_yrs >= 20 ~ TRUE,
        TRUE ~ FALSE
      )
    ) |>
    dplyr::filter(outlier %in% c(FALSE, ifelse(filter, FALSE, TRUE)))

  if (drop) out |> dplyr::select(-outlier) else out
}

#' Plot weight-at-age outlier diagnostics
#'
#' Produces length-weight and age-weight scatterplots (including log scales),
#' with outliers highlighted.
#'
#' @param data Long-format EWAA data. If `outlier` column not present, it is computed.
#' @param ... Variables to facet by (passed to facet_wrap).
#' @return A cowplot object.
#' @export
plot_weight_at_age_outlier <- function(data, ...) {
  wrap_by <- function(...) {
    ggplot2::facet_wrap(ggplot2::vars(...), labeller = ggplot2::label_both)
  }

  data_plot <- if (!"outlier" %in% names(data)) {
    weight_at_age_outlier(data, filter = FALSE, drop = FALSE)
  } else {
    dplyr::filter(data, !is.na(Weight_kg))
  }

  data_lines <- data.frame(Length_cm = 1:max(data_plot[["Length_cm"]], na.rm = TRUE)) |>
    dplyr::mutate(lower = Length_cm^3 * 2e-6,
                  upper = Length_cm^3 * 20e-6)

  gg_length <- ggplot2::ggplot(
    data_plot,
    ggplot2::aes(x = Length_cm, y = Weight_kg)
  ) +
    ggplot2::geom_point(pch = 16, ggplot2::aes(color = outlier)) +
    ggplot2::scale_colour_manual(values = c(grDevices::rgb(0, 0, 0, 0.2), 2)) +
    ggplot2::geom_line(data = data_lines, colour = 4, ggplot2::aes(y = lower)) +
    ggplot2::geom_line(data = data_lines, colour = 4, ggplot2::aes(y = upper)) +
    ggplot2::ylim(c(0, max(data_plot[["Weight_kg"]], na.rm = TRUE) * 1.01)) +
    ggplot2::xlab("Length (cm)") +
    ggplot2::ylab("Weight (kg)") +
    ggplot2::theme(legend.title = ggplot2::element_text("Outlier"),
                   legend.position = "none") +
    wrap_by(...)

  gg_age <- ggplot2::ggplot(
    data_plot,
    ggplot2::aes(x = Age_yrs, y = Weight_kg)
  ) +
    ggplot2::geom_point(pch = 16, ggplot2::aes(colour = outlier)) +
    ggplot2::scale_colour_manual(values = c(grDevices::rgb(0, 0, 0, 0.2), 2)) +
    ggplot2::xlab("Age (years)") +
    ggplot2::ylab("Weight (kg)") +
    ggplot2::theme(legend.title = ggplot2::element_text("Outlier"),
                   legend.position = "none") +
    wrap_by(...)

  gg_age_log <- gg_age +
    ggplot2::scale_x_continuous(trans = "log10") +
    ggplot2::scale_y_continuous(trans = "log10") +
    ggplot2::theme(legend.position = "top", legend.box = "horizontal") +
    wrap_by(...)

  cowplot::plot_grid(gg_length, gg_age, gg_age_log, align = "h", nrow = 1)
}

#' Read weight-at-age files from disk
#'
#' Reads a `.csv` file containing the required EWAA columns and normalizes `Source`.
#'
#' @param file Path to a `.csv` file with columns: Source, Weight_kg, Age_yrs,
#'   Length_cm, Month, Year (Sex column is optional and will be dropped).
#'
#' @return A data frame of EWAA information.
#' @export
weight_at_age_read <- function(file) {
  if (tools::file_ext(file) != "csv") {
    stop("Extensions other than csv are not currently supported.")
  }
  data_in <- utils::read.csv(file) |>
    dplyr::select(-dplyr::matches("^X$|^X\\."))

  out <- data_in |>
    dplyr::mutate(
      Source = gsub("(^SHORE$|^ATSEA$)", "US_\\L\\1", Source, perl = TRUE),
      Source = dplyr::case_when(
        grepl("_jv$", Source, ignore.case = TRUE) ~ toupper(Source),
        Source == "Poland_acoustic" ~ "Acoustic Poland",
        Source == "Acoustic U.S." ~ "US_acoustic",
        Source == "Acoustic Canada" ~ "CAN_acoustic",
        Source == "ATSEA" ~ "US_atsea",
        TRUE ~ Source
      )
    ) |>
    dplyr::select(-dplyr::matches("Sex"))

  invisible(out)
}
