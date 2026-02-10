#' Get survey specimen ages and optionally apply an external age-correction model
#'
#' Queries specimen-level survey age data from AKFIN (via the SQL template
#' \code{survey_age.sql}) and returns a table of specimen records. Adds \code{AGE1},
#' which pools all ages \code{>= max_age} into a plus group.
#'
#' Optionally, an external CSV can be provided (via \code{extra_csv}) to fit a
#' correction model (legacy GAM-based approach) and overwrite \code{AGE1} for
#' a subset of years. This replaces the previous hard-coded dependency on
#' \code{"Steves pcod4.csv"} while keeping the ability to use external data.
#'
#' @param con_akfin A DBI connection to the AKFIN database.
#' @param area Character area identifier: one of \code{"GOA"}, \code{"BS"},
#'   \code{"AI"}, \code{"SLOPE"}.
#' @param species Numeric species code (single value or vector).
#' @param start_yr Numeric start year (inclusive) for the AKFIN query.
#' @param max_age Integer maximum age; ages \code{>= max_age} are pooled into \code{max_age}.
#' @param extra_csv Optional path to a CSV file used for external age-correction.
#'   If \code{NULL}, no correction is applied.
#' @param extra_reader Function used to read \code{extra_csv}. Defaults to
#'   \code{utils::read.csv}.
#' @param apply_gam_correction Logical; if TRUE and \code{extra_csv} is provided,
#'   attempts the legacy GAM correction. If required columns are missing, a warning
#'   is issued and correction is skipped.
#' @param correction_year_cutoff Numeric; if GAM correction is applied, years
#'   \code{< correction_year_cutoff} are assigned GAM-predicted ages (legacy default 2007).
#'
#' @return A data.table of specimen records with an added \code{AGE1} column.
#'
#' @export
GET_SURV_AGE_cor <- function(con_akfin,
                             area = "BS-NBS",
                             species = 21720,
                             start_yr = 1977,
                             max_age = 10,
                             extra_csv = NULL,
                             extra_reader = utils::read.csv,
                             apply_gam_correction = TRUE,
                             correction_year_cutoff = 2007) {

  if (missing(con_akfin) || is.null(con_akfin)) {
    stop("`con_akfin` must be a valid DBI connection.", call. = FALSE)
  }
  if (!is.numeric(species)) stop("`species` must be numeric.", call. = FALSE)
  if (!is.numeric(start_yr) || length(start_yr) != 1L) stop("`start_yr` must be a single numeric year.", call. = FALSE)
  if (!is.numeric(max_age) || length(max_age) != 1L) stop("`max_age` must be a single numeric value.", call. = FALSE)
  max_age <- as.integer(max_age)

  # ---- map area -> survey_definition_id (numeric) ----
  area <- toupper(area)
  survey <- switch(
    area,
    "GOA"   = 47,
    "BS"    = c(98,143),
    "NBS"   = 143,
    "BS-NBS" = c(98),
    "AI"    = 52,
    "SLOPE" = 78,
    "BS-WGOA" = c(47,98,143),
    stop("Not a valid `area`. Use GOA, BS, NBS, BS-NBS, AI, BS-WGOA, or SLOPE.", call. = FALSE)
  )

  # ---- query using packaged SQL template ----

  sql <- sql_reader("survey_age.sql")
  sql <- sql_filter(sql_precode = ">=", x = start_yr, sql_code = sql,
                    flag = "-- insert start_year", value_type = "numeric")
  sql <- sql_filter(sql_precode = "IN", x = species,  sql_code = sql,
                    flag = "-- insert species", value_type = "numeric")
  sql <- sql_filter(sql_precode = "IN", x = survey,   sql_code = sql,
                    flag = "-- insert survey", value_type = "numeric")

  Age <- sql_run(con_akfin, sql) |>
    data.table::as.data.table()
  
  if(area=='BS-WGOA') { Age[longitude_dd_end <= -158 | longitude_dd_end > 0]}

  # Standardize column names to uppercase for consistency
  data.table::setnames(Age, toupper(names(Age)))

  # Create legacy-friendly aliases (without dropping original fields)
  # Expected from survey_age.sql:
  # YEAR, CRUISEJOIN, CRUISE, VESSEL_ID, HAULJOIN, HAUL, SPECIES_CODE, LENGTH_MM, SEX, WEIGHT_G, MATURITY, AGE, ...
  if ("VESSEL_ID" %in% names(Age)) Age[, VESSEL := VESSEL_ID]
  if ("LENGTH_MM" %in% names(Age)) Age[, LENGTH := LENGTH_MM]
  if ("WEIGHT_G"  %in% names(Age)) Age[, WEIGHT := WEIGHT_G]

  # ---- default AGE1: plus-group only ----
  Age[, AGE1 := AGE]
  Age[!is.na(AGE1) & AGE1 >= max_age, AGE1 := max_age]

  # ---- optional external correction data (replaces hard-coded "Steves pcod4.csv") ----
  if (!is.null(extra_csv) && isTRUE(apply_gam_correction)) {

    if (!file.exists(extra_csv)) {
      stop("`extra_csv` not found: ", extra_csv, call. = FALSE)
    }

    ext <- data.table::as.data.table(extra_reader(extra_csv))

    # Try legacy correction only if required columns exist
    required <- c("Read","Year","Cruise_Number","Haul","Vessel_Code",
                  "Group","Sex","Length","Weight","Final_Age")
    missing_cols <- setdiff(required, names(ext))

    if (length(missing_cols) > 0) {
      warning(
        "extra_csv provided, but missing required columns for legacy GAM correction: ",
        paste(missing_cols, collapse = ", "),
        "\nSkipping correction and returning plus-grouped AGE1 only."
      )
      return(Age[])
    }

    # Legacy transformation block (kept as close as possible to your original)
    data_G <- ext[, .(Read, Year, Cruise_Number, Haul, Vessel_Code, Group, Sex, Length, Weight, Final_Age)]
    data1 <- data_G[Read == "First"]
    data2 <- data_G[Read != "First"]

    # legacy fixes
    data2[Year == 2004, Vessel_Code := "999"]
    data2[Year %in% c(1998, 1999), Vessel_Code := "41"]

    data1[, Read := NULL]
    data2[, Read := NULL]
    data.table::setnames(data1, "Final_Age", "First_Age")

    data3 <- merge(data1, data2)
    data3 <- data3[!is.na(Final_Age)]

    # Legacy GOA-only model subset (as in your script)
    dataGOA <- data3[Group == "GOA groundfish survey"]
    data.table::setnames(
      dataGOA,
      old = c("Year","Cruise_Number","Haul","Vessel_Code","Sex","Length","Weight","Final_Age"),
      new = c("YEAR","CRUISE","HAUL","VESSEL","SEX","LENGTH","WEIGHT","Final_Age"),
      skip_absent = TRUE
    )
    # Ensure the legacy columns exist
    if (!all(c("SEX","AGE","Final_Age") %in% names(dataGOA))) {
      # AGE is created below by rename if present; if not, bail
      warning("Legacy correction data did not produce expected columns; skipping correction.")
      return(Age[])
    }

    # Fit GAM and predict for queried Age table
    # (kept consistent with your original: Final_Age ~ s(AGE, by = factor(SEX)))
    if (!requireNamespace("mgcv", quietly = TRUE)) {
      warning("Package 'mgcv' not installed; skipping GAM correction.")
      return(Age[])
    }

    # In your original, AGE is the observed age in the correction dataset
    # Here, dataGOA must contain AGE; if it doesn't, skip.
    if (!"AGE" %in% names(dataGOA)) {
      warning("Correction CSV does not contain AGE after processing; skipping GAM correction.")
      return(Age[])
    }

    fit <- mgcv::gam(Final_Age ~ mgcv::s(AGE, by = factor(SEX)), data = dataGOA)

    # Predict corrected age for the pulled AKFIN Age records
    Age[, GAM_AGE := round(stats::predict(fit, newdata = Age))]

    # Legacy rule: overwrite AGE1 for years prior to cutoff
    Age[!is.na(GAM_AGE) & YEAR < correction_year_cutoff, AGE1 := GAM_AGE]
    Age[!is.na(AGE1) & AGE1 >= max_age, AGE1 := max_age]
  }

  Age[]
}
