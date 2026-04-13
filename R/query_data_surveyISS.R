#' Query data to run surveyISS (region_def-driven with REGION_GRP output)
#'
#' @description
#' Pulls cpue, length frequency (lfreq), specimen, strata, and species information
#' from GAP_PRODUCTS tables using a supplied DBI connection (con_akfin).
#'
#' `region_def` is a named list that defines regions by STRATUM IDs. The output
#' data frames include REGION_GRP set to the name of the list element.
#'
#' @param con_akfin A live DBI connection to AKFIN (or equivalent) containing gap_products schema.
#' @param survey survey_definition_id(s) to pull (ai=52, goa=47, ebs=98, nbs=143, etc.).
#' @param region_def Named list mapping REGION_GRP -> stratum IDs (must be non-overlapping).
#'   Example: list(WGOA=c(101,102), CGOA=c(201,202))
#' @param species species_codes, e.g. c(10110, 21740)
#' @param yrs minimum survey year to consider (default NULL; treated as 0)
#' @param write_csv Logical; if TRUE write csvs to data/{region_name}/ where region_name is each REGION_GRP
#' @param disconnect Logical; if TRUE, disconnect con_akfin at end (default FALSE)
#'
#' @return A list with lfreq, specimen, cpue, strata, species (all tidytable),
#'   each including REGION_GRP (except species table).
#' @export
query_data_surveyISS <- function(con_akfin,
                       survey,
                       region_def,
                       species,
                       yrs = NULL,
                       write_csv = TRUE,
                       disconnect = FALSE) {

  # ---- deps ----
  for (pkg in c("dplyr", "tidytable", "vroom", "here", "DBI")) {
    if (!requireNamespace(pkg, quietly = TRUE)) stop(pkg, " required.", call. = FALSE)
  }

  if (missing(con_akfin) || is.null(con_akfin)) stop("`con_akfin` is required (a live DBI connection).", call. = FALSE)
  if (!DBI::dbIsValid(con_akfin)) stop("`con_akfin` is not a valid/active DBI connection.", call. = FALSE)

  if (missing(survey) || length(survey) < 1) stop("`survey` must be one or more survey_definition_id values.", call. = FALSE)
  survey <- suppressWarnings(as.integer(unique(as.vector(survey))))
  if (anyNA(survey)) stop("`survey` must be integer survey_definition_id values.", call. = FALSE)

  if (missing(species) || length(species) < 1) stop("`species` must be one or more species_code values.", call. = FALSE)
  species <- suppressWarnings(as.integer(unique(as.vector(species))))
  if (anyNA(species)) stop("`species` must be integer species_code values.", call. = FALSE)

  if (is.null(yrs)) yrs <- 0
  yrs <- suppressWarnings(as.integer(yrs))
  if (length(yrs) != 1L || is.na(yrs)) stop("`yrs` must be a single integer year (or NULL).", call. = FALSE)

  # ---- region_def validation + mapping table ----
  if (!is.list(region_def) || length(region_def) == 0 || is.null(names(region_def)) || any(!nzchar(names(region_def)))) {
    stop("`region_def` must be a *named* list mapping REGION_GRP -> stratum IDs.", call. = FALSE)
  }
  reg_names <- names(region_def)

  reg_clean <- lapply(region_def, function(x) suppressWarnings(as.integer(unique(as.vector(x)))))
  bad <- vapply(reg_clean, function(x) length(x) == 0 || anyNA(x), logical(1))
  if (any(bad)) stop("`region_def` has empty/invalid strata for: ", paste(reg_names[bad], collapse = ", "), call. = FALSE)

  all_strata <- unlist(reg_clean, use.names = FALSE)
  dup <- all_strata[duplicated(all_strata)]
  if (length(dup) > 0) {
    stop("`region_def` assigns the same STRATUM to multiple regions. Duplicates: ",
         paste(sort(unique(dup)), collapse = ", "), call. = FALSE)
  }

  # lookup: STRATUM -> REGION_GRP
  region_key <- data.frame(
    STRATUM = unlist(reg_clean, use.names = FALSE),
    REGION_GRP = rep(reg_names, times = vapply(reg_clean, length, integer(1))),
    stringsAsFactors = FALSE
  )

  strata_keep <- sort(unique(region_key$STRATUM))

  # ---- output folders ----
  if (isTRUE(write_csv)) {
    for (rg in reg_names) {
      out_dir <- here::here("data", rg)
      if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
    }
  }

  maybe_write <- function(df, region_grp, filename) {
    if (isTRUE(write_csv)) {
      vroom::vroom_write(df, here::here("data", region_grp, filename), delim = ",")
    }
    df
  }

  conn <- con_akfin
  on.exit({
    if (isTRUE(disconnect)) {
      try(DBI::dbDisconnect(conn), silent = TRUE)
    }
  }, add = TRUE)

  # helper to attach REGION_GRP via STRATUM
  add_region_grp <- function(df, stratum_col = "stratum") {
    df <- as.data.frame(df)
    names(df) <- tolower(names(df))
    if (!stratum_col %in% names(df)) stop("Expected column '", stratum_col, "' in result.", call. = FALSE)
    df[[stratum_col]] <- suppressWarnings(as.integer(df[[stratum_col]]))
    out <- dplyr::left_join(
      df,
      dplyr::rename(region_key, !!stratum_col := STRATUM),
      by = stratum_col
    )
    out <- out[!is.na(out$REGION_GRP), , drop = FALSE]
    out
  }

  # ---------------------------
  # lfreq
  # ---------------------------
  cat("pulling length frequency...\n")

  lfreq <- dplyr::tbl(conn, dplyr::sql("gap_products.akfin_haul")) %>%
    dplyr::inner_join(dplyr::tbl(conn, dplyr::sql("gap_products.akfin_cruise")),
                      by = c("CRUISEJOIN")) %>%
    dplyr::inner_join(dplyr::tbl(conn, dplyr::sql("gap_products.akfin_length")),
                      by = c("HAULJOIN")) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::select(year,
                  survey_definition_id,
                  species_code,
                  stratum,
                  hauljoin,
                  latitude_dd_start,
                  latitude_dd_end,
                  longitude_dd_start,
                  longitude_dd_end,
                  sex,
                  length_mm,
                  frequency) %>%
    dplyr::filter(survey_definition_id %in% survey,
                  species_code %in% species,
                  year >= yrs,
                  stratum %in% strata_keep) %>%
    dplyr::mutate(lat_mid = (latitude_dd_start + latitude_dd_end) / 2,
                  long_mid = (longitude_dd_start + longitude_dd_end) / 2) %>%
    dplyr::select(year,
                  survey = survey_definition_id,
                  species_code,
                  stratum,
                  hauljoin,
                  sex,
                  length = length_mm,
                  frequency,
                  lat_mid,
                  long_mid) %>%
    dplyr::collect()

  lfreq <- add_region_grp(lfreq, stratum_col = "stratum")

  # write per-region csv
  if (isTRUE(write_csv)) {
    for (rg in reg_names) {
      maybe_write(lfreq[lfreq$REGION_GRP == rg, , drop = FALSE], rg, "lfreq.csv")
    }
  }

  # ---------------------------
  # specimen
  # ---------------------------
  cat("pulling specimen...\n")

  specimen <- dplyr::tbl(conn, dplyr::sql("gap_products.akfin_haul")) %>%
    dplyr::inner_join(dplyr::tbl(conn, dplyr::sql("gap_products.akfin_cruise")),
                      by = c("CRUISEJOIN")) %>%
    dplyr::inner_join(dplyr::tbl(conn, dplyr::sql("gap_products.akfin_specimen")),
                      by = c("HAULJOIN")) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::select(year,
                  survey_definition_id,
                  species_code,
                  stratum,
                  hauljoin,
                  latitude_dd_start,
                  latitude_dd_end,
                  longitude_dd_start,
                  longitude_dd_end,
                  sex,
                  length_mm,
                  age) %>%
    dplyr::filter(survey_definition_id %in% survey,
                  species_code %in% species,
                  year >= yrs,
                  stratum %in% strata_keep) %>%
    dplyr::mutate(lat_mid = (latitude_dd_start + latitude_dd_end) / 2,
                  long_mid = (longitude_dd_start + longitude_dd_end) / 2) %>%
    dplyr::select(year,
                  survey = survey_definition_id,
                  species_code,
                  stratum,
                  hauljoin,
                  sex,
                  length = length_mm,
                  age,
                  lat_mid,
                  long_mid) %>%
    dplyr::collect()

  specimen <- add_region_grp(specimen, stratum_col = "stratum")

  if (isTRUE(write_csv)) {
    for (rg in reg_names) {
      maybe_write(specimen[specimen$REGION_GRP == rg, , drop = FALSE], rg, "specimen.csv")
    }
  }

  # ---------------------------
  # cpue + cpue_calc
  # ---------------------------
  cat("pulling cpue...\n")

  cpue <- dplyr::tbl(conn, dplyr::sql("gap_products.akfin_haul")) %>%
    dplyr::inner_join(dplyr::tbl(conn, dplyr::sql("gap_products.akfin_cruise")),
                      by = c("CRUISEJOIN")) %>%
    dplyr::inner_join(dplyr::tbl(conn, dplyr::sql("gap_products.akfin_cpue")),
                      by = c("HAULJOIN")) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::select(year,
                  survey_definition_id,
                  species_code,
                  stratum,
                  hauljoin,
                  latitude_dd_start,
                  latitude_dd_end,
                  longitude_dd_start,
                  longitude_dd_end,
                  cpue_nokm2) %>%
    dplyr::filter(survey_definition_id %in% survey,
                  species_code %in% species,
                  year >= yrs,
                  stratum %in% strata_keep) %>%
    dplyr::mutate(lat_mid = (latitude_dd_start + latitude_dd_end) / 2,
                  long_mid = (longitude_dd_start + longitude_dd_end) / 2) %>%
    dplyr::select(year,
                  species_code,
                  stratum,
                  hauljoin,
                  survey = survey_definition_id,
                  numcpue = cpue_nokm2,
                  lat_mid,
                  long_mid) %>%
    dplyr::collect()

  cpue <- add_region_grp(cpue, stratum_col = "stratum")

  cpue_calc <- dplyr::tbl(conn, dplyr::sql("gap_products.akfin_haul")) %>%
    dplyr::inner_join(dplyr::tbl(conn, dplyr::sql("gap_products.akfin_cruise")),
                      by = c("CRUISEJOIN")) %>%
    dplyr::inner_join(dplyr::tbl(conn, dplyr::sql("gap_products.akfin_catch")),
                      by = c("HAULJOIN")) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::select(year,
                  survey_definition_id,
                  species_code,
                  stratum,
                  hauljoin,
                  latitude_dd_start,
                  latitude_dd_end,
                  longitude_dd_start,
                  longitude_dd_end,
                  distance_fished_km,
                  net_width_m,
                  count) %>%
    dplyr::filter(survey_definition_id %in% survey,
                  species_code %in% species,
                  year >= yrs,
                  stratum %in% strata_keep) %>%
    dplyr::mutate(lat_mid = (latitude_dd_start + latitude_dd_end) / 2,
                  long_mid = (longitude_dd_start + longitude_dd_end) / 2,
                  numcpue = count / (distance_fished_km * (0.001 * net_width_m))) %>%
    dplyr::select(year,
                  survey = survey_definition_id,
                  species_code,
                  stratum,
                  hauljoin,
                  lat_mid,
                  long_mid,
                  numcpue) %>%
    dplyr::collect()

  cpue_calc <- add_region_grp(cpue_calc, stratum_col = "stratum")

  # fill in zeros like gapindex, *within each REGION_GRP*
  cpue_out <- tidytable::expand_grid(
    REGION_GRP = unique(cpue$REGION_GRP),
    hauljoin = unique(cpue$hauljoin),
    species_code = species
  ) %>%
    tidytable::left_join(
      cpue %>%
        tidytable::select(REGION_GRP, hauljoin, year, survey, stratum, lat_mid, long_mid) %>%
        tidytable::slice_head(n = 1, .by = c(REGION_GRP, hauljoin)),
      .by = c("REGION_GRP", "hauljoin")
    ) %>%
    tidytable::left_join(
      cpue_calc %>%
        tidytable::replace_na(list(numcpue = -1)),
      .by = c("REGION_GRP", "hauljoin", "species_code")
    ) %>%
    tidytable::replace_na(list(numcpue = 0))

  if (isTRUE(write_csv)) {
    for (rg in reg_names) {
      maybe_write(cpue_out[cpue_out$REGION_GRP == rg, , drop = FALSE], rg, "cpue.csv")
    }
  }

  # ---------------------------
  # strata
  # ---------------------------
  cat("pulling strata...\n")

  st_area <- dplyr::tbl(conn, dplyr::sql("gap_products.akfin_area")) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::filter(survey_definition_id %in% survey,
                  area_type == "STRATUM",
                  area_id %in% strata_keep) %>%
    dplyr::select(survey = survey_definition_id,
                  design_year,
                  stratum = area_id,
                  area = area_km2) %>%
    dplyr::collect()

  # regulatory areas
  subreg <- dplyr::tbl(conn, dplyr::sql("gap_products.akfin_area")) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::filter(survey_definition_id %in% survey,
                  area_type == "REGULATORY AREA") %>%
    dplyr::select(area_id, subarea_name = description, design_year) %>%
    dplyr::collect()

  st_subreg <- dplyr::tbl(conn, dplyr::sql("gap_products.akfin_stratum_groups")) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::filter(survey_definition_id %in% survey,
                  stratum %in% strata_keep) %>%
    dplyr::select(stratum, area_id) %>%
    dplyr::collect()

  strata <- st_area %>%
    tidytable::left_join(
      st_subreg %>%
        tidytable::left_join(subreg) %>%
        tidytable::drop_na(),
      .by = "stratum"
    ) %>%
    tidytable::filter(design_year == max(design_year), .by = c(stratum)) %>%
    as.data.frame()

  strata <- add_region_grp(strata, stratum_col = "stratum")

  if (isTRUE(write_csv)) {
    for (rg in reg_names) {
      maybe_write(strata[strata$REGION_GRP == rg, , drop = FALSE], rg, "strata.csv")
    }
  }

  # ---------------------------
  # species
  # ---------------------------
  cat("pulling species info...\n")

  species_tbl <- dplyr::tbl(conn, dplyr::sql("gap_products.akfin_taxonomic_classification")) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::filter(species_code %in% species) %>%
    dplyr::select(species_code, species_name, common_name) %>%
    dplyr::collect()

  if (isTRUE(write_csv)) {
    # species table not region-specific; write once to each region folder for convenience
    for (rg in reg_names) {
      maybe_write(species_tbl, rg, "species.csv")
    }
  }

  cat("finished.\n")

  cpue<-merge(strata,cpue,by=c("REGION_GRP","survey","stratum"))

  list(
    lfreq    = tidytable::as_tidytable(lfreq),
    specimen = tidytable::as_tidytable(specimen),
    cpue     = tidytable::as_tidytable(cpue_out),
    strata   = tidytable::as_tidytable(strata),
    species  = tidytable::as_tidytable(species_tbl)
  )
}