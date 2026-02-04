#' Build all Stock Synthesis inputs for EBS/AI Pacific cod (DR4SS)
#'
#' @description
#' Wrapper that orchestrates the DR4SS data pulls/formatters for a single assessment
#' “area” (e.g., BS/AI/GOA) and returns a named list of SS-ready objects.
#'
#' **Key updates vs legacy wrapper**
#' - Catch is built via [build_fishery_catch_ss()] (no inline SQL in the wrapper).
#' - Longline RPN CPUE is built via [build_ll_cpue()] and **fleet number is user-specified**
#'   (no `ONE_FLEET` logic to toggle index 3 vs 5).
#' - Fishery EWAA is built via [fishery_ewaa()] (optionally split by sex).
#' - Fishery catch-weighted lcomp is built via [LENGTH_BY_CATCH_short()].
#'
#' @param con_akfin DBI connection to AKFIN (required).
#' @param con_afsc  DBI connection to AFSC (required for some survey/fishery helpers).
#'
#' @param ly Last year to include (final assessment year / last fishery year).
#'
#' @param area Survey area code used by survey functions (e.g. "BS","AI","GOA","SLOPE").
#' @param sp_area Fishery “area” selector passed into fishery length-by-catch and LL CPUE
#'   SQL filters (your existing conventions; e.g. "BSAI" or "BS-NBS" depending on function).
#'
#' @param srv_sp_str Survey species code (AKFIN survey code; e.g. 21720 for EBS PCOD).
#' @param fsh_species Fishery species code used by fishery EWAA and fishery SQL (e.g. 202).
#' @param fsh_sp_label Fishery species label used by fishery catch SQL filter (as used in your SQL).
#' @param fsh_sp_area Fishery subarea filter injected into dom_catch.sql (vector).
#'
#' @param one_fleet Logical; if TRUE, aggregate fishery catch gears into fleet 1 in
#'   [build_fishery_catch_ss()].
#'
#' @param do_LL Logical; if TRUE include longline survey RPN CPUE via [build_ll_cpue()].
#' @param LL_fleet Integer SS fleet number for the LL CPUE index (e.g. 3 or 5).
#' @param LL_sp_region Region filter injected into LL_RPN.sql (your existing convention).
#' @param LLsrv_start_yr First year to include in LL CPUE output.
#' @param LL_seas Season number for LL CPUE (default 7).
#'
#' @param srv_start_yr Start year for survey biomass/comps pulls.
#' @param len_bins Length bins for survey length comps.
#' @param max_age Plus-group age for age comps and fishery EWAA outputs.
#'
#' @param do_fsh_ewaa Logical; if TRUE run [fishery_ewaa()].
#' @param fsh_ewaa_split_sex Logical; if TRUE, fishery EWAA fits/predicts split by sex and
#'   outputs F/M tables.
#'
#' @param do_fsh_lcomp Logical; if TRUE run [LENGTH_BY_CATCH_short()].
#' @param fsh_lcomp_sex Logical; passed to LENGTH_BY_CATCH_short(SEX=...).
#' @param fsh_lcomp_port Logical; passed to LENGTH_BY_CATCH_short(PORT=...).
#' @param fsh_species_catch Species code used by fishery catch/length SQL (can differ from survey).
#' @param for_species_catch Optional “foreign catch species” code used by LENGTH_BY_CATCH_short.
#'
#' @return Named list of SS inputs (and intermediate objects) suitable for writing into a
#' .dat file pipeline.
#'
#' @export
SBSS_GET_ALL_DATA <- function(
  con_akfin = conn$akfin,
  con_afsc = conn$afsc,
  ly =2025,
  area = "BS",
  sp_area = "BSAI",
  srv_sp_str = 21720,
  fsh_species = 202,
  fsh_sp_label = 'PCOD',
  fsh_sp_area = 'BS',
  one_fleet = FALSE,
  catch_se = 0.01,
  init_catch = 42500,
  # ---- LL CPUE (RPN) ----
  do_LL = FALSE,
  LL_fleet = 3,
  LL_sp_region = "Bering Sea",
  LLsrv_start_yr = 1990,
  LL_seas = 7,
  # ---- survey comps ----
  srv_start_yr = 1977,
  len_bins = seq(4.5, 119.5, 5),
  max_age = 10,
  # ---- fishery EWAA ----
  do_fsh_ewaa = TRUE,
  fsh_ewaa_split_sex = FALSE,
  # ---- fishery catch-weighted lcomp ----
  do_fsh_lcomp = TRUE,
  fsh_lcomp_sex = TRUE,
  fsh_lcomp_port = TRUE,
  fsh_species_catch = fsh_sp_label,
  for_species_catch = NULL
) {

  # ---- basic checks ----
  if (missing(con_akfin) || is.null(con_akfin)) stop("`con_akfin` is required.", call. = FALSE)
  if (missing(con_afsc)  || is.null(con_afsc))  stop("`con_afsc` is required.",  call. = FALSE)
  if (missing(ly) || length(ly) != 1L || !is.numeric(ly)) stop("`ly` must be a single numeric year.", call. = FALSE)

  area <- toupper(as.character(area))

  # ---- 1) Fishery catch (SS catch table) ----
  old_obj <- if (exists("OLD_SEAS_GEAR_CATCH", inherits = TRUE)) get("OLD_SEAS_GEAR_CATCH", inherits = TRUE) else NULL

  catch_res <- build_fishery_catch_ss(
    con                 = conn$akfin,
    final_year          = ly,
    fsh_sp_label        = fsh_sp_label,
    fsh_sp_area         = fsh_sp_area,
    one_fleet           = one_fleet,
    old_seas_gear_catch = if (!is.null(old_obj) && nrow(old_obj) > 0) old_obj else NULL,
    catch_total         = NULL,
    catch_se            = catch_se,
    init_catch          = init_catch
  )

  # ---- 2) Survey biomass ----
  surv_biom <- GET_SURVEY_BIOM(
    con_akfin = con_akfin,
    area      = area,
    species   = srv_sp_str,
    start_yr  = srv_start_yr
  )

  # ---- 3) Survey length comps ----
  surv_lcomp <- GET_SURVEY_LCOMP(
    con_akfin = con_akfin,
    species   = srv_sp_str,
    bins      = len_bins,
    bin       = TRUE,
    area      = area,
    sex       = 1,
    SS        = TRUE
  )

  # ---- 4) Survey age comps ----
  surv_acomp <- GET_SURVEY_ACOMP(
    con_akfin  = con_akfin,
    use_vast   = FALSE,
    vast_agecomp = NULL,
    species    = srv_sp_str,
    start_yr   = srv_start_yr,
    area       = area,
    max_age    = max_age
  )

  # ---- 5) Survey specimen ages (if you need the specimen table downstream) ----
  surv_age_specimens <- GET_SURV_AGE_cor(
    con_akfin = con_akfin,
    area      = sp_area,    # NOTE: this function uses your "BS-NBS" style default
    species   = srv_sp_str,
    start_yr  = srv_start_yr,
    max_age   = max_age
  )

  # ---- 6) Length–weight env series (if used in SS env block) ----
  lenwt_env <- get_lengthweight(
    con_akfin = con_akfin,
    con_afsc  = con_afsc,
    species   = fsh_species,
    area      = area
  )

  # ---- 7) Fishery catch-weighted length comps ----
  fsh_lcomp <- NULL
  if (isTRUE(do_fsh_lcomp)) {
    fsh_lcomp <- LENGTH_BY_CATCH_short(
      con_akfin          = con_akfin,
      con_afsc           = con_afsc,
      species            = fsh_species,
      species_catch      = fsh_species_catch,
      for_species_catch  = for_species_catch,
      sp_area            = sp_area,
      ly                 = ly,
      SEX                = fsh_lcomp_sex,
      PORT               = fsh_lcomp_port
    )
  }

  # ---- 8) Fishery EWAA (WTAGE/NAGE) ----
  fsh_ewaa <- NULL
  if (isTRUE(do_fsh_ewaa)) {
    fsh_ewaa <- fishery_ewaa(
      con        = con_akfin,
      species    = fsh_species,
      region     = c("AI","BS","GOA","BSWGOA"),  # you can pass numeric NMFS areas here too
      maxage     = max_age,
      split_sex  = fsh_ewaa_split_sex
    )
  }

  # ---- 9) LL RPN CPUE ----
  LL_CPUE <- NULL
  if (isTRUE(do_LL)) {
    LL_CPUE <- build_ll_cpue(
      con          = con_akfin,
      ly           = ly,
      srv_sp_str   = srv_sp_str,
      sp_area      = sp_area,
      LL_sp_region = LL_sp_region,
      LLsrv_start_yr = LLsrv_start_yr,
      fleet        = LL_fleet,
      seas         = LL_seas
    )
  }

  # ---- return ----
  out <- list(
    meta = list(
      ly = ly,
      area = area,
      sp_area = sp_area,
      srv_sp_str = srv_sp_str,
      fsh_species = fsh_species,
      one_fleet = one_fleet,
      do_LL = do_LL,
      LL_fleet = LL_fleet
    ),
    # fishery
    catch      = catch_res$catch,
    catch_raw  = catch_res$catch_raw,
    N_catch    = catch_res$N_catch,
    fsh_lcomp  = fsh_lcomp,
    fsh_ewaa   = fsh_ewaa,
    # survey
    surv_biom  = surv_biom,
    surv_lcomp = surv_lcomp,
    surv_acomp = surv_acomp,
    surv_age_specimens = surv_age_specimens,
    # env / aux
    lenwt_env  = lenwt_env,
    # indices
    LL_CPUE    = LL_CPUE
  )

  return(out)
}
