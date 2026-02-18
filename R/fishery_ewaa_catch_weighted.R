#' Catch-weighted empirical weight-at-age (EWAA) for fishery data
#'
#' Builds Stock Synthesis-style WTAGE and NAGE tables using fishery biology
#' (age-length-weight) and quarterly blend catch. The workflow is:
#' \enumerate{
#'   \item Pull fishery biological samples (age, length, weight, sex, gear, area, date).
#'   \item Pull quarterly blend catch and compute within-year quarter proportions (\eqn{p_q}).
#'   \item Build an empirical age-length distribution (length bins by age) from the fishery samples.
#'   \item Fit a length-weight (LW) key and apply it to the empirical age-length distribution to
#'         obtain quarter-specific weight-at-age.
#'   \item Optionally "standardize" to a target quarter by translating lengths to that quarter
#'         using a delta-length (quarter growth) model before applying the LW key.
#'   \item Collapse quarter-specific weight-at-age to annual values using blend catch quarter weights.
#'   \item Convert annual weight-at-age and sample counts into SS WTAGE/NAGE matrices and fill
#'         missing ages safely.
#' }
#'
#' The function supports both aggregation choices:
#' \itemize{
#'   \item Gear: combined across gears, or by gear (Fleet mapping: 1=combined, 2=gear2, 3=gear3).
#'   \item Sex: combined (gender=0) or split (F=1, M=2 in SS convention).
#' }
#'
#' @param con_akfin A DBI connection to AKFIN (or equivalent schema containing the blend catch
#'   and fishery biological tables queried by the helper functions).
#' @param species Numeric fishery species code used by the fishery biology SQL/data pull.
#' @param species_group Character species code used by the blend catch query
#'   (e.g., "PCOD" if blend catch uses a group label rather than numeric code).
#' @param region Region selector passed to `get_fishery_age_wt_data()` (e.g., "BS", "GOA", etc.).
#' @param len_bins Numeric vector of length bin edges used to build the empirical length-at-age.
#' @param maxage Plus-group age; ages > maxage are pooled to maxage.
#' @param gear_mode Either "combined" or "by_gear".
#' @param sex_mode Either "combined" or "split".
#' @param trim_outliers Logical; if TRUE, trim extreme weight observations before fitting the LW key.
#' @param trim_q Numeric length-2 vector of quantiles used when trimming weights (e.g., c(0.01, 0.99)).
#' @param min_strata_n Minimum sample size required to retain a model stratum when fitting (e.g., by gear/area/quarter).
#' @param year_min Minimum fishery YEAR to include in biology pulls.
#' @param catch_subarea Subarea code(s) passed to the blend catch query (character, e.g., "BS").
#' @param catch_year_max Maximum year (inclusive) to include in blend catch.
#' @param standard_season Logical. If TRUE, translate lengths to `target_qtr` using a delta-length
#'   growth model before applying the LW key. This yields a WTAGE that represents fish weights as-if
#'   they were observed in the target quarter.
#' @param target_qtr Integer 1..4. Target quarter used when `standard_quarter = TRUE`.
#'
#' @return A list in the format returned by `make_ss_wtage_nage()`:
#'   \itemize{
#'     \item If `sex_mode="combined"`: list(WTAGE=..., NAGE=...)
#'     \item If `sex_mode="split"`: list(F=list(WTAGE=..., NAGE=...), M=list(WTAGE=..., NAGE=...))
#'   }
#'
#' @seealso \code{\link{get_fishery_age_wt_data}}, \code{\link{get_blend_catch}},
#'   \code{\link{build_empirical_length_at_age}}, \code{\link{fit_lw_key}},
#'   \code{\link{fit_quarter_growth_model}}, \code{\link{translate_lengths_to_quarter}},
#'   \code{\link{apply_lw_to_al}}, \code{\link{collapse_catch_weighted_wtaa}},
#'   \code{\link{make_ss_wtage_nage}}
#'
#' @export
fishery_ewaa_catch_weighted <- function(
  con_akfin,
  species = 202,
  species_group = "PCOD",
  region_def,
  season_def,
  len_bins,
  maxage = 12,
  gear_mode = c("combined", "by_gear"),
  sex_mode  = c("combined", "split"),
  trim_outliers = TRUE,
  trim_q = c(0.01, 0.99),
  min_strata_n = 1,
  start_year,
  end_year,
  catch_subarea,
  catch_year_max,
  standard_season = TRUE,
  target_season = "B"
) {

  gear_mode <- match.arg(gear_mode)
  sex_mode  <- match.arg(sex_mode)

  # ---- 1) Pull fishery biological samples (age/length/weight) ----
  # get_fishery_age_wt_data() should return fish-level records with at least:
  # YEAR, MONTH/QUARTER, LENGTH, (AGE_YRS or AGE), WEIGHT, SEX, GEAR2, AREA, etc.
  #fish_raw <- get_fishery_age_wt_data(
  #  con      = con_akfin,
  #  species  = species,
  #  region   = region,
  #  year_min = year_min
  #)

  fish_raw <- get_fishery_age_wt_data(con=con_akfin,
                                    species=species,
                                    season_def=season_def,
                                    region_def=region_def,
                                    start_year = start_year,
                                    end_year = end_year,
                                    max_wt=50,
                                    wgoa_cod=TRUE,
                                    drop_unmapped=TRUE)




  # ---- 2) Pull quarterly blend catch and compute within-year quarter proportions ----
  # Blend catch typically contains total catch by YEAR x (MONTH or QUARTER) x GEAR x AREA.
  # get_blend_catch() should return a tidy table sufficient for build_catch_qtr_from_blend().
  blend <- get_blend_catch_region(con_akfin = con_akfin,
                            species_group = species_group,
                            region_def = region_def,
                            season_def = season_def,
                            start_year = start_year,
                            end_year = end_year,
                            wgoa_cod = TRUE)

                           

  # Convert blend catch to seasonal totals and within-year proportions p_q.
  # If gear_mode == "by_gear", build_catch_season_from_blend() should keep a GEAR2 column
  # so later weighting can occur within gear.
  
  catch_qtr <- build_catch_season_from_blend(
    blend   = blend,
    by_gear = (gear_mode == "by_gear")
  )
  # catch_qtr columns should look like:
  # YEAR, QUARTER, (GEAR2), CATCH, p_q

  # ---- 3) Build empirical length-at-age (AL) from fishery samples ----
  # This produces a *distribution* of lengths by YEAR x QUARTER x AGE (and optionally gear/sex),
  # typically with columns including:
  # YEAR, QUARTER, AGE_YRS, BIN_ID, L_BAR, P_LEN, N_AL, (GEAR2), (SEX)
  al <- build_empirical_length_at_age(
    fish_data = fish_raw,
    len_bins  = len_bins,
    maxage    = maxage,
    gear_mode = gear_mode,
    sex_mode  = sex_mode,
    min_strata_n = 1
  )

  # ---- 4) Optional: standardize lengths to a target quarter using delta-length growth ----
  # Idea:
  # - Fish grow between quarters, so a fish observed at length L in quarter q is expected to be
  #   length L + ΔL(age, q -> target_qtr) in the target quarter.
  # - We estimate ΔL from the fishery age-length data by comparing mean length-at-age in q vs target_qtr.
  # - Then we translate the AL bin representative length (L_BAR) to L_QSTAR, and overwrite L_BAR
  #   so downstream LW predictions treat bin lengths as if they were in the target quarter.
  if (isTRUE(standard_quarter)) {

    growth_fit <- fit_seasonal_growth_model(
      fish_al      = fish_raw,
      target_qtr   = as.integer(target_season),
      maxage       = as.integer(maxage),
      gear_mode    = gear_mode,
      sex_mode     = sex_mode,
      min_n_cell   = 1L,
      shrink_k     = 30
    )

    al <- translate_lengths_to_season(
      al_emp     = al,
      growth_fit = growth_fit,
      clamp      = TRUE,
      min_L      = NULL,
      max_L      = NULL
    )

    # Replace original bin length with translated bin length so WL is evaluated at target-quarter length
    
    if ("L_BAR" %in% names(al)) {al[, L_BAR := NULL]
    data.table::setnames(al, "L_QSTAR", "L_BAR", skip_absent=TRUE)}
    
    }

  # ---- 5) Fit length-weight key and apply it to AL to get quarter-specific WAA ----
  # LW key uses fish-level LENGTH+WEIGHT (and optional strata terms).
  # apply_lw_to_al() should:
  # - predict weight for each AL bin length (L_BAR)
  # - then integrate across bins using P_LEN to get WAA by YEAR x QUARTER x AGE (and gear/sex modes)
  lw=fit_lw_key(fish_data=fish_raw,
                       trim_outliers = TRUE,
                       trim_q = trim_q,
                       min_strata_n = min_strata_n,
                       allow_year = TRUE,
                       allow_season = length(season_def)>1,
                       allow_gear = (gear_mode != "combined"),
                       allow_region = (length(region_def) < 1 ),
                       allow_area = (length(region_def)==1),
                       allow_sex = (sex_mode  != "combined"))



  waa_q <- apply_lw_to_al(
    al_emp       = al,
    lw_fit       = lw,
    gear_mode    = gear_mode,
    sex_mode     = sex_mode,
    model_length = isTRUE(standard_season)  # if your apply_lw_to_al uses this flag
  )

  # Keep a copy suitable for downstream collapse
  # Expected columns in waa_q at minimum:
  # YEAR, QUARTER, AGE_YRS, WAA, N_AL, (GEAR2), (SEX)
  pred_like <- data.table::copy(waa_q)
  data.table::setnames(pred_like, "WAA", "WEIGHT_KG")

  # ---- 6) Collapse quarter-specific WAA to annual WAA using catch quarter weights p_q ----
  # For each YEAR (and optionally by gear/sex), compute:
  #   annual_WAA(age) = sum_q [ p_q * WAA_q(age) ]
  out <- collapse_catch_weighted_wtaa(
    pred      = pred_like,
    catch_seas = catch_qtr,
    gear_mode = gear_mode,
    sex_mode  = sex_mode
  )
  # out is expected to be long with columns YEAR, AGE_YRS, WAA (+ optional GEAR2/SEX)

  # ---- 7) Convert to SS WTAGE and NAGE matrices ----
  # make_ss_wtage_nage() will:
  # - cast out_long to wide a0..amax
  # - build NAGE from the aged sample size information (N_AL) (recommended)
  # - fill missing WTAGE cells with pooled reference curves to satisfy SS requirements
  ss <- make_ss_wtage_nage(
    out_long  = out,
    aged_pred = pred_like,  # provides N_AL by YEAR/AGE (and gear/sex if applicable)
    maxage    = maxage,
    gear_mode = gear_mode,
    sex_mode  = sex_mode
  )

  ss
}
