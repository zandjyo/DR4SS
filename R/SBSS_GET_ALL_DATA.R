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
#' - Fishery EWAA is built via (optionally split by sex).
#' - Fishery catch-weighted lcomp is built via .
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
#' @param do_fsh_ewaa Logical; if TRUE run fishery_ewaa().
#' @param fsh_ewaa_split_sex Logical; if TRUE, fishery_ewaa_catch_weighted() fits/predicts split by sex and
#'   outputs F/M tables.
#'
#' @param do_fsh_lcomp Logical; if TRUE run LENGTH_BY_CATCH().
#' @param fsh_lcomp_sex Logical; passed to LENGTH_BY_CATCH(SEX=...).
#' @param fsh_lcomp_port Logical; passed to LENGTH_BY_CATCH(PORT=...).
#' @param fsh_species_catch Species code used by fishery catch/length SQL (can differ from survey).
#' @param for_species_catch Optional “foreign catch species” code used by LENGTH_BY_CATCH.
#'
#' @return Named list of SS inputs (and intermediate objects) suitable for writing into a
#' .dat file pipeline.
#'
#' @export
SBSS_GET_ALL_DATA <- function(
  con_akfin = conn$akfin,
  con_afsc = conn$afsc,
  srv_sp_str = 21720,
  fsh_species = 202,
  fsh_sp_label = 'PCOD',
  fsh_species_catch = 'PACIFIC COD',
  ly = 2025,
  # ---- Area definitions
  area = 'BS',
  sp_area = 'BSAI',
  fsh_sp_area = 'BS',
  LL_sp_region = 'Bering Sea',
  
  # ---- survey indices----
  srv_start_yr = 1977,
  do_LL = FALSE,
  LLsrv_start_yr = 1990,
  srv_flt = 2,
  LL_fleet = 3,
  LL_seas = 7,
  srv_seas = 7,
  use_vast=TRUE,
  VAST_abundance = VAST_abundance,
  
  # ---- survey comps ----
  len_bins = seq(4.5, 119.5, 5),
  max_age = 12,
  srv_biom =FALSE,   
  vast_agecomp = VAST_AGECOMP,
    
  # ---- Catch ----
  one_fleet = TRUE,
  catch_se = 0.01,
  init_catch = 42500,
  old_catch = OLD_SEAS_GEAR_CATCH,
      
  # ---- fishery EWAA ----
  do_fsh_ewaa = TRUE,
  ewaa_split_sex = FALSE,
  ewaa_ymin = 2007,
  standard_quarter = FALSE,
  target_qtr = 2L,

  # ---- fishery catch-weighted lcomp ----
  do_fsh_lcomp = TRUE,
  fsh_lcomp_sex = TRUE,
  fsh_lcomp_port = TRUE,
  use_foriegn = TRUE,
  for_species_catch = NULL,
  
  #-- conditional LAA
  do_fsh_CLAA = TRUE,
  do_srv_CLAA = TRUE,
  # ---- environmental LW coefficients
  env_LW=FALSE
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
    fsh_sp_area         = area,
    one_fleet           = one_fleet,
    old_seas_gear_catch = if (!is.null(old_obj) && nrow(old_obj) > 0) old_obj else NULL,
    catch_total         = NULL,
    catch_se            = catch_se,
    init_catch          = init_catch
  )

print("Fishery catch done")
  # ---- 2) Survey biomass ----

  if(!use_vast){
  surv_biom <- GET_SURVEY_BIOM(
    con_akfin = con_akfin,
    area      = area,
    species   = srv_sp_str,
    start_yr  = srv_start_yr
  )

  if(srv_biom){
    CPUE<-data.frame(year=surv_biom$YEAR,seas=srv_seas,index=srv_flt,obs=surv_biom$BIOMASS,se_log=sqrt(surv_biom$VARBIO)/surv_biom$BIOMASS)
    } else { CPUE<-data.frame(year=surv_biom$YEAR,seas=srv_seas,index=srv_flt,obs=surv_biom$POPULATION,se_log=sqrt(surv_biom$VARPOP)/surv_biom$POPULATION)}

 } else{
 
    VAST_abundance<-data.table(VAST_abundance)[Fleet=="Both"]
    BIOM<-VAST_abundance
    names(BIOM)<-c("YEAR","FLEET","Unit","BIOM","SD_mt","se_log")
   CPUE<-data.frame(year=BIOM$YEAR,seas=srv_seas,index=srv_flt,obs=BIOM$BIOM,se_log=BIOM$se_log)
 }

 ## check to see if there is a missing years and replace it with zeros and a negative value for fleet

 all_years<-c(srv_start_yr:ly)

 # find missing years
 missing_years <- setdiff(all_years, CPUE$year)


 
CPUE <- data.table::rbindlist(
  list(
      CPUE,
      data.table::data.table(
        year   = missing_years,
        seas   = srv_seas,
        index  = -srv_flt,
        obs    = 0,
        se_log = 0
      )
    ),
    use.names = TRUE,
    fill = TRUE
  )


# sort for cleanliness
data.table::setorder(CPUE, year)

# ---- 3) LL RPN CPUE ----
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

  CPUE<-rbind(CPUE,LL_CPUE)

  print("CPUE done")



  # ---- 4) Survey length comps ----
  surv_lcomp <- GET_SURVEY_LCOMP(
    con_akfin = con_akfin,
    species   = srv_sp_str,
    bins      = len_bins,
    bin       = TRUE,
    area      = area,
    sex       = 1,
    SS        = TRUE,
    flt = srv_flt
  )
  surv_lcomp<-data.table::data.table(surv_lacomp)

  names(surv_lcomp)<-c("Year","Seas","FltSrv","Gender","Part","Nsamp",len_bins)

  print("Survey LCOMP done")

# ---- 5) Fishery catch-weighted length comps ----
  fsh_lcomp <- NULL
  if (isTRUE(do_fsh_lcomp)) {
    fsh_lcomp <- LENGTH_BY_CATCH_short(
      con_akfin          = con_akfin,
      con_afsc           = con_afsc,
      species            = fsh_species,
      species_catch      = fsh_species_catch,
      for_species_catch  = for_species_catch,
      sp_area            = area,
      ly                 = ly,
      SEX                = fsh_lcomp_sex,
      PORT               = fsh_lcomp_port,
      use_foreign        = use_foreign
    )
  }

if(one_fleet){
    FSH_LCOMP<-fsh_lcomp[[1]]
    Nsamp<-FSH_LCOMP[,list(nsamp=max(NHAUL)),by=c("YEAR")]

    lcomp<-BIN_LEN_DATA(data=FSH_LCOMP,len_bins=len_bins)
    lcomp<-aggregate(list(TOTAL=lcomp$FREQ),by=list(BIN=lcomp$BIN,YEAR=lcomp$YEAR),FUN=sum)

    N_TOTAL <- aggregate(list(T_NUMBER=lcomp$TOTAL),by=list(YEAR=lcomp$YEAR),FUN=sum)
    lcomp <- merge(lcomp,N_TOTAL)
    lcomp$TOTAL <- lcomp$TOTAL / lcomp$T_NUMBER

    years<-unique(lcomp$YEAR)

    nbin=length(len_bins)

    nyr<-length(years)

    x<-matrix(ncol=((nbin)+6),nrow=nyr)
    x[,2]<-1
    x[,3]<-1
    x[,4]<-0
    x[,5]<-0
    x[,6]<-round(Nsamp$nsamp/(mean(Nsamp$nsamp)/mean(surv_lcomp$Nsamp)))  ## scale number of hauls to survey number of stations...

    for(i in 1:nyr)
        {
            x[i,1]<-years[i]
            x[i,7:(nbin+6)]<-lcomp$TOTAL[lcomp$YEAR==years[i]]
        }
        FISHLCOMP<-data.frame(x)

      names(FISHLCOMP) <- c("Year","Seas","FltSrv","Gender","Part","Nsamp",len_bins)
    } else{
      FSH_LCOMP<-fsh_lcomp[[2]]
    
      Nsamp<-data.table::data.table(FSH_LCOMP[,list(nsamp=max(NHAUL)),by=c("GEAR","YEAR")])

      lcomp<-BIN_LEN_DATA(data=FSH_LCOMP,len_bins=len_bins)

      lcomp<-aggregate(list(TOTAL=lcomp$FREQ),by=list(BIN=lcomp$BIN,GEAR=lcomp$GEAR,YEAR=lcomp$YEAR),FUN=sum)

      N_TOTAL <- aggregate(list(T_NUMBER=lcomp$TOTAL),by=list(GEAR=lcomp$GEAR,YEAR=lcomp$YEAR),FUN=sum)

      lcomp <- data.table::data.table(merge(lcomp,N_TOTAL))
      lcomp$TOTAL <- lcomp$TOTAL / lcomp$T_NUMBER

      lcomp$GEAR1<-1
      lcomp[GEAR=='HAL']$GEAR1<-2
      lcomp[GEAR=='POT']$GEAR1<-3

      Nsamp$GEAR1<-1
      Nsamp[GEAR=='HAL']$GEAR1<-2
      Nsamp[GEAR=='POT']$GEAR1<-3  

   
      gear  <- unique(lcomp$GEAR1)
      nbin  <- length(len_bins)
      ngear <- length(gear)

      y <- vector("list")
      
      for(j in 1:ngear){
        lcomp1<-lcomp[GEAR1==j]
        years <- unique(lcomp1$YEAR)
        nyr   <- length(years)

        x<-matrix(ncol=((nbin)+6),nrow=nyr)
        x[,2]<-1
        x[,4]<-0
        x[,5]<-0
        x[,6]<-round(Nsamp[GEAR1==j]$nsamp/(mean(Nsamp[GEAR1==j]$nsamp)/mean(surv_lcomp$Nsamp)))
           
      for(i in 1:nyr)
          {
            x[i,1]<-years[i]
            x[i,3]<-j
            x[i,7:(nbin+6)]<-lcomp1$TOTAL[lcomp1$YEAR==years[i]]
          }
         y[[j]]<-x
      }
      
      y=do.call(rbind,y)  
      FISHLCOMP<-data.frame(y)
      names(FISHLCOMP) <- c("Year","Seas","FltSrv","Gender","Part","Nsamp",len_bins)
      FISHLCOMP <- subset(FISHLCOMP,!is.na(data.table(FISHLCOMP)[,7]))
  
  }

 print("Fisheries LCOMP done")

 ## combine all the length comp data
  LCOMP<-rbind(FISHLCOMP,surv_lcomp)
  LCOMP<-subset(LCOMP,!is.na(LCOMP['119.5']))

  # ---- 6) Survey age comps ----
  vast_obj <- if (exists("vast_agecomp", inherits = TRUE)) get("vast_agecomp", inherits = TRUE) else NULL

  acomp <- GET_SURVEY_ACOMP(
    con_akfin  = con_akfin,
    use_vast   = use_vast,
    vast_agecomp = if (!is.null(vast_obj) && nrow(vast_obj) > 0) vast_obj else NULL,
    species    = srv_sp_str,
    start_yr   = srv_start_yr,
    area       = area,
    max_age    = max_age,
    flt = srv_flt,
    seas= srv_seas,
    gender = 1,
    part = 0,
    ageerr = 0,
    lgin_lo = 1,
    lgin_hi = round(max(len_bins))
  )

  # ---- 5) Survey specimen ages (if you need the specimen table downstream) ----
  surv_age_specimens <- GET_SURV_AGE_cor(
    con_akfin = con_akfin,
    area      = area, # NOTE: this function uses your "BS-NBS" style default
    species   = srv_sp_str,
    start_yr  = srv_start_yr,
    max_age   = max_age    
  )

# ---- 6) Conditional length at age
  if(isTRUE(srv_CLAA)){
    srv_CLAA<-cond_length_age_cor(con_akfin=con_akfin,
                                species = srv_sp_str,
                                area = sp_area,
                                start_year= srv_start_yr,
                                max_age = max_age,
                                len_bins = len_bins,
                                wt = 1,
                                seas = srv_seas,
                                flt = srv_flt,
                                ageerr = 1)
    acomp=rbind(acomp,srv_CLAA)
    }

  if(isTRUE(fsh_CLAA)){

    fsh_CLAA<-cond_length_age_corFISH(
                                  con=con_akfin,
                                  species = fsh_sp_str,
                                  area = fsh_sp_area,
                                  max_age1 = max_age,
                                  len_bins1 = len_bins,
                                  one_fleet = one_fleet,
                                  wt = 1,
                                  seas =1,
                                  ageerr=1)
    acomp<-rbind(acomp,fsh_CLAA)
    }



 





  # ---- 7) Length–weight env series (if used in SS env block) alpha and beta on growth----
  if(env_LW){
  lenwt_env <- get_lengthweight(
    con_akfin = con_akfin,
    con_afsc  = con_afsc,
    species   = fsh_species,
    area      = area
  )
 }
  

  # ---- 8) Fishery and survey EWAA (WTAGE/NAGE) ----
  fsh_ewaa <- NULL
  srv_ewaa <- NULL
  if (isTRUE(do_ewaa)) {
    fsh_ewaa<-fishery_ewaa_catch_weighted(
      con_akfin = con_akfin,
      species   = fsh_species,
      species_group = fsh_sp_label,
      region        = area,
      len_bins      = len_bins,
      maxage = max_age,
      gear_mode = c("combined", "by_gear"),
      sex_mode  = c("combined", "split"),
      trim_outliers = TRUE,
      trim_q = c(0.02, 0.98),
      min_strata_n = 1,
      year_min = ewaa_ymin,
      catch_subarea = area,
      catch_year_max = ly,
      standard_quarter = standard_quarter,
      target_qtr = qtr
)
    
   srv_ewaa <- survey_ewaa(
      con = con_akfin,
      species = srv_sp_str,
      region  = area,
      maxage  = max_age,
      split_sex = ewaa_split_sex,
      year_min=ewaa_ymin
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