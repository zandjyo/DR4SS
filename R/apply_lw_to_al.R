#' Apply a length–weight key to empirical length-at-age to get quarter WAA
#'
#' @param al_emp output of build_empirical_length_at_age()
#' @param lw_fit output of fit_lw_key()
#' @param gear_mode "combined" or "by_gear"
#' @param sex_mode "combined" or "split"
#'
#' @return data.table with YEAR, REGION_GRP,SEASON, AGE_YRS, (GEAR2), (SEX), WAA, N_AL
#' @export
apply_lw_to_al <- function(al_emp,
                           lw_fit,
                           gear_mode = c("combined","by_gear"),
                           sex_mode  = c("combined","split"),
                           model_length = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) stop("Need data.table.", call. = FALSE)
  
  al_emp$YEAR<-as.factor(al_emp$YEAR)
  al_emp$SEASON<-as.factor(al_emp$SEASON)
  al_emp$REGION_GRP<-as.factor(al_emp$REGION_GRP)
  al_emp$GEAR2<-as.factor(al_emp$GEAR2)

  gear_mode <- match.arg(gear_mode)
  sex_mode  <- match.arg(sex_mode)

  # if(model_length){

  #    al_emp[,"L_BAR" := NULL]
  #    data.table::setnames(al_emp,old="L_QSTAR", new="L_BAR")
  #  }


  AL <- data.table::as.data.table(al_emp)
  names(AL) <- toupper(names(AL))

  FITDATA <- data.table::as.data.table(lw_fit$data_used)

  need <- c("YEAR","AGE_YRS","BIN_ID","L_BAR","P_LEN","N")
  miss <- setdiff(need, names(AL))
  if (length(miss) > 0) stop("al_emp missing: ", paste(miss, collapse = ", "), call. = FALSE)

  # strata keys for AL
  strata_keys <- c("YEAR")
  if (gear_mode == "by_gear") strata_keys <- c(strata_keys, "GEAR2")
  if (sex_mode  == "split")  strata_keys <- c(strata_keys, "SEX")

  # if LW model kept AREA, it MUST be present in AL and included in prediction keys
  kept <- lw_fit$kept_terms
  
  if ("AREA" %in% kept) {
    if (!"AREA" %in% names(AL)) stop("LW model kept AREA but AL table has no AREA.", call. = FALSE)
    strata_keys <- c(strata_keys, "AREA")
  }

 if ("REGION_GRP" %in% kept) {
    if (!"REGION_GRP" %in% names(AL)) stop("LW model kept REGION_GRP but AL table has no REGION_GRP.", call. = FALSE)
    strata_keys <- c(strata_keys, "REGION_GRP")
  }

  if ("SEASON" %in% kept) {
    if (!"SEASON" %in% names(AL)) stop("LW model kept SEASON but AL table has no SEASON.", call. = FALSE)
    strata_keys <- c(strata_keys, "SEASON")
  }


# Ensure AL has BIN_ID, and ensure it's integer
AL[, BIN_ID := as.integer(BIN_ID)]

# Keys that define a bin-specific LW prediction
pred_keys <- c("YEAR","BIN_ID")
if (gear_mode == "by_gear") pred_keys <- c(pred_keys, "GEAR2")
if (sex_mode  == "split")  pred_keys <- c(pred_keys, "SEX")
if ("AREA" %in% lw_fit$kept_terms) pred_keys <- c(pred_keys, "AREA")
if ("REGION_GRP" %in% lw_fit$kept_terms) pred_keys <- c(pred_keys, "REGION_GRP")
if ("SEASON" %in% lw_fit$kept_terms) pred_keys <- c(pred_keys, "SEASON")

# Build a UNIQUE bin-level table for prediction
pred_dt <- AL[, .(L_BAR = mean(L_BAR, na.rm = TRUE)), by = pred_keys]

# model expects LENGTH
pred_dt[, LENGTH := as.numeric(L_BAR)]

# Align factor levels only for factor kept terms
FITDATA <- data.table::as.data.table(lw_fit$data_used)
for (v in lw_fit$kept_terms) {
  if (!v %in% names(pred_dt)) {
    stop("LW model kept term '", v, "' not present in AL bins.", call. = FALSE)
  }
  if (is.factor(FITDATA[[v]])) {
    pred_dt[, (v) := factor(get(v), levels = levels(FITDATA[[v]]))]
  }
}

# Drop unseen factor levels
for (v in lw_fit$kept_terms) {
  if (is.factor(pred_dt[[v]]) && anyNA(pred_dt[[v]])) {
    pred_dt <- pred_dt[!is.na(get(v))]
  }
}
if (nrow(pred_dt) == 0) stop("No rows remain for LW prediction after level alignment.", call. = FALSE)

# Predict once per bin key
pred_dt[, W_HAT := stats::predict(lw_fit$model, newdata = pred_dt, type = "response")]

# GUARANTEE uniqueness on keys (this should be TRUE now)
chk <- pred_dt[, .N, by = pred_keys][N > 1]
if (nrow(chk) > 0) {
  stop("pred_dt has duplicate key rows. Example:\n",
       paste(capture.output(print(head(chk, 10))), collapse = "\n"),
       call. = FALSE)
}

# Merge back by the explicit keys only
AL2 <- merge(
  AL,
  pred_dt[, c(pred_keys, "W_HAT"), with = FALSE],
  by = pred_keys,
  all.x = TRUE,
  allow.cartesian = FALSE
)


  # Now integrate over bins
  by_cols <- c("YEAR","AGE_YRS")
  if (gear_mode == "by_gear") by_cols <- c(by_cols, "GEAR2")
  if (sex_mode  == "split")  by_cols <- c(by_cols, "SEX")
  if ("AREA" %in% kept)      by_cols <- c(by_cols, "AREA")
  if ("REGION_GRP" %in% kept) by_cols <- c(by_cols, "REGION_GRP")
  if ("SEASON" %in% kept)     by_cols <- c(by_cols, "SEASON")

  # sanity: p_len sums to 1
  chk <- AL2[, .(p_sum = sum(P_LEN, na.rm = TRUE)), by = by_cols]
  if (any(abs(chk$p_sum - 1) > 1e-6)) {
    bad <- chk[abs(p_sum - 1) > 1e-6][1:min(.N, 10)]
    stop("P_LEN does not sum to 1 within some strata. Example:\n",
         paste(capture.output(print(bad)), collapse = "\n"),
         call. = FALSE)
  }

  out <- AL2[, .(
    WAA  = sum(P_LEN * W_HAT, na.rm = TRUE),
    N_AL = sum(N, na.rm = TRUE)
  ), by = by_cols]

  data.table::setorder(out, REGION_GRP, YEAR, SEASON, AGE_YRS)
  out
}
