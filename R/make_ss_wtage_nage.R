#' Make SS WTAGE and NAGE with safe filling for missing ages
#'
#' Fills missing WTAGE cells (where NAGE==0 or WTAGE is NA) using a pooled
#' reference curve by Fleet x gender, then optionally smooths internal gaps.
#' Ensures all a0..amax are filled for Stock Synthesis.
#'
#' @param out_long Long WAA output (YEAR, AGE_YRS, WAA, plus optional GEAR2/SEX).
#' @param aged_pred Row-level aged data to build NAGE (YEAR, AGE_YRS, optional GEAR2/SEX).
#' @param maxage Plus-group age.
#' @param gear_mode "combined" or "by_gear".
#' @param sex_mode "combined" or "split".
#' @param seas,GP,bseas SS header values.
#' @param fill_policy One of c("pooled_median","pooled_smooth"). Default pooled_median.
#' @param internal_interp Logical; if TRUE, linearly interpolate internal gaps within-year.
#'
#' @return WTAGE/NAGE lists (split by sex if requested)
#' @export
make_ss_wtage_nage <- function(out_long,
                               aged_pred,
                               maxage = 10,
                               gear_mode = c("combined","by_gear"),
                               sex_mode  = c("combined","split"),
                               seas = 1,
                               GP = 1,
                               bseas = 1,
                               fill_policy = c("pooled_median", "pooled_smooth"),
                               internal_interp = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  fill_policy <- match.arg(fill_policy)
  gear_mode <- match.arg(gear_mode)
  sex_mode  <- match.arg(sex_mode)

  DT <- data.table::as.data.table
  W <- DT(out_long); A <- DT(aged_pred)
  names(W) <- toupper(names(W)); names(A) <- toupper(names(A))

  need_w <- c("YEAR","AGE_YRS","WAA")
  if (length(setdiff(need_w, names(W))) > 0) stop("out_long missing YEAR/AGE_YRS/WAA", call. = FALSE)
  need_a <- c("YEAR","AGE_YRS")
  if (length(setdiff(need_a, names(A))) > 0) stop("aged_pred missing YEAR/AGE_YRS", call. = FALSE)

  suppressWarnings({
    W[, YEAR := as.integer(as.character(YEAR))]
    W[, AGE_YRS := as.integer(as.character(AGE_YRS))]
    W[, WAA := as.numeric(WAA)]
    A[, YEAR := as.integer(as.character(YEAR))]
    A[, AGE_YRS := as.integer(as.character(AGE_YRS))]
  })

  W[AGE_YRS > maxage, AGE_YRS := maxage]
  A[AGE_YRS > maxage, AGE_YRS := maxage]

  # modes
  if (gear_mode == "by_gear") {
    if (!"GEAR2" %in% names(W) || !"GEAR2" %in% names(A)) stop("by_gear requires GEAR2 in both inputs.", call. = FALSE)
    W[, GEAR2 := as.integer(as.character(GEAR2))]
    A[, GEAR2 := as.integer(as.character(GEAR2))]
  } else {
    if ("GEAR2" %in% names(W)) W[, GEAR2 := NULL]
    if ("GEAR2" %in% names(A)) A[, GEAR2 := NULL]
  }

  if (sex_mode == "split") {
    if (!"SEX" %in% names(W) || !"SEX" %in% names(A)) stop("split requires SEX in both inputs.", call. = FALSE)
    W[, SEX := toupper(trimws(as.character(SEX)))]
    A[, SEX := toupper(trimws(as.character(SEX)))]
    W <- W[SEX %in% c("F","M")]
    A <- A[SEX %in% c("F","M")]
  } else {
    if ("SEX" %in% names(W)) W[, SEX := NULL]
    if ("SEX" %in% names(A)) A[, SEX := NULL]
  }

  age_levels <- 0:maxage
  age_cols <- paste0("a", age_levels)

  # ---- helpers ----
  sex_to_gender <- function(sex_chr) ifelse(sex_chr == "F", 1L, 2L)

  wide_from_long <- function(long_dt, value_col, by_cols) {
    tmp <- long_dt[, .(val = get(value_col)), by = c(by_cols, "AGE_YRS")]
    tmp[, AGE_YRS := pmin(pmax(AGE_YRS, 0L), maxage)]
    tmp[, age_name := paste0("a", AGE_YRS)]
    w <- data.table::dcast(tmp, stats::as.formula(paste(paste(by_cols, collapse=" + "), "~ age_name")),
                           value.var = "val")
    for (ac in age_cols) if (!ac %in% names(w)) w[, (ac) := NA_real_]
    data.table::setcolorder(w, c(by_cols, age_cols))
    w
  }

  add_headers <- function(d, fleet_vec, gender_vec) {
    d[, `:=`(seas = as.integer(seas),
             Fleet = as.integer(fleet_vec),
             gender = as.integer(gender_vec),
             GP = as.integer(GP),
             bseas = as.integer(bseas))]
    d
  }

  # ---- build WTAGE wide ----
  by_keys <- c("YEAR")
  if (gear_mode == "by_gear") by_keys <- c(by_keys, "GEAR2")
  if (sex_mode  == "split")  by_keys <- c(by_keys, "SEX")

  WT_wide <- wide_from_long(W[!is.na(WAA)], "WAA", by_keys)

  # ---- build NAGE wide from counts ----
  #N_long <- A[, .(COUNT = .N), by = c(by_keys, "AGE_YRS")]
  N_long <- A[, .(COUNT = sum(N_AL)), by = c(by_keys, "AGE_YRS")]

  NG_wide <- wide_from_long(N_long, "COUNT", by_keys)
  for (ac in age_cols) NG_wide[is.na(get(ac)), (ac) := 0L]
  for (ac in age_cols) data.table::set(NG_wide, j = ac, value = as.integer(NG_wide[[ac]]))

  # ---- filling strategy: pooled reference curve by Fleet x gender ----
  # Create SS-style Fleet/gender fields first (but keep YEAR for pooling)
  prep_ss_block <- function(WT, NG) {
    if (gear_mode == "combined") {
      fleet <- 1L
      if ("GEAR2" %in% names(WT)) WT[, GEAR2 := NULL]
      if ("GEAR2" %in% names(NG)) NG[, GEAR2 := NULL]
    } else {
      fleet <- WT$GEAR2
      WT[, GEAR2 := NULL]
      NG[, GEAR2 := NULL]
    }

    if (sex_mode == "combined") {
      gender <- 0L
      if ("SEX" %in% names(WT)) WT[, SEX := NULL]
      if ("SEX" %in% names(NG)) NG[, SEX := NULL]
    } else {
      gender <- sex_to_gender(WT$SEX)
      WT[, SEX := NULL]
      NG[, SEX := NULL]
    }

    WT <- add_headers(WT, fleet_vec = fleet, gender_vec = gender)
    NG <- add_headers(NG, fleet_vec = fleet, gender_vec = gender)
    
    if ("YEAR" %in% names(WT) && !"#Yr" %in% names(WT)) {
      data.table::setnames(WT, "YEAR", "#Yr", skip_absent=TRUE)
    }
    if ("YEAR" %in% names(NG) && !"#Yr" %in% names(NG)) {
      data.table::setnames(NG, "YEAR", "#Yr", skip_absent=TRUE)
    }
   
    header <- c("#Yr","seas","gender","Fleet","GP","bseas")
    data.table::setcolorder(WT, c(header, age_cols))
    data.table::setcolorder(NG, c(header, age_cols))

    WT <- WT[order(`#Yr`, Fleet, gender)]
    NG <- NG[order(`#Yr`, Fleet, gender)]
    list(WT=WT, NG=NG)
  }

fill_block <- function(WT, NG) {

  # WT and NG must already be SS-keyed consistently:
  # columns: #Yr, Fleet, gender, seas, GP, bseas, and a0..amax in age_cols
  key_cols <- c("#Yr", "Fleet", "gender")

  # ---- pooled reference by Fleet x gender (median across years) ----
  ref <- WT[, lapply(.SD, function(x) stats::median(x, na.rm = TRUE)),
            by = .(Fleet, gender),
            .SDcols = age_cols]

  # If pooled medians still NA for some ages, carry forward/back within ref row
  fill_ref_row <- function(v) {
    if (all(is.na(v))) return(v)
    for (i in seq_along(v)) if (is.na(v[i]) && i > 1) v[i] <- v[i - 1]
    for (i in rev(seq_along(v))) if (is.na(v[i]) && i < length(v)) v[i] <- v[i + 1]
    v
  }
  ref[, (age_cols) := lapply(.SD, fill_ref_row), .SDcols = age_cols]

  # ---- (optional but recommended) borrow across pools if any ref NAs remain ----
  # pool across fleets within gender
  ref_gender <- WT[, lapply(.SD, function(x) stats::median(x, na.rm = TRUE)),
                   by = .(gender),
                   .SDcols = age_cols]
  # pool across all fleets+genders
  ref_all <- WT[, lapply(.SD, function(x) stats::median(x, na.rm = TRUE)),
                .SDcols = age_cols]

  # Join in broader refs to fill any remaining NA in ref
  ref2 <- merge(ref, ref_gender, by = "gender", all.x = TRUE, suffixes = c("", ".g"))
  # add global columns with suffix ".all"
  for (ac in age_cols) ref2[, paste0(ac, ".all") := ref_all[[ac]]]

  for (ac in age_cols) {
    acg   <- paste0(ac, ".g")
    acall <- paste0(ac, ".all")
    ref2[is.na(get(ac)) & !is.na(get(acg)),   (ac) := get(acg)]
    ref2[is.na(get(ac)) & !is.na(get(acall)), (ac) := get(acall)]
    ref2[, c(acg, acall) := NULL]
  }
  ref <- ref2

  # If still NA (extreme case), fill by age-extrapolation on log scale
  fill_missing_by_age <- function(v) {
    a <- 0:(length(v) - 1)
    ok <- which(is.finite(v) & v > 0)
    if (length(ok) == 0) return(rep(1e-6, length(v)))      # absolute last resort
    if (length(ok) == 1) return(rep(v[ok], length(v)))
    fit <- stats::lm(log(v[ok]) ~ a[ok])
    vhat <- exp(stats::predict(fit, newdata = data.frame(a = a)))
    v[!is.finite(v)] <- vhat[!is.finite(v)]
    cummax(v) # keep nondecreasing
  }
  ref[, (age_cols) := lapply(.SD, fill_missing_by_age), .SDcols = age_cols]

  # ---- join reference onto WT ----
  WT2 <- merge(WT, ref, by = c("Fleet", "gender"), suffixes = c("", ".ref"), all.x = TRUE)

  # ---- join NAGE onto WT2 by keys to guarantee alignment ----
  NG2 <- NG[, c(key_cols, age_cols), with = FALSE]
  WT2 <- merge(WT2, NG2, by = key_cols, all.x = TRUE, suffixes = c("", ".n"))

  # ---- fill rule: if WT missing OR NAGE==0 -> use reference ----
  for (ac in age_cols) {
    refc <- paste0(ac, ".ref")
    nac  <- paste0(ac, ".n")

    if (!refc %in% names(WT2)) stop("Missing reference column: ", refc, call. = FALSE)
    if (!nac  %in% names(WT2)) stop("Missing NAGE column after merge: ", nac, call. = FALSE)

    WT2[(is.na(get(ac)) | get(nac) == 0L), (ac) := get(refc)]

    # drop helper cols for this age
    WT2[, c(refc, nac) := NULL]
  }

  # ---- optional internal interpolation row-wise (smooth age curve; no extrapolation) ----
  if (isTRUE(internal_interp)) {
    interp_row <- function(w) {
      idx <- which(is.finite(w))
      if (length(idx) < 2) return(w)
      lo <- min(idx); hi <- max(idx)
      x <- seq_along(w)
      w[lo:hi] <- stats::approx(x[idx], w[idx], xout = x[lo:hi], rule = 1)$y
      w
    }
    mat <- as.matrix(WT2[, ..age_cols])
    mat2 <- t(apply(mat, 1, interp_row))
    WT2[, (age_cols) := data.table::as.data.table(mat2)]
  }

  WT2
}


  # ---- assemble outputs by sex_mode ----
  build_out <- function(WT_w, NG_w) {
    blk <- prep_ss_block(WT_w, NG_w)
    WT_filled <- fill_block(blk$WT, blk$NG)
    print(WT_filled)                             ##cluge to get rid of bad behavior that I couldn't track down...
    out=list(WTAGE = WT_filled, NAGE = blk$NG)
    return(out)
  }

  if (sex_mode == "combined") {
    return(build_out(WT_wide, NG_wide))
  } else {
    out <- list()
    out$F <- build_out(WT_wide[SEX=="F"], NG_wide[SEX=="F"])
    out$M <- build_out(WT_wide[SEX=="M"], NG_wide[SEX=="M"])
    return(out)
  }
}
