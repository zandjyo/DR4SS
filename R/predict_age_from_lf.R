#' Predict ages for large fishery length-frequency data (bulk)
#'
#' Fits should be precomputed with fit_age_predictor().
#'
#' @param lf_dt data.frame/data.table with LENGTH (cm), SEX (M/F/U), MONTH, AREA, YEAR,
#'   and optionally N (counts) and REGION_GRP (user-defined region grouping from region_def).
#'   If REGION_GRP is present, predictions will use the hierarchical spatial structure
#'   (REGION + AREA nested within REGION) that was fit in fit_age_predictor().
#' @param predictor Object from fit_age_predictor().
#' @param target Return type: "agecomp", "posterior_rows", or "row_age".
#' @param map_or_sample If returning integer ages, use "MAP" or "sample".
#' @param seed RNG seed when sampling.
#'
#' @return Depending on target:
#' \describe{
#'   \item{agecomp}{expected age counts by (YEAR,QUARTER,AREA_K,SEX2,AGE)}
#'   \item{posterior_rows}{one row per unique (YEAR,QUARTER,AREA_K,SEX2,LENGTH) with posterior list-column}
#'   \item{row_age}{original rows plus QUARTER, SEX2, and predicted AGE_HAT (and optional AGE_METHOD)}
#' }
#' @export
predict_age_from_lf <- function(lf_dt,
                                predictor,
                                target = c("agecomp", "posterior_rows", "row_age"),
                                map_or_sample = c("MAP", "sample"),
                                seed = 1L) {

  if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table required.", call. = FALSE)

  target <- match.arg(target)
  map_or_sample <- match.arg(map_or_sample)

  DT <- data.table::as.data.table
  lf <- DT(lf_dt)
  names(lf) <- toupper(names(lf))

  need <- c("LENGTH","SEX","MONTH","AREA","YEAR")
  miss <- setdiff(need, names(lf))
  if (length(miss) > 0) stop("lf_dt missing: ", paste(miss, collapse=", "), call. = FALSE)

  if (!"N" %in% names(lf)) lf[, N := 1L]

  suppressWarnings({
    lf[, LENGTH := as.numeric(LENGTH)]
    lf[, YEAR := as.integer(as.character(YEAR))]
    lf[, MONTH := as.integer(as.character(MONTH))]
  })
  lf[, SEX := toupper(trimws(as.character(SEX)))]

  # AREA -> AREA_K (keep consistent with fit_age_predictor() survey NMFS_AREA logic)
  # Here AREA is expected numeric NMFS_AREA (e.g., 510, 520, 610, ...)
  # If AREA is already a 2-digit code (51/52/...), trunc(AREA/10) will also work.
  suppressWarnings(lf[, AREA := as.numeric(AREA)])
  lf[, AREA_K := as.character(trunc(AREA / 10))]
  lf[AREA_K == "50", AREA_K := "51"]

  lf <- lf[is.finite(LENGTH) & !is.na(YEAR) & is.finite(MONTH) & !is.na(AREA_K)]

  # month->quarter
  lf[, QUARTER := 4L]
  lf[MONTH < 3, QUARTER := 1L]
  lf[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
  lf[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
  lf[, QUARTER := as.integer(QUARTER)]

  # Handle sex U = combined
  lf[, SEX2 := data.table::fifelse(SEX %in% c("F","M"), SEX, "U")]

  # REGION_GRP support (optional): if model was fit with REGION_F/AREA_R, we should supply them
  have_region_levels <- !is.null(predictor$levels) &&
    !is.null(predictor$levels$survey$REGION_F) &&
    length(predictor$levels$survey$REGION_F) > 0 &&
    !all(is.na(predictor$levels$survey$REGION_F))

  survey_uses_region <- have_region_levels && length(predictor$levels$survey$REGION_F) > 1
  delta_uses_region  <- !is.null(predictor$levels$fishery$REGION_F) &&
    length(predictor$levels$fishery$REGION_F) > 1

  # if either fit uses region, REGION_GRP column must exist (and be non-missing)
  if (survey_uses_region || delta_uses_region) {
    if (!"REGION_GRP" %in% names(lf)) {
      stop("predictor models include REGION_GRP effects, but lf_dt has no REGION_GRP column.", call. = FALSE)
    }
    lf[, REGION_GRP := as.character(REGION_GRP)]
    lf <- lf[!is.na(REGION_GRP) & nzchar(REGION_GRP)]
    if (nrow(lf) == 0) stop("lf_dt has 0 usable rows after REGION_GRP filtering.", call. = FALSE)
  } else {
    # ensure column exists for keying simplicity, but it won't be used in predict()
    if (!"REGION_GRP" %in% names(lf)) lf[, REGION_GRP := "ALL"]
    lf[, REGION_GRP := as.character(REGION_GRP)]
  }

  # Collapse LF to unique combos to avoid redundant predictions
  # NOTE: REGION_GRP is included in keys to support region-specific predictions.
  keys <- c("YEAR","QUARTER","AREA_K","REGION_GRP","SEX2","LENGTH")
  lf_u <- lf[, .(N = sum(N, na.rm = TRUE)), by = keys]

  ages <- 0:predictor$maxage

  # priors (still keyed by AREA_K, YEAR, QUARTER, SEX as in fit_age_predictor())
  pool <- data.table::copy(predictor$prior_pool)
  glob <- data.table::copy(predictor$prior_global)
  cell <- data.table::copy(predictor$prior_cell)

  # function: get prior vector for one row
  get_prior_vec <- function(y, q, a_k, sx) {
    if (sx == "U") {
      pF <- get_prior_vec(y, q, a_k, "F")
      pM <- get_prior_vec(y, q, a_k, "M")
      return((pF + pM) / 2)
    }

    pc <- cell[YEAR == y & QUARTER == q & AREA_K == a_k & SEX == sx]
    ncell <- if (nrow(pc) > 0) unique(pc$N_CELL) else 0L

    pp <- pool[AREA_K == a_k & SEX == sx]
    if (nrow(pp) == 0) {
      pg <- glob[SEX == sx]
      base <- pg[, .(AGE_G, P = P_GLOB)]
    } else {
      base <- pp[, .(AGE_G, P = P_POOL)]
    }

    p_base <- rep(0, length(ages)); names(p_base) <- as.character(ages)
    p_base[as.character(base$AGE_G)] <- base$P

    if (ncell >= predictor$min_n_cell && nrow(pc) > 0) {
      p_cell <- rep(0, length(ages)); names(p_cell) <- as.character(ages)
      p_cell[as.character(pc$AGE_G)] <- pc$P_CELL
      p <- predictor$prior_mix * p_cell + (1 - predictor$prior_mix) * p_base
    } else {
      p <- p_base
    }

    p <- p + 1e-12
    p / sum(p)
  }

  # model residual SD (once)
  sd_s <- stats::sd(stats::residuals(predictor$survey_fit), na.rm = TRUE)
  sd_d <- stats::sd(stats::residuals(predictor$delta_fit),  na.rm = TRUE)
  sd_q <- sqrt(sd_s^2 + sd_d^2)
  if (!is.finite(sd_q) || sd_q <= 0) sd_q <- max(sd_s, sd_d, 1)

  # helper to get mu_q(age) for one row (sex U handled by averaging)
  get_mu_q <- function(y, q, a_k, r_g, sx) {

    mu_for_sex <- function(sx2) {

      # ---- Survey backbone prediction newdata ----
      lev_s <- predictor$levels$survey
      nd_q3 <- data.frame(
        SEX_F  = factor(sx2, levels = lev_s$SEX_F),
        AGE_G  = ages,
        YEAR_F = factor(y, levels = lev_s$YEAR_F)
      )

      if (!is.null(lev_s$REGION_F) && length(lev_s$REGION_F) > 1) {
        # region + nested area
        region_f <- factor(r_g, levels = lev_s$REGION_F)
        area_f   <- factor(a_k, levels = lev_s$AREA_F)
        area_r   <- factor(interaction(region_f, area_f, drop = TRUE), levels = lev_s$AREA_R)

        nd_q3$REGION_F <- region_f
        nd_q3$AREA_F   <- area_f
        nd_q3$AREA_R   <- area_r
      } else {
        # area only
        if (!is.null(lev_s$AREA_F)) nd_q3$AREA_F <- factor(a_k, levels = lev_s$AREA_F)
      }

      mu_q3 <- as.numeric(stats::predict(predictor$survey_fit, newdata = nd_q3, type = "response"))

      # ---- Fishery delta prediction newdata ----
      lev_f <- predictor$levels$fishery
      nd_d <- data.frame(
        Q_F    = factor(q, levels = lev_f$Q_F),
        AGE_G  = ages,
        SEX_F  = factor(sx2, levels = lev_f$SEX_F),
        YEAR_F = factor(y, levels = lev_f$YEAR_F)
      )

      if (!is.null(lev_f$REGION_F) && length(lev_f$REGION_F) > 1) {
        region_f <- factor(r_g, levels = lev_f$REGION_F)
        area_f   <- factor(a_k, levels = lev_f$AREA_F)
        area_r   <- factor(interaction(region_f, area_f, drop = TRUE), levels = lev_f$AREA_R)

        nd_d$REGION_F <- region_f
        nd_d$AREA_F   <- area_f
        nd_d$AREA_R   <- area_r
      } else {
        if (!is.null(lev_f$AREA_F)) nd_d$AREA_F <- factor(a_k, levels = lev_f$AREA_F)
      }

      mu_d <- as.numeric(stats::predict(predictor$delta_fit, newdata = nd_d, type = "response"))

      mu_q3 + mu_d
    }

    if (sx == "U") return((mu_for_sex("F") + mu_for_sex("M")) / 2)
    mu_for_sex(sx)
  }

  # Bulk posterior for unique keys
  post_list <- vector("list", nrow(lf_u))
  for (i in seq_len(nrow(lf_u))) {
    y  <- lf_u$YEAR[i]
    q  <- lf_u$QUARTER[i]
    a  <- lf_u$AREA_K[i]
    rg <- lf_u$REGION_GRP[i]
    sx <- lf_u$SEX2[i]
    L  <- lf_u$LENGTH[i]

    mu_q <- get_mu_q(y, q, a, rg, sx)
    lik  <- stats::dnorm(L, mean = mu_q, sd = sd_q)
    pr   <- get_prior_vec(y, q, a, sx)
    post <- lik * pr
    if (!any(is.finite(post)) || sum(post) <= 0) post <- rep(1/length(ages), length(ages))
    post <- post / sum(post)
    post_list[[i]] <- post
  }

  lf_u[, posterior := post_list]

  # ---- target: posterior_rows ----
  if (target == "posterior_rows") return(lf_u[])

  # ---- target: row_age ----
  if (target == "row_age") {
    # compute AGE_HAT per unique key
    if (map_or_sample == "MAP") {
      lf_u[, AGE_HAT := ages[max.col(do.call(rbind, posterior), ties.method = "first")]]
    } else {
      set.seed(seed)
      lf_u[, AGE_HAT := vapply(posterior, function(p) sample(ages, 1L, prob = p), integer(1))]
    }

    # join back to original rows (no cartesian: keys are unique in lf_u)
    out <- merge(
      lf,
      lf_u[, c(keys, "AGE_HAT"), with = FALSE],
      by = keys,
      all.x = TRUE
    )

    out[, AGE_METHOD := map_or_sample]
    return(out[])
  }

  # ---- target: agecomp ----
  # Note: output retains REGION_GRP in the grouping to keep compositions consistent with how predictions were made.
  age_dt <- lf_u[, .(AGE = ages, P = unlist(posterior)), by = keys]
  age_dt <- merge(age_dt, lf_u[, c(keys, "N"), with = FALSE], by = keys)
  age_dt[, EXP_N := N * P]

  age_dt[, .(EXP_N = sum(EXP_N, na.rm = TRUE)),
         by = .(YEAR, QUARTER, AREA_K, REGION_GRP, SEX2, AGE)][
           order(YEAR, QUARTER, REGION_GRP, AREA_K, SEX2, AGE)
         ]
}