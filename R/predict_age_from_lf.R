#' Predict ages for large fishery length-frequency data (bulk)
#'
#' Fits should be precomputed with fit_age_predictor().
#'
#' @param lf_dt data.frame/data.table with LENGTH (cm), SEX (M/F/U), MONTH, AREA, YEAR,
#'   and optionally N (counts). If N missing, assumes 1 per row.
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
  
  lf[, AREA_K := as.character(trunc(AREA / 10))]
  lf[AREA_K == "50", AREA_K := "51"]

  lf <- lf[is.finite(LENGTH) & !is.na(YEAR) & is.finite(MONTH)]

  # month->quarter
  lf[, QUARTER := 4L]
  lf[MONTH < 3, QUARTER := 1L]
  lf[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
  lf[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
  lf[, QUARTER := as.integer(QUARTER)]

  # Handle sex U = combined
  lf[, SEX2 := data.table::fifelse(SEX %in% c("F","M"), SEX, "U")]

  # Collapse LF to unique combos to avoid redundant predictions
  keys <- c("YEAR","QUARTER","AREA_K","SEX2","LENGTH")
  lf_u <- lf[, .(N = sum(N, na.rm = TRUE)), by = keys]

  ages <- 0:predictor$maxage

  # priors
  pool <- data.table::copy(predictor$prior_pool)
  glob <- data.table::copy(predictor$prior_global)
  cell <- data.table::copy(predictor$prior_cell)

  # function: get prior vector for one row
  get_prior_vec <- function(y, q, a_k, sx) {
    if (sx == "U") {
      pF <- get_prior_vec(y,q,a_k,"F")
      pM <- get_prior_vec(y,q,a_k,"M")
      return((pF + pM)/2)
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
  get_mu_q <- function(y, q, a_k, sx) {
    mu_for_sex <- function(sx2) {
      nd_q3 <- data.frame(
        SEX_F  = factor(sx2, levels = c("F","M")),
        AGE_G  = ages,
        YEAR_F = factor(y),
        AREA_F = factor(a_k)
      )
      mu_q3 <- as.numeric(stats::predict(predictor$survey_fit, newdata = nd_q3, type = "response"))

      nd_d <- data.frame(
        Q_F    = factor(q, levels = 1:4),
        AGE_G  = ages,
        SEX_F  = factor(sx2, levels = c("F","M")),
        YEAR_F = factor(y),
        AREA_F = factor(a_k)
      )
      mu_d <- as.numeric(stats::predict(predictor$delta_fit, newdata = nd_d, type = "response"))
      mu_q3 + mu_d
    }

    if (sx == "U") return((mu_for_sex("F") + mu_for_sex("M"))/2)
    mu_for_sex(sx)
  }

  # Bulk posterior for unique keys
  post_list <- vector("list", nrow(lf_u))
  for (i in seq_len(nrow(lf_u))) {
    y  <- lf_u$YEAR[i]
    q  <- lf_u$QUARTER[i]
    a  <- lf_u$AREA_K[i]
    sx <- lf_u$SEX2[i]
    L  <- lf_u$LENGTH[i]

    mu_q <- get_mu_q(y,q,a,sx)
    lik  <- stats::dnorm(L, mean = mu_q, sd = sd_q)
    pr   <- get_prior_vec(y,q,a,sx)
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
  age_dt <- lf_u[, .(AGE = ages, P = unlist(posterior)), by = keys]
  age_dt <- merge(age_dt, lf_u[, c(keys,"N"), with = FALSE], by = keys)
  age_dt[, EXP_N := N * P]

  age_dt[, .(EXP_N = sum(EXP_N, na.rm = TRUE)),
         by = .(YEAR, QUARTER, AREA_K, SEX2, AGE)][order(YEAR,QUARTER,AREA_K,SEX2,AGE)]
}
