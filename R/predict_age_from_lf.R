#' Predict ages from length-frequency using region-specific predictors (bulk)
#'
#' Uses predictors fit by fit_age_predictor() (region-independent by REGION_GRP).
#' No borrowing among regions: each row is predicted using its REGION_GRP model only.
#' Borrowing within region occurs for missing YEAR/AREA levels by setting those REs to 0.
#'
#' @param lf_dt data.frame/data.table with LENGTH (cm), SEX (M/F/U), MONTH, AREA, YEAR,
#'   and REGION_GRP (required unless predictor only has region "ALL"). Optional N counts.
#' @param predictor Object from fit_age_predictor().
#' @param target "agecomp", "posterior_rows", or "row_age".
#' @param map_or_sample If returning integer ages, use "MAP" or "sample".
#' @param seed RNG seed when sampling.
#' @return See original predict_age_from_lf() documentation.
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

  if (is.null(predictor$by_region) || length(predictor$by_region) == 0) {
    stop("predictor does not look like output from fit_age_predictor().", call. = FALSE)
  }

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
    lf[, AREA := as.numeric(AREA)]
  })
  lf[, SEX := toupper(trimws(as.character(SEX)))]

  lf[, AREA_K := as.character(trunc(AREA / 10))]
  lf[AREA_K == "50", AREA_K := "51"]

  lf <- lf[is.finite(LENGTH) & !is.na(YEAR) & is.finite(MONTH) & !is.na(AREA_K)]

  # month->quarter
  lf[, QUARTER := 4L]
  lf[MONTH < 3, QUARTER := 1L]
  lf[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
  lf[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
  lf[, QUARTER := as.integer(QUARTER)]

  # Handle sex U
  lf[, SEX2 := data.table::fifelse(SEX %in% c("F","M"), SEX, "U")]

  # region assignment
  if (!"REGION_GRP" %in% names(lf)) {
    if (length(predictor$regions) == 1 && predictor$regions[1] == "ALL") {
      lf[, REGION_GRP := "ALL"]
    } else {
      stop("length frequency data must include REGION_GRP to use region-specific predictors.", call. = FALSE)
    }
  }
  lf[, REGION_GRP := as.character(REGION_GRP)]
  lf <- lf[!is.na(REGION_GRP) & nzchar(REGION_GRP)]
  if (nrow(lf) == 0) stop("length frequency data has 0 usable rows after REGION_GRP filtering.", call. = FALSE)

  # collapse to unique combos
  keys <- c("REGION_GRP","YEAR","QUARTER","AREA_K","SEX2","LENGTH")
  lf_u <- lf[, .(N = sum(N, na.rm = TRUE)), by = keys]

  # per-row posterior construction
  build_posteriors_one_region <- function(dt_reg, pred_reg) {
    ages <- 0:pred_reg$maxage

    pool <- data.table::copy(pred_reg$prior_pool)
    glob <- data.table::copy(pred_reg$prior_global)
    cell <- data.table::copy(pred_reg$prior_cell)

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

      if (ncell >= pred_reg$min_n_cell && nrow(pc) > 0) {
        p_cell <- rep(0, length(ages)); names(p_cell) <- as.character(ages)
        p_cell[as.character(pc$AGE_G)] <- pc$P_CELL
        p <- pred_reg$prior_mix * p_cell + (1 - pred_reg$prior_mix) * p_base
      } else {
        p <- p_base
      }

      p <- p + 1e-12
      p / sum(p)
    }

    sd_s <- stats::sd(stats::residuals(pred_reg$survey_fit), na.rm = TRUE)
    sd_d <- stats::sd(stats::residuals(pred_reg$delta_fit),  na.rm = TRUE)
    sd_q <- sqrt(sd_s^2 + sd_d^2)
    if (!is.finite(sd_q) || sd_q <= 0) sd_q <- max(sd_s, sd_d, 1)

    safe_predict <- function(gam_fit, newdata, lev, which = c("survey","delta")) {
      which <- match.arg(which)
      exclude <- character(0)

      # YEAR
      newdata$YEAR_F <- factor(as.character(newdata$YEAR_F), levels = lev$YEAR_F)
      if (any(is.na(newdata$YEAR_F))) {
        newdata$YEAR_F[is.na(newdata$YEAR_F)] <- lev$YEAR_F[1]
        exclude <- c(exclude, "s(YEAR_F)")
      }

      # AREA
      newdata$AREA_F <- factor(as.character(newdata$AREA_F), levels = lev$AREA_F)
      if (any(is.na(newdata$AREA_F))) {
        newdata$AREA_F[is.na(newdata$AREA_F)] <- lev$AREA_F[1]
        exclude <- c(exclude, "s(AREA_F)")
      }

      # SEX
      if ("SEX_F" %in% names(newdata)) {
        newdata$SEX_F <- factor(as.character(newdata$SEX_F), levels = lev$SEX_F)
      }

      # Quarter (delta only)
      if (which == "delta" && "Q_F" %in% names(newdata)) {
        newdata$Q_F <- factor(as.character(newdata$Q_F), levels = lev$Q_F)
      }

      exclude <- unique(exclude)
      tryCatch(
        as.numeric(stats::predict(gam_fit, newdata = newdata, type = "response", exclude = exclude)),
        error = function(e) as.numeric(stats::predict(gam_fit, newdata = newdata, type = "response"))
      )
    }

    # compute mu and posterior for each row
    dt_reg[, `:=`(
      YEAR_F = as.character(YEAR),
      AREA_F = as.character(AREA_K)
    )]

    mu_for <- function(row, age, sx) {
      # Survey backbone at Q3
      nd_s <- data.frame(
        SEX_F  = sx,
        AGE_G  = age,
        YEAR_F = row$YEAR_F,
        AREA_F = row$AREA_F
      )
      mu_q3 <- safe_predict(pred_reg$survey_fit, nd_s, pred_reg$levels$survey, which = "survey")

      # Delta for quarter
      nd_d <- data.frame(
        Q_F    = as.character(row$QUARTER),
        AGE_G  = age,
        SEX_F  = sx,
        YEAR_F = row$YEAR_F,
        AREA_F = row$AREA_F
      )
      d_q <- safe_predict(pred_reg$delta_fit, nd_d, pred_reg$levels$fishery, which = "delta")
      mu_q3 + d_q
    }

    dt_reg[, POST := lapply(seq_len(.N), function(ii) {
      y  <- YEAR[ii]
      q  <- QUARTER[ii]
      a_k<- AREA_K[ii]
      sx <- SEX2[ii]
      L  <- LENGTH[ii]
      row_sd <- dt_reg[ii]

      if (sx == "U") {
        # 50/50 sex split for the likelihood by averaging the two sex-specific posteriors
        pF <- get_prior_vec(y, q, a_k, "F")
        pM <- get_prior_vec(y, q, a_k, "M")

        llF <- vapply(ages, function(a) stats::dnorm(L, mean = mu_for(row_sd, a, "F"), sd = sd_q), numeric(1))
        llM <- vapply(ages, function(a) stats::dnorm(L, mean = mu_for(row_sd, a, "M"), sd = sd_q), numeric(1))

        postF <- pF * llF; postF <- postF / sum(postF)
        postM <- pM * llM; postM <- postM / sum(postM)

        post <- 0.5 * postF + 0.5 * postM
        post <- post / sum(post)
        post
      } else {
        p  <- get_prior_vec(y, q, a_k, sx)
        ll <- vapply(ages, function(a) stats::dnorm(L, mean = mu_for(row_sd, a, sx), sd = sd_q), numeric(1))
        post <- p * ll
        post <- post / sum(post)
        post
      }
    })]

    dt_reg
  }

  # split and run
  out_list <- lapply(split(lf_u, by = "REGION_GRP", keep.by = TRUE), function(dtr) {
    reg <- unique(dtr$REGION_GRP)
    if (!(reg %in% names(predictor$by_region))) stop("No fitted model for REGION_GRP='", reg, "'.", call. = FALSE)
    build_posteriors_one_region(data.table::copy(dtr), predictor$by_region[[reg]])
  })

  lf_post <- data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE)

  # ----- outputs -----
  ages <- 0:predictor$maxage

  if (target == "posterior_rows") {
    return(lf_post[])
  }

  if (target == "row_age") {
    set.seed(seed)
    lf_post[, AGE_HAT := {
      p <- POST[[1]]
      if (map_or_sample == "MAP") {
        ages[which.max(p)]
      } else {
        sample(ages, size = 1, prob = p)
      }
    }, by = seq_len(nrow(lf_post))]
    return(lf_post[])
  }

  # agecomp
  agecomp <- lf_post[, {
    p <- POST[[1]]
    data.table::data.table(AGE_G = ages, N_AGE = N * p)
  }, by = .(REGION_GRP, YEAR, QUARTER, AREA_K, SEX2)]
  agecomp[, AGE_G := as.integer(AGE_G)]

  return(agecomp[])
}
