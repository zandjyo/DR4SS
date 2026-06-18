#' Predict ages from length-frequency using region-specific predictors (bulk)
#'
#' Uses predictors fit by fit_age_predictor() (region-independent by REGION_GRP).
#' No borrowing among regions: each row is predicted using its REGION_GRP model only.
#' Borrowing within region occurs for missing YEAR/AREA levels by setting those REs to 0.
#'
#' @param lf_dt data.frame/data.table with LENGTH (cm), SEX (M/F/U), MONTH, AREA, YEAR,
#'   GEAR, and REGION_GRP (required unless predictor only has region "ALL"). Optional N counts.
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
  need <- c("LENGTH", "SEX", "MONTH", "AREA", "YEAR", "GEAR")
  miss <- setdiff(need, names(lf))
  if (length(miss) > 0) stop("lf_dt missing: ", paste(miss, collapse = ", "), call. = FALSE)
  if (!"N" %in% names(lf)) lf[, N := 1L]

  suppressWarnings({
    lf[, LENGTH := as.numeric(LENGTH)]
    lf[, YEAR := as.integer(as.character(YEAR))]
    lf[, MONTH := as.integer(as.character(MONTH))]
    lf[, AREA := as.numeric(AREA)]
    lf[, N := as.numeric(N)]
  })
  lf[, SEX := toupper(trimws(as.character(SEX)))]

  # Convert numeric sex codes to M/F/U
  lf[SEX %in% c("2", "M", "MALE"), SEX := "M"]
  lf[SEX %in% c("1", "F", "FEMALE"), SEX := "F"]
  lf[SEX %in% c("0", "3", "U", "UNK", "UNKNOWN", ""), SEX := "U"]
  lf[is.na(SEX), SEX := "U"]

  lf[, GEAR := trimws(as.character(GEAR))]

  lf[, AREA_K := as.character(trunc(AREA / 10))]
  lf[AREA_K == "50", AREA_K := "51"]

  lf <- lf[is.finite(LENGTH) & !is.na(YEAR) & is.finite(MONTH) & !is.na(AREA_K) & !is.na(GEAR) & nzchar(GEAR)]

  # month->quarter
  lf[, QUARTER := 4L]
  lf[MONTH < 3, QUARTER := 1L]
  lf[MONTH >= 3 & MONTH < 7, QUARTER := 2L]
  lf[MONTH >= 7 & MONTH < 10, QUARTER := 3L]
  lf[, QUARTER := as.integer(QUARTER)]

  # Handle sex U
  lf[, SEX2 := data.table::fifelse(SEX %in% c("F", "M"), SEX, "U")]

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
  if (nrow(lf) == 0) stop("length frequency data has 0 usable rows after filtering.", call. = FALSE)

  # collapse to unique combos   

  keys<-c('SPECIES','REGION_GRP', 'YEAR', 'GEAR', 'AREA2', 'AREA', 'AREA_K', 'MONTH', 'QUARTER', 'MONTH_WED', 'CRUISE', 'PERMIT', 'VES_AKR_ADFG', 'HAUL_JOIN', 'SEX2', 'LENGTH', 'SUM_FREQUENCY', 'EXTRAPOLATED_WEIGHT', 'NUMB') 

  #keys <- c("REGION_GRP", "GEAR", "YEAR", "MONTH","QUARTER", "NUMB", "AREA_K", "SEX2", "LENGTH")
  lf_u <- lf[, .(N = sum(N, na.rm = TRUE)), by = keys]

  build_posteriors_one_region <- function(dt_reg, pred_reg) {
    ages <- 0:pred_reg$maxage
    age_chr <- as.character(ages)

    pool <- data.table::copy(pred_reg$prior_pool)
    glob <- data.table::copy(pred_reg$prior_global)
    cell <- data.table::copy(pred_reg$prior_cell)
    data.table::setkey(cell, YEAR, QUARTER, AREA_K, SEX)
    data.table::setkey(pool, AREA_K, SEX)
    data.table::setkey(glob, SEX)

    # Be tolerant of predictors saved with slightly different component names.
    min_n_cell <- pred_reg$min_n_cell
    if (is.null(min_n_cell) || length(min_n_cell) == 0 || !is.finite(min_n_cell[1])) {
      min_n_cell <- 0L
    } else {
      min_n_cell <- min_n_cell[1]
    }

    prior_mix <- pred_reg$prior_mix
    if (is.null(prior_mix) || length(prior_mix) == 0 || !is.finite(prior_mix[1])) {
      prior_mix <- 0.5
    } else {
      prior_mix <- prior_mix[1]
    }

    # Cache priors because many rows share YEAR/QUARTER/AREA/SEX.
    prior_cache <- new.env(parent = emptyenv())
    get_prior_vec <- function(y, q, a_k, sx) {
      cache_key <- paste(y, q, a_k, sx, sep = "|")
      if (exists(cache_key, envir = prior_cache, inherits = FALSE)) {
        return(get(cache_key, envir = prior_cache, inherits = FALSE))
      }

      if (sx == "U") {
        pF <- get_prior_vec(y, q, a_k, "F")
        pM <- get_prior_vec(y, q, a_k, "M")
        p <- (pF + pM) / 2
        assign(cache_key, p, envir = prior_cache)
        return(p)
      }

      pc <- cell[.(y, q, a_k, sx)]
      ncell <- if (nrow(pc) > 0 && "N_CELL" %in% names(pc)) unique(pc$N_CELL)[1] else 0L
      if (is.na(ncell) || !is.finite(ncell)) ncell <- 0L

      pp <- pool[.(a_k, sx)]
      if (nrow(pp) == 0) {
        base <- glob[.(sx), .(AGE_G, P = P_GLOB)]
      } else {
        base <- pp[, .(AGE_G, P = P_POOL)]
      }

      p_base <- rep(0, length(ages)); names(p_base) <- age_chr
      p_base[as.character(base$AGE_G)] <- base$P

      if (ncell >= min_n_cell && nrow(pc) > 0) {
        p_cell <- rep(0, length(ages)); names(p_cell) <- age_chr
        p_cell[as.character(pc$AGE_G)] <- pc$P_CELL
        p <- prior_mix * p_cell + (1 - prior_mix) * p_base
      } else {
        p <- p_base
      }

      p <- p + 1e-12
      p <- normalize_prob(p)
      assign(cache_key, p, envir = prior_cache)
      p
    }

    normalize_prob <- function(x) {
      x[!is.finite(x)] <- 0
      s <- sum(x)
      if (!is.finite(s) || s <= 0) {
        rep(1 / length(x), length(x))
      } else {
        x / s
      }
    }

    sd_s <- stats::sd(stats::residuals(pred_reg$survey_fit), na.rm = TRUE)
    sd_d <- stats::sd(stats::residuals(pred_reg$delta_fit),  na.rm = TRUE)
    sd_q <- sqrt(sd_s^2 + sd_d^2)
    if (!is.finite(sd_q) || sd_q <= 0) sd_q <- max(sd_s, sd_d, 1)

    prep_newdata <- function(newdata, lev, kind = c("survey", "delta")) {
      kind <- match.arg(kind)
      newdata$YEAR_F <- factor(as.character(newdata$YEAR_F), levels = lev$YEAR_F)
      newdata$AREA_F <- factor(as.character(newdata$AREA_F), levels = lev$AREA_F)
      newdata$SEX_F <- factor(as.character(newdata$SEX_F), levels = lev$SEX_F)
      if (kind == "delta" && "Q_F" %in% names(newdata)) {
        newdata$Q_F <- factor(as.character(newdata$Q_F), levels = lev$Q_F)
      }
      newdata
    }

    predict_by_missing_re <- function(gam_fit, newdata, lev, kind = c("survey", "delta")) {
      kind <- match.arg(kind)
      nd <- prep_newdata(newdata, lev, kind = kind)
      out <- rep(NA_real_, nrow(nd))

      miss_y <- is.na(nd$YEAR_F)
      miss_a <- is.na(nd$AREA_F)
      nd$YEAR_F[miss_y] <- lev$YEAR_F[1]
      nd$AREA_F[miss_a] <- lev$AREA_F[1]

      groups <- list(
        none = which(!miss_y & !miss_a),
        year = which( miss_y & !miss_a),
        area = which(!miss_y &  miss_a),
        both = which( miss_y &  miss_a)
      )
      excludes <- list(
        none = character(0),
        year = "s(YEAR_F)",
        area = "s(AREA_F)",
        both = c("s(YEAR_F)", "s(AREA_F)")
      )

      for (nm in names(groups)) {
        ii <- groups[[nm]]
        if (length(ii) == 0) next
        ex <- excludes[[nm]]
        out[ii] <- tryCatch(
          as.numeric(stats::predict(gam_fit, newdata = nd[ii, , drop = FALSE], type = "response", exclude = ex)),
          error = function(e) as.numeric(stats::predict(gam_fit, newdata = nd[ii, , drop = FALSE], type = "response"))
        )
      }
      out
    }

    dt_reg[, `:=`(
      ROW_ID = .I,
      YEAR_F = as.character(YEAR),
      AREA_F = as.character(AREA_K)
    )]

    # Batched prediction table for mu by row, sex, and age.
    base_rows <- unique(dt_reg[, .(ROW_ID, YEAR, QUARTER, AREA_K, YEAR_F, AREA_F)])
    sex_age <- data.table::CJ(SEX_F = c("F", "M"), AGE_G = ages, unique = TRUE)
    pred_grid <- base_rows[rep(seq_len(nrow(base_rows)), each = nrow(sex_age))]
    pred_grid[, `:=`(
      SEX_F = rep(sex_age$SEX_F, times = nrow(base_rows)),
      AGE_G = rep(sex_age$AGE_G, times = nrow(base_rows))
    )]

    nd_s <- data.frame(
      SEX_F  = pred_grid$SEX_F,
      AGE_G  = pred_grid$AGE_G,
      YEAR_F = pred_grid$YEAR_F,
      AREA_F = pred_grid$AREA_F
    )
    pred_grid[, MU_Q3 := predict_by_missing_re(pred_reg$survey_fit, nd_s, pred_reg$levels$survey, kind = "survey")]

    nd_d <- data.frame(
      Q_F    = as.character(pred_grid$QUARTER),
      AGE_G  = pred_grid$AGE_G,
      SEX_F  = pred_grid$SEX_F,
      YEAR_F = pred_grid$YEAR_F,
      AREA_F = pred_grid$AREA_F
    )
    pred_grid[, D_Q := predict_by_missing_re(pred_reg$delta_fit, nd_d, pred_reg$levels$fishery, kind = "delta")]
    pred_grid[, MU := MU_Q3 + D_Q]
    data.table::setkey(pred_grid, ROW_ID, SEX_F, AGE_G)

    dt_reg[, POST := lapply(seq_len(.N), function(ii) {
      y   <- YEAR[ii]
      q   <- QUARTER[ii]
      a_k <- AREA_K[ii]
      sx  <- SEX2[ii]
      L   <- LENGTH[ii]
      rid <- ROW_ID[ii]

      if (sx == "U") {
        pF <- get_prior_vec(y, q, a_k, "F")
        pM <- get_prior_vec(y, q, a_k, "M")

        muF <- pred_grid[.(rid, "F", ages), MU]
        muM <- pred_grid[.(rid, "M", ages), MU]
        llF <- stats::dnorm(L, mean = muF, sd = sd_q)
        llM <- stats::dnorm(L, mean = muM, sd = sd_q)

        postF <- normalize_prob(pF * llF)
        postM <- normalize_prob(pM * llM)
        post <- 0.5 * postF + 0.5 * postM
        normalize_prob(post)
      } else {
        p <- get_prior_vec(y, q, a_k, sx)
        mu <- pred_grid[.(rid, sx, ages), MU]
        ll <- stats::dnorm(L, mean = mu, sd = sd_q)
        post <- p * ll
        normalize_prob(post)
      }
    })]

    dt_reg[, ROW_ID := NULL]
    dt_reg
  }

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

  # agecomp, now including GEAR and MONTH
  agecomp <- lf_post[, {
    p <- POST[[1]]
    data.table::data.table(AGE_G = ages, N_AGE = N * p)
  #}, by = .(REGION_GRP, GEAR, YEAR, MONTH, QUARTER, NUMB, AREA_K, SEX2)]
  }, by = .(SPECIES, REGION_GRP, YEAR, GEAR, AREA2, AREA, AREA_K, MONTH, QUARTER, MONTH_WED, CRUISE, PERMIT, VES_AKR_ADFG, HAUL_JOIN, SEX2, LENGTH, SUM_FREQUENCY, EXTRAPOLATED_WEIGHT, NUMB)]
  agecomp[, AGE_G := as.integer(AGE_G)]

  return(agecomp[])
}
