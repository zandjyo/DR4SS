#' Format composition data to a Stock Synthesis (.ss) comp block
#'
#' Generalized formatter for age or length composition data for Stock Synthesis.
#' For age data, the source bin column is used directly. For length data, a
#' user-supplied binning function can be applied before aggregation.
#'
#' @param data Nested list containing survey and fishery composition data.
#' @param bins Numeric vector of bins to include in the output.
#' @param bin_col Character string naming the source column holding age or length.
#' @param comp_type Either "age" or "len".
#' @param sexed Logical; if TRUE output female and male compositions separately.
#' @param ageerr Integer age-error definition used for age comps only.
#' @param lbin_lo Optional low bin value written to SS block; defaults to min(bins).
#' @param lbin_hi Optional high bin value written to SS block; defaults to max(bins).
#' @param bin_fun Optional function used to bin raw length data before formatting.
#'   It must take arguments `data` and `len_bins` and return a data.frame/data.table
#'   containing a column named in `bin_col`.
#' @param survey_label_fun Function to create survey fleet labels.
#' @param fishery_label_fun Function to create fishery fleet labels.
#' @param file Optional output file path.
#'
#' @return A list with formatted tables, fleet map, and optionally written file.
#' @import data.table
#' @export
format_comp_to_ss_block <- function(data,
                                    bins,
                                    bin_col = "AGE",
                                    comp_type = c("age", "len"),
                                    sexed = TRUE,
                                    ageerr = 1L,
                                    lbin_lo = NULL,
                                    lbin_hi = NULL,
                                    bin_fun = BIN_LEN_DATA,
                                    survey_label_fun = function(z) {
                                      paste0("FISH_", z$REGION_GRP, "_", z$SEASON)
                                    },
                                    fishery_label_fun = function(z) {
                                      paste0("FISH_", z$REGION_GRP, "_", z$GEAR, "_", z$SEASON)
                                    },
                                    file = NULL) {

  comp_type <- match.arg(comp_type)

  if (missing(bins) || length(bins) == 0) {
    stop("`bins` must be provided and contain at least one value.")
  }

  if (!is.character(bin_col) || length(bin_col) != 1L) {
    stop("`bin_col` must be a single character string.")
  }

  if (is.null(lbin_lo)) lbin_lo <- min(bins)
  if (is.null(lbin_hi)) lbin_hi <- max(bins)

  safe_max_int <- function(x) {
    x <- x[!is.na(x)]
    if (length(x) == 0) return(NA_integer_)
    as.integer(max(x))
  }

  fmt_num <- function(x) {
    ifelse(
      is.na(x),
      "NA",
      ifelse(
        abs(x - round(x)) < 1e-12,
        as.character(as.integer(round(x))),
        formatC(x, format = "f", digits = 6, drop0trailing = TRUE)
      )
    )
  }

  .prep_bins <- function(dt) {
    dt <- copy(as.data.table(dt))

    if (comp_type == "len") {
      if (is.null(bin_fun)) {
        stop("For comp_type = 'len', please provide `bin_fun`, e.g. BIN_LEN_DATA.")
      }
      dt <- as.data.table(bin_fun(data = dt, len_bins = bins))
      dt$LENGTH<-dt$BIN
    }

    if (!bin_col %in% names(dt)) {
      stop("Column `", bin_col, "` not found after preprocessing.")
    }

    dt[, BIN := get(bin_col)]
    dt
  }

  .build_comp <- function(dt, group_vars, fleet_label_fun, source_name) {
    dt <- .prep_bins(dt)

    needed <- c("BIN", "FREQ", "NHAUL", "YEAR", "SEASON", "REGION_GRP")
    miss <- setdiff(needed, names(dt))
    if (length(miss) > 0) {
      stop(
        source_name, " is missing required columns: ",
        paste(miss, collapse = ", ")
      )
    }

    if ("GEAR" %in% group_vars && !"GEAR" %in% names(dt)) {
      stop(source_name, " needs a GEAR column.")
    }

    if (sexed && !"SEX" %in% names(dt)) {
      stop(source_name, " needs a SEX column when sexed = TRUE.")
    }

    if (!sexed) {
      dt[, SEX := "U"]
    }

    acomp <- dt[
      BIN %in% bins,
      .(
        TOTAL = sum(FREQ, na.rm = TRUE),
        NHAUL = safe_max_int(NHAUL)
      ),
      by = c("BIN", group_vars, "SEX")
    ]

    acomp[
      ,
      T_NUMBER := sum(TOTAL, na.rm = TRUE),
      by = group_vars
    ][
      ,
      TOTAL := fifelse(T_NUMBER > 0, TOTAL / T_NUMBER, 0)
    ]

    combo_list <- lapply(group_vars, function(v) sort(unique(acomp[[v]])))
    names(combo_list) <- group_vars
    combo_list[["SEX"]] <- if (sexed) c("F", "M") else "U"
    combo_list[["BIN"]] <- bins

    all_combos <- do.call(CJ, c(combo_list, list(unique = TRUE)))

    setkeyv(acomp, c(group_vars, "SEX", "BIN"))
    setkeyv(all_combos, c(group_vars, "SEX", "BIN"))
    acomp <- all_combos[acomp]

    acomp[is.na(TOTAL), TOTAL := 0]
    acomp[is.na(NHAUL), NHAUL := NA_integer_]

    nsamp_dt <- acomp[
      ,
      .(Nsamp = safe_max_int(NHAUL)),
      by = group_vars
    ]

    fleet_key <- unique(acomp[, ..group_vars])
    fleet_key[
      ,
      FleetLabel := fleet_label_fun(.SD),
      .SDcols = group_vars
    ]
    fleet_key[, Source := source_name]

    if (sexed) {
      wide <- dcast(
        acomp,
        as.formula(
          paste(paste(group_vars, collapse = " + "), "~ SEX + BIN")
        ),
        value.var = "TOTAL",
        fill = 0
      )

      female_cols <- paste0("F_", bins)
      male_cols   <- paste0("M_", bins)

      for (nm in c(female_cols, male_cols)) {
        if (!nm %in% names(wide)) {
          wide[, (nm) := 0]
        }
      }

      comp_cols <- c(female_cols, male_cols)
    } else {
      wide <- dcast(
        acomp,
        as.formula(
          paste(paste(group_vars, collapse = " + "), "~ BIN")
        ),
        value.var = "TOTAL",
        fill = 0
      )

      comp_cols <- as.character(bins)
      for (nm in comp_cols) {
        if (!nm %in% names(wide)) {
          wide[, (nm) := 0]
        }
      }
    }

    out <- merge(wide, nsamp_dt, by = group_vars, all.x = TRUE)
    out <- merge(out, fleet_key, by = group_vars, all.x = TRUE)

    out[, Seas := match(SEASON, sort(unique(SEASON)))]
    out[, Gender := if (sexed) 3L else 0L]
    out[, Part := 0L]
    if (comp_type == "age") {
      out[, Ageerr := as.integer(ageerr)]
    }
    out[, Lbin_lo := lbin_lo]
    out[, Lbin_hi := lbin_hi]
    out[, Nsamp := as.integer(Nsamp)]

    meta_cols <- c("Seas", "Gender", "Part")
    if (comp_type == "age") meta_cols <- c(meta_cols, "Ageerr")
    meta_cols <- c(meta_cols, "Lbin_lo", "Lbin_hi", "Nsamp")

    setcolorder(
      out,
      c(group_vars, "FleetLabel", "Source", meta_cols, comp_cols)
    )

    list(
      data = out[],
      comp_cols = comp_cols,
      fleet_key = unique(out[, .(Source, FleetLabel)])
    )
  }

  if (comp_type == "len") {
    data1 <- data[[1]]
    data2 <- data[[2]]
  } else {
    data1 <- data[[1]][[1]]
    data2 <- data[[1]][[2]]
  }

  survey_res <- .build_comp(
    dt = data1,
    group_vars = c("YEAR", "SEASON", "REGION_GRP"),
    fleet_label_fun = survey_label_fun,
    source_name = "fishery_single"
  )

  fishery_res <- .build_comp(
    dt = data2,
    group_vars = c("YEAR", "SEASON", "REGION_GRP", "GEAR"),
    fleet_label_fun = fishery_label_fun,
    source_name = "fishery_split"
  )

  fleet_map <- rbindlist(
    list(survey_res$fleet_key, fishery_res$fleet_key),
    use.names = TRUE,
    fill = TRUE
  )
  fleet_map <- unique(fleet_map)
  setorder(fleet_map, Source, FleetLabel)
  fleet_map[, Fleet := .I]

  survey_dt <- merge(
    survey_res$data,
    fleet_map,
    by = c("Source", "FleetLabel"),
    all.x = TRUE
  )

  fishery_dt <- merge(
    fishery_res$data,
    fleet_map,
    by = c("Source", "FleetLabel"),
    all.x = TRUE
  )

  comp_cols <- if (sexed) {
    c(paste0("F_", bins), paste0("M_", bins))
  } else {
    as.character(bins)
  }

  all_dt <- rbindlist(
    list(survey_dt, fishery_dt),
    use.names = TRUE,
    fill = TRUE
  )

  for (nm in comp_cols) {
    if (!nm %in% names(all_dt)) {
      all_dt[, (nm) := 0]
    }
  }

  setorder(all_dt, Fleet, YEAR)

  base_cols <- c("YEAR", "Seas", "Fleet", "Gender", "Part")
  if (comp_type == "age") {
    base_cols <- c(base_cols, "Ageerr")
  }
  base_cols <- c(base_cols, "Lbin_lo", "Lbin_hi", "Nsamp")

  ss_dt <- cbind(
    all_dt[, ..base_cols],
    all_dt[, ..comp_cols]
  )

  setnames(ss_dt, "Fleet", "FltSvy")

  data_lines <- apply(
    ss_dt,
    1,
    function(r) paste(fmt_num(as.numeric(r)), collapse = " ")
  )

  if (comp_type == "age") {
    block_name <- "#_agecomp"
    header_cols <- "#_year seas fleet gender part ageerr Lbin_lo Lbin_hi Nsamp datavector"
    type_comment <- "#_Age composition block"
  } else {
    block_name <- "#_lencomp"
    header_cols <- "#_year seas fleet gender part Lbin_lo Lbin_hi Nsamp datavector"
    type_comment <- "#_Length composition block"
  }

  header <- c(
    block_name,
    "#_Columns:",
    header_cols,
    type_comment,
    "#",
    "#_Fleet definitions:"
  )

  fleet_lines <- fleet_map[
    ,
    paste0("#_fleet ", Fleet, " = ", FleetLabel)
  ]

  if (sexed) {
    bin_comment <- c(
      paste0("#_Female bins: ", paste(bins, collapse = " ")),
      paste0("#_Male bins: ", paste(bins, collapse = " "))
    )
  } else {
    bin_comment <- paste0("#_Bins: ", paste(bins, collapse = " "))
  }

  end_line <- paste(c("-9999", rep("0", ncol(ss_dt) - 1)), collapse = " ")

  

  list(
    fishery_single = survey_dt,
    fishery_split = fishery_dt,
    file = file
  )
}