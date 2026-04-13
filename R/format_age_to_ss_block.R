
#' Format age composition data to a Stock Synthesis (.ss) agecomp block
#'
#' This function processes survey and fishery age-composition data and formats
#' them into a ready-to-write Stock Synthesis (SS) age composition block.
#' It aggregates frequencies, normalizes proportions within strata, fills in
#' missing age bins (including age-0 if required), and outputs a properly
#' structured composition block with fleet definitions.
#'
#' The function assumes input data are structured as:
#' \itemize{
#'   \item data[[1]][[1]]: survey age composition data
#'   \item data[[1]][[2]]: fishery age composition data
#' }
#'
#' Required columns in input data:
#' \itemize{
#'   \item AGE: age bin
#'   \item FREQ: frequency/count
#'   \item NHAUL: number of hauls/samples
#'   \item YEAR, SEASON, REGION_GRP
#'   \item SEX (if sexed = TRUE)
#'   \item GEAR (for fishery data)
#' }
#'
#' @param data Nested list containing survey and fishery age composition data.
#' @param bins Integer vector of age bins to include (default = 0:10).
#'   Age-0 will be included in output even if not present in data, with values set to 0.
#' @param sexed Logical. If TRUE, outputs female and male compositions separately
#'   (Gender = 3). If FALSE, outputs pooled compositions (Gender = 0).
#' @param file Optional character string. If provided, writes the SS agecomp block
#'   directly to this file.
#'
#' @details
#' \itemize{
#'   \item Missing age bins are filled with zeros to ensure consistent bin structure.
#'   \item NHAUL is handled as an integer and used as Nsamp in SS output.
#'   \item Fleet IDs are automatically generated and mapped from survey and fishery strata.
#'   \item Output includes the required SS terminator line (-9999 ...).
#' }
#'
#' @return A list containing:
#' \itemize{
#'   \item ONE_FLEET: formatted survey composition data
#'   \item SPLIT_FLEET: formatted fishery composition data
#'   \item file: file path if output was written, otherwise NULL
#' }
#'
#' @examples
#' \dontrun{
#' res <- format_age_to_ss_block(out3, bins = 0:10, sexed = TRUE)
#' cat(paste(res$block, collapse = "\n"))
#'
#' # Write directly to file
#' format_age_to_ss_block(out3, file = "agecomp_block.ss")
#' }
#'
#' @import data.table
#' @export

format_age_to_ss_block <- function(data,
                                   bins = 0:10,
                                   sexed = TRUE,
                                   file = NULL) {

  safe_max_int <- function(x) {
    x <- x[!is.na(x)]
    if (length(x) == 0) return(NA_integer_)
    as.integer(max(x))
  }

  fmt_num <- function(x) {
    ifelse(
      abs(x - round(x)) < 1e-12,
      as.character(as.integer(round(x))),
      formatC(x, format = "f", digits = 6, drop0trailing = TRUE)
    )
  }

  .build_comp <- function(dt, group_vars, fleet_label_fun, source_name) {
    dt <- copy(as.data.table(dt))

    needed <- c("AGE", "FREQ", "NHAUL", "YEAR", "SEASON", "REGION_GRP")
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

    dt[, BIN := AGE]

    # Aggregate observed ages only; age-0 or other missing bins get filled later
    acomp <- dt[
      BIN %in% bins,
      .(
        TOTAL = sum(FREQ, na.rm = TRUE),
        NHAUL = safe_max_int(NHAUL)
      ),
      by = c("BIN", group_vars, "SEX")
    ]

    # Normalize within YEAR/SEASON/REGION[/GEAR]
    acomp[
      ,
      T_NUMBER := sum(TOTAL, na.rm = TRUE),
      by = group_vars
    ][
      ,
      TOTAL := fifelse(T_NUMBER > 0, TOTAL / T_NUMBER, 0)
    ]

    # Complete all combinations so missing bins (including age 0) are present
    combo_list <- lapply(group_vars, function(v) sort(unique(acomp[[v]])))
    names(combo_list) <- group_vars
    combo_list[["SEX"]] <- if (sexed) c("F", "M") else "U"
    combo_list[["BIN"]] <- bins

    all_combos <- do.call(CJ, c(combo_list, list(unique = TRUE)))

    setkeyv(acomp, c(group_vars, "SEX", "BIN"))
    setkeyv(all_combos, c(group_vars, "SEX", "BIN"))
    acomp <- all_combos[acomp]

    acomp[is.na(TOTAL), TOTAL := 0]

    # NHAUL should remain integer
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

    out[, Seas   := match(SEASON, sort(unique(SEASON)))]
    out[, Gender := if (sexed) 3L else 0L]
    out[, Part   := 0L]
    out[, Ageerr := 1L]
    out[, Lbin_lo := as.integer(min(bins))]
    out[, Lbin_hi := as.integer(max(bins))]
    out[, Nsamp := as.integer(Nsamp)]

    setcolorder(
      out,
      c(
        group_vars, "FleetLabel", "Source",
        "Seas", "Gender", "Part", "Ageerr",
        "Lbin_lo", "Lbin_hi", "Nsamp", comp_cols
      )
    )

    list(
      data = out[],
      comp_cols = comp_cols,
      fleet_key = unique(out[, .(Source, FleetLabel)])
    )
  }

  # Survey block
  survey_res <- .build_comp(
    dt = data[[1]][[1]],
    group_vars = c("YEAR", "SEASON", "REGION_GRP"),
    fleet_label_fun = function(z) paste0("FISH_", z$REGION_GRP, "_", z$SEASON),
    source_name = "fishery"
  )

  # Fishery block
  fishery_res <- .build_comp(
    dt = data[[1]][[2]],
    group_vars = c("YEAR", "SEASON", "REGION_GRP", "GEAR"),
    fleet_label_fun = function(z) paste0("FISH_", z$REGION_GRP, "_", z$GEAR, "_", z$SEASON),
    source_name = "fishery_gear"
  )

  # Fleet map
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

  ss_dt <- all_dt[
    ,
    c(
      list(
        YEAR    = as.integer(YEAR),
        Seas    = as.integer(Seas),
        FltSvy  = as.integer(Fleet),
        Gender  = as.integer(Gender),
        Part    = as.integer(Part),
        Ageerr  = as.integer(Ageerr),
        Lbin_lo = as.integer(Lbin_lo),
        Lbin_hi = as.integer(Lbin_hi),
        Nsamp   = as.integer(Nsamp)
      ),
      .SD
    ),
    .SDcols = comp_cols
  ]

  data_lines <- apply(
    ss_dt,
    1,
    function(r) paste(fmt_num(as.numeric(r)), collapse = " ")
  )

  end_line <- paste(c("-9999", rep("0", ncol(ss_dt) - 1)), collapse = " ")

  header <- c(
    "#_agecomp",
    "#_Columns:",
    "#_year seas fleet gender part ageerr Lbin_lo Lbin_hi Nsamp datavector",
    "#_Age-0 bin included in output; values are zero when not observed.",
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

  block <- c(
    header,
    fleet_lines,
    "#",
    bin_comment,
    "#",
    data_lines,
    end_line
  )

  if (!is.null(file)) {
    writeLines(block, con = file)
  }

  list(
    ONE_FLEET = survey_dt,
    SPLIT_FLEET = fishery_dt,
    file = file
  )
}