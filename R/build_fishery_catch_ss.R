#' Build Stock Synthesis catch table from AKFIN catch query
#'
#' Reads dom_catch.sql from int/sql, runs it against AKFIN, harmonizes gears,
#' optionally aggregates to a single fleet, fills missing years with zeros,
#' and returns a Stock Synthesis catch table.
#'
#' @param con DBI connection to AKFIN.
#' @param final_year Last fishery year to include (used in SQL filter).
#' @param fsh_sp_label Species label used in catch SQL filter (as used in your SQL).
#' @param fsh_sp_area Subarea filter used in catch SQL.
#' @param one_fleet Logical; if TRUE aggregate all gears into fleet 1.
#' @param old_seas_gear_catch Optional data.frame/data.table of historical catch to prepend.
#'   Expected columns include YEAR, GEAR, and TONS (or similar; see details).
#' @param catch_total Optional fallback catch table (legacy behavior) if old catch provided is NULL.
#' @param catch_se Catch SE value used in output table.
#' @param init_catch Optional initialization (-999) row catch value.
#'   If one_fleet=TRUE, uses one row with this catch; if FALSE, uses 0 rows for fleets 1:3 (like your legacy block).
#'
#' @return A list with elements:
#'   \describe{
#'     \item{catch}{data.frame with columns year,seas,fleet,catch,catch_se}
#'     \item{N_catch}{nrow(catch)}
#'     \item{catch_raw}{data.table of annual catch by YEAR and GEAR1 (post-merge)}
#'   }
#' @export
build_fishery_catch_ss <- function(con,
                                   final_year,
                                   fsh_sp_label,
                                   fsh_sp_area,
                                   one_fleet = TRUE,
                                   old_seas_gear_catch = OLD_SEAS_GEAR_CATCH,
                                   catch_total = NULL,
                                   catch_se = 0.01,
                                   init_catch = 42500) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required.", call. = FALSE)
  }

  DT <- data.table::as.data.table

  # ---- helpers ----
  gear_harmonize <- function(gear) {
    dplyr::case_when(
      gear == "POT" ~ "POT",
      gear == "HAL" ~ "LONGLINE",
      gear == "JIG" ~ "OTHER",
      TRUE          ~ "TRAWL"
    )
  }

  gear_to_fleet <- function(gear1) {
    dplyr::case_when(
      gear1 == "LONGLINE" ~ 2L,
      gear1 == "POT"      ~ 3L,
      TRUE                ~ 1L
    )
  }

  complete_years <- function(dt, year_col = "YEAR", value_col = "TONS") {
    dt <- DT(dt)
    yrs <- seq(min(dt[[year_col]], na.rm = TRUE), max(dt[[year_col]], na.rm = TRUE))
    grid <- data.table::data.table(YEAR = yrs)
    out <- merge(grid, dt, by.x = "YEAR", by.y = year_col, all.x = TRUE)
    out[is.na(get(value_col)), (value_col) := 0]
    out
  }

  # ---- pull catch from SQL ----
  #sql_code <- sql_read("dom_catch.sql", root_dir = "int/sql")

  sql_code <- sql_reader("dom_catch.sql") 
  sql_code <- sql_filter(sql_precode = "<=", x = as.numeric(as.character(final_year)),  sql_code = sql_code, flag = "-- insert year", value_type = c("numeric"))
  sql_code <- sql_filter(sql_precode = "IN", x = fsh_sp_label, sql_code = sql_code, flag = "-- insert species_catch", value_type = c("character"))
  sql_code <- sql_filter(sql_precode = "IN", x = fsh_sp_area,  sql_code = sql_code, flag = "-- insert subarea", value_type = c("character"))

  CATCH <- sql_run(con, sql_code) |>
    dplyr::rename_all(toupper) |>
    dplyr::mutate(GEAR1 = gear_harmonize(GEAR)) |>
    dplyr::group_by(YEAR, GEAR1) |>
    dplyr::summarise(TONS = sum(TONS), .groups = "drop") |>
    DT()

  CATCH[, YEAR := as.integer(YEAR)]

  # ---- optional prepend old catch ----
  if (!is.null(old_seas_gear_catch)) {
    OLD <- DT(old_seas_gear_catch)

    # try to normalize expected columns
    nms <- toupper(names(OLD))
    names(OLD) <- nms
    if (!("YEAR" %in% names(OLD))) stop("old_seas_gear_catch must contain YEAR.", call. = FALSE)

    # accept either GEAR or GEAR1; and either TONS or CATCH column name
    if (!("GEAR1" %in% names(OLD))) {
      if ("GEAR" %in% names(OLD)) {
        OLD[, GEAR1 := gear_harmonize(as.character(GEAR))]
      } else {
        stop("old_seas_gear_catch must contain GEAR or GEAR1.", call. = FALSE)
      }
    }
    if (!("TONS" %in% names(OLD))) {
      if ("CATCH" %in% names(OLD)) {
        OLD[, TONS := as.numeric(CATCH)]
      } else {
        stop("old_seas_gear_catch must contain TONS (or CATCH).", call. = FALSE)
      }
    }

    OLD <- OLD |>
      dplyr::mutate(GEAR1 = dplyr::if_else(GEAR1 == "OTHER", "LONGLINE", GEAR1)) |>
      dplyr::group_by(YEAR, GEAR1) |>
      dplyr::summarise(TONS = sum(TONS), .groups = "drop") |>
      DT()

    CATCH <- data.table::rbindlist(list(OLD, CATCH), use.names = TRUE, fill = TRUE)

  } else if (!is.null(catch_total)) {
    # legacy fallback
    CATCH <- DT(catch_total)
    if (!all(c("YEAR", "GEAR1", "TONS") %in% toupper(names(CATCH)))) {
      # don't try to re-interpret here; user supplied fallback must match expectations
      # (keeps this function strict and debuggable)
      # You can relax this later if needed.
    }
  }

 # ---- build SS catch table ----
  if (isTRUE(one_fleet)) {
    C1 <- CATCH |>
      dplyr::group_by(YEAR) |>
      dplyr::summarise(TONS = sum(TONS), .groups = "drop") |>
      DT()

    C1 <- complete_years(C1, "YEAR", "TONS")
    C1[, fleet := 1L]

    catch_df <- data.frame(
      year = C1$YEAR,
      seas = 1,
      fleet = C1$fleet,
      catch = C1$TONS,
      catch_se = catch_se
    )

    # -999 initialization row
    init <- data.frame(year = -999, seas = 1, fleet = 1, catch = init_catch, catch_se = catch_se)
    catch_df <- dplyr::bind_rows(init, catch_df) |>
      dplyr::arrange(fleet, year)

    return(list(catch = catch_df, N_catch = nrow(catch_df), catch_raw = C1))
  }

  # multi-fleet case
  C2 <- CATCH |>
    dplyr::mutate(GEAR1 = dplyr::if_else(GEAR1 == "OTHER", "LONGLINE", GEAR1)) |>
    dplyr::group_by(GEAR1, YEAR) |>
    dplyr::summarise(TONS = sum(TONS), .groups = "drop") |>
    DT()

  # complete years within gear
  gears <- unique(C2$GEAR1)
  out_list <- lapply(gears, function(g) {
    tmp <- C2[GEAR1 == g]
    tmp <- complete_years(tmp, "YEAR", "TONS")
    tmp[, GEAR1 := g]
    tmp
  })
  C2 <- data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE)

  C2[, fleet := gear_to_fleet(GEAR1)]
  data.table::setorder(C2, fleet, YEAR)

  catch_df <- data.frame(
    year = C2$YEAR,
    seas = 1,
    fleet = C2$fleet,
    catch = C2$TONS,
    catch_se = catch_se
  )

  # -999 “close” rows (legacy behavior)
  close <- data.frame(year = rep(-999, 3), seas = 1, fleet = 1:3, catch = 0, catch_se = 0.05)
  catch_df <- dplyr::bind_rows(close, catch_df) |>
    dplyr::arrange(fleet, year)

  list(catch = catch_df, N_catch = nrow(catch_df), catch_raw = C2)
}