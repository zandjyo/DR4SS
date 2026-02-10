#' Build longline survey CPUE (RPN) index for Stock Synthesis
#'
#' Reads LL_RPN.sql from int/sql, filters by year/species/area/region, aggregates
#' RPN by year, fills missing years, and formats a Stock Synthesis CPUE table.
#'
#' @param con DBI connection to AKFIN.
#' @param ly Last year to include in the SQL query.
#' @param srv_sp_str Survey species code.
#' @param sp_area Area filter injected into SQL.
#' @param LL_sp_region Longline survey region code(s).
#' @param LLsrv_start_yr First year to include in the output CPUE.
#' @param fleet Integer SS fleet number to assign to this index (e.g. 3 or 5).
#' @param seas Season used in SS CPUE table (default 7).
#'
#' @return A data.frame with columns year, seas, index, obs, se_log
#' @export
build_ll_cpue <- function(con=conn$akfin,
                          ly = 2025,
                          srv_sp_str = 21720,
                          sp_area='BSAI',
                          LL_sp_region ='Bering Sea',
                          LLsrv_start_yr = 1990,
                          fleet=3,
                          seas = 7) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required.", call. = FALSE)
  }

  if (!is.numeric(fleet) || length(fleet) != 1) {
    stop("`fleet` must be a single numeric SS fleet number.", call. = FALSE)
  }

  DT <- data.table::as.data.table

  # ---- read and filter SQL ----
  sql_code <- sql_reader("LL_RPN.sql")

  sql_code <- sql_filter(sql_precode = "<=", x = ly,           sql_code = sql_code, flag = "-- insert year", value_type = c("numeric"))
  sql_code <- sql_filter(sql_precode = "IN", x = srv_sp_str,   sql_code = sql_code, flag = "-- insert species", value_type = c("numeric"))
  sql_code <- sql_filter(sql_precode = "IN", x = sp_area,      sql_code = sql_code, flag = "-- insert area", value_type = c("character"))
  sql_code <- sql_filter(sql_precode = "IN", x = LL_sp_region, sql_code = sql_code, flag = "-- insert region",value_type = c("character"))

  # ---- run query ----
  LL_RPN <- sql_run(con, sql_code) %>%
    dplyr::rename_all(tolower) %>%
    dplyr::group_by(year) %>%
    dplyr::summarise(
      rpn = sum(rpn, na.rm = TRUE),
      se  = sqrt(sum(rpn_var, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    DT()

  if (nrow(LL_RPN) == 0) {
    stop("LL RPN query returned 0 rows.", call. = FALSE)
  }

  # ---- restrict years ----
  LL_RPN <- LL_RPN[year >= LLsrv_start_yr]

  # ---- fill missing years ----
  yrs <- data.table::data.table(year = seq(min(LL_RPN$year), max(LL_RPN$year)))
  LL_RPN <- merge(LL_RPN, yrs, by = "year", all = TRUE)
  LL_RPN[is.na(LL_RPN)] <- 1

  # ---- build SS CPUE table ----
  LL_CPUE <- data.table::data.table(
    year   = LL_RPN$year,
    seas   = seas,
    index  = as.integer(fleet),
    obs    = LL_RPN$rpn,
    se_log = LL_RPN$se / LL_RPN$rpn
  )

  # SS convention: obs == 1 → negative fleet index
  LL_CPUE[obs == 1, index := -index]

  as.data.frame(LL_CPUE)
}
