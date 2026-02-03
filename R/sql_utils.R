# sql_utils.R
# Utilities for reading SQL templates from an installed package and safely
# inserting filter clauses at placeholder flags.

#' Read a SQL file either from installed package extdata or from a direct path
#'
#' @param x File name like "afsc/catch.sql" OR a full/relative file path.
#' @param package Package name to search in. Defaults to current package.
#' @param root_dir Subfolder inside extdata (default "sql").
#'
#' @return Character vector of SQL lines.
#' @export
sql_read <- function(x, package = utils::packageName(), root_dir = "sql") {

  # 1) direct path (development-friendly)
  if (file.exists(x)) {
    return(readLines(x, warn = FALSE))
  }

  # 2) package-installed path
  pkg_path <- system.file("extdata", root_dir, x, package = package)
  if (!nzchar(pkg_path) || !file.exists(pkg_path)) {
    stop(
      "SQL file not found.\n",
      "Tried direct path: ", x, "\n",
      "Tried package path: inst/extdata/", root_dir, "/", x, " (package: ", package, ")",
      call. = FALSE
    )
  }

  readLines(pkg_path, warn = FALSE)
}

#' Read a SQL file bundled with DR4SS-NP
#'
#' @param filename SQL file name (e.g., "LL_RPN.sql")
#' @param package Package name (default "DR4SS-NP")
#' @return Character vector of SQL lines
#' @export
sql_reader <- function(filename) {

  stopifnot(is.character(filename), length(filename) == 1)

  sql_file <- system.file("sql", filename, package = "DR4SS")

  if (!nzchar(sql_file)) {
    stop("SQL file not found in DR4SS/inst/sql/: ", filename,
         call. = FALSE)
  }

  readLines(sql_file, warn = FALSE)
}




#' Collapse numeric values into a SQL list (unquoted)
#'
#' @param x Numeric/integer vector. NAs are dropped.
#' @return A length-1 character string like "1,2,3". Returns "NULL" if empty.
#' @keywords internal
collapse_filters_num <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0L) return("NULL")

  x <- as.numeric(x)
  if (any(!is.finite(x))) stop("Non-finite values in numeric filter.", call. = FALSE)

  paste(format(x, trim = TRUE, scientific = FALSE), collapse = ",")
}

#' Collapse character values into a SQL list (quoted)
#'
#' @param x Character vector. NAs are dropped.
#' @return A length-1 character string like "'a','b'". Returns "NULL" if empty.
#' @keywords internal
collapse_filters_chr <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0L) return("NULL")

  x <- as.character(x)
  x <- gsub("'", "''", x, fixed = TRUE)
  paste0("'", paste(x, collapse = "','"), "'")
}

#' Replace a placeholder line in a SQL template
#'
#' @param replacement Replacement text to insert (single string).
#' @param sql_code Character vector of SQL lines.
#' @param flag Placeholder string to search for (exact match via fixed grep).
#' @param multiple If TRUE, replaces all matches; otherwise requires exactly one match.
#'
#' @return Modified SQL as a character vector.
#' @export
sql_add <- function(replacement, sql_code, flag = "-- insert table", multiple = FALSE) {
  i <- grep(flag, sql_code, fixed = TRUE)
  if (length(i) == 0L) stop("Flag not found in SQL template: ", flag, call. = FALSE)
  if (!multiple && length(i) > 1L) stop("Flag matched multiple lines: ", flag, call. = FALSE)

  sql_code[i] <- replacement
  sql_code
}

#' Insert a filter expression into a SQL template at a placeholder flag
#'
#' @param sql_precode SQL code preceding the filter (e.g., "IN", "=", ">=", "<").
#' @param x Values to insert (numeric or character).
#' @param sql_code Character vector of SQL lines.
#' @param flag Placeholder line to be replaced.
#' @param value_type One of "numeric" or "character".
#' @param multiple If TRUE, replaces all matches of flag; otherwise requires exactly one.
#'
#' @return Modified SQL as a character vector.
#' @export
sql_filter <- function(sql_precode = "IN",
                       x,
                       sql_code,
                       flag = "-- insert species",
                       value_type = c("numeric", "character"),
                       multiple = FALSE) {
  value_type <- match.arg(value_type)

  i <- grep(flag, sql_code, fixed = TRUE)
  if (length(i) == 0L) stop("Flag not found in SQL template: ", flag, call. = FALSE)
  if (!multiple && length(i) > 1L) stop("Flag matched multiple lines: ", flag, call. = FALSE)

  rhs <- switch(
    value_type,
    numeric   = collapse_filters_num(x),
    character = collapse_filters_chr(x)
  )

  # For "IN", wrap in parentheses list; for scalar operators (=, >=, <, etc.)
  # also wrap in parentheses to match legacy behavior in existing templates.
  # If you prefer scalar operators without parens, change to paste0(sql_precode, " ", rhs).
  sql_code[i] <- paste0(sql_precode, " (", rhs, ")")
  sql_code
}

#' Run a SQL query via DBI
#'
#' @param database A DBI connection.
#' @param query Character vector of SQL lines or a single SQL string.
#' @param as.is Passed to \code{DBI::dbGetQuery()}.
#'
#' @return A data.frame result from \code{DBI::dbGetQuery()}.
#' @export
sql_run <- function(database, query, as.is = TRUE) {
  if (length(query) > 1L) query <- paste(query, collapse = "\n")

  tryCatch(
    DBI::dbGetQuery(database, query, as.is = as.is, believeNRows = FALSE),
    error = function(e) {
      stop("SQL query failed:\n", conditionMessage(e), "\n\nQuery:\n", query, call. = FALSE)
    }
  )
}
