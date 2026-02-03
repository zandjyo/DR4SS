#' Disconnect DBI connections safely
#' @export
db_disconnect <- function(con_list) {
  for (nm in names(con_list)) {
    con <- con_list[[nm]]
    if (inherits(con, "DBIConnection") && DBI::dbIsValid(con)) {
      try(DBI::dbDisconnect(con), silent = TRUE)
    }
  }
  invisible(TRUE)
}
