#' Connect to AFSC and AKFIN databases
#'
#' Uses keyring to securely retrieve credentials and opens DBI ODBC connections.
#'
#' @param afsc_dsn ODBC DSN name for AFSC (default "afsc")
#' @param akfin_dsn ODBC DSN name for AKFIN (default "akfin")
#' @param afsc_service keyring service name for AFSC credentials (default "afsc")
#' @param akfin_service keyring service name for AKFIN credentials (default "akfin")
#' @param afsc_user Optional username override (otherwise pulled from keyring)
#' @param akfin_user Optional username override (otherwise pulled from keyring)
#' @param validate If TRUE, checks DBI::dbIsValid on returned connections
#'
#' @return A list with AFSC and AKFIN DBI connections: list(afsc=..., akfin=...)
#' @export
db_connect <- function(afsc_dsn = "afsc",
                       akfin_dsn = "akfin",
                       afsc_service = "afsc",
                       akfin_service = "akfin",
                       afsc_user = NULL,
                       akfin_user = NULL,
                       validate = TRUE) {

  .get_user <- function(service) {
    kl <- keyring::key_list(service)
    if (nrow(kl) < 1L) stop("No keyring entries found for service: ", service, call. = FALSE)
    if (!"username" %in% names(kl)) stop("keyring::key_list() returned no `username` column for: ", service, call. = FALSE)
    # Choose first username by default (can be made smarter later)
    kl$username[[1]]
  }

  .get_pwd <- function(service, user) {
    tryCatch(
      keyring::key_get(service = service, username = user),
      error = function(e) stop("Failed to get password from keyring for service=", service,
                               ", username=", user, "\n", conditionMessage(e), call. = FALSE)
    )
  }

  if (is.null(afsc_user))  afsc_user  <- .get_user(afsc_service)
  if (is.null(akfin_user)) akfin_user <- .get_user(akfin_service)

  afsc_pwd  <- .get_pwd(afsc_service, afsc_user)
  akfin_pwd <- .get_pwd(akfin_service, akfin_user)

  afsc <- tryCatch(
    DBI::dbConnect(odbc::odbc(), afsc_dsn, UID = afsc_user, PWD = afsc_pwd),
    error = function(e) stop("AFSC connection failed (DSN=", afsc_dsn, ")\n",
                             conditionMessage(e), call. = FALSE)
  )

  akfin <- tryCatch(
    DBI::dbConnect(odbc::odbc(), akfin_dsn, UID = akfin_user, PWD = akfin_pwd),
    error = function(e) stop("AKFIN connection failed (DSN=", akfin_dsn, ")\n",
                             conditionMessage(e), call. = FALSE)
  )

  if (isTRUE(validate)) {
    if (!DBI::dbIsValid(afsc))  stop("AFSC connection is not valid after connect().", call. = FALSE)
    if (!DBI::dbIsValid(akfin)) stop("AKFIN connection is not valid after connect().", call. = FALSE)
  }

  list(afsc = afsc, akfin = akfin)
}
