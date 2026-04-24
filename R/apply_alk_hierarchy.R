#' Apply a hierarchy of age-length keys (ALKs) to haul-length data
#'
#' Matches haul-level length-frequency data to a hierarchy of
#' age-length keys (ALKs), using progressively broader keys when
#' more specific matches are unavailable.
#'
#' The function expands each haul-length observation into
#' haul-length-age rows by applying the conditional probabilities
#' \eqn{P(\text{AGE} \mid \text{LENGTH})} from the best available ALK.
#'
#' @param freq_df A data.frame or data.table containing haul-level
#'   abundance-at-length data with required columns:
#'   \code{YEAR}, \code{REGION}, \code{HAULJOIN},
#'   \code{SPECIES_CODE}, \code{SEX}, \code{LENGTH},
#'   and \code{N_LENGTH}.
#'
#' @param alk_list A named list of ALKs as returned by
#'   \code{build_alk_hierarchy()}, containing:
#' \describe{
#'   \item{alk_y_r_s}{ALK by YEAR + REGION + SPECIES_CODE + SEX + LENGTH}
#'   \item{alk_r_s}{ALK by REGION + SPECIES_CODE + SEX + LENGTH}
#'   \item{alk_s}{ALK by SPECIES_CODE + SEX + LENGTH}
#'   \item{alk_all}{ALK by SPECIES_CODE + LENGTH}
#' }
#'
#' @return A data.frame containing haul-length-age data with columns:
#' \describe{
#'   \item{YEAR}{Integer survey year.}
#'   \item{REGION}{Survey region.}
#'   \item{HAULJOIN}{Haul identifier.}
#'   \item{SPECIES_CODE}{Species code.}
#'   \item{SEX}{Sex category.}
#'   \item{LENGTH}{Length bin.}
#'   \item{AGE}{Age (or plus group if applied upstream).}
#'   \item{N}{Number of aged fish used in ALK cell.}
#'   \item{TOTAL_N}{Total number of fish in ALK length bin.}
#'   \item{ALK_PROP}{Proportion at age given length.}
#'   \item{N_LENGTH}{Abundance at length (input from \code{freq_df}).}
#'   \item{ALK_LEVEL}{Indicator of which ALK level was used for matching.}
#' }
#'
#' @details
#' Matching proceeds sequentially using the following hierarchy:
#' \enumerate{
#'   \item YEAR + REGION + SPECIES_CODE + SEX + LENGTH
#'   \item REGION + SPECIES_CODE + SEX + LENGTH
#'   \item SPECIES_CODE + SEX + LENGTH
#'   \item SPECIES_CODE + LENGTH
#' }
#'
#' For each haul-length observation, the function attempts to match
#' to the most specific ALK. If no match is found, progressively
#' broader ALKs are used until a match is obtained.
#'
#' Observations are expanded across all ages present in the matched
#' ALK, allowing subsequent calculation of abundance-at-age.
#'
#' A warning is issued if any haul-length rows remain unmatched after
#' all fallback levels are applied.
#'
#' @export
apply_alk_hierarchy <- function(freq_df, alk_list) {

  req_freq <- c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE", "SEX", "LENGTH", "N_LENGTH")
  if (!all(req_freq %in% names(freq_df))) {
    stop("freq_df is missing required columns: ",
         paste(setdiff(req_freq, names(freq_df)), collapse = ", "))
  }

  req_alk <- c("alk_y_r_s", "alk_r_s", "alk_s", "alk_all")
  if (!all(req_alk %in% names(alk_list))) {
    stop("alk_list must contain: ", paste(req_alk, collapse = ", "))
  }

  key_cols <- c("YEAR", "REGION", "HAULJOIN", "SPECIES_CODE", "SEX", "LENGTH")

  matched <- list()
  remaining <- unique(freq_df[, key_cols])

  # helper
  get_unmatched <- function(all_df, matched_keys) {
    if (nrow(matched_keys) == 0) return(all_df)
    idx <- do.call(paste, all_df[, key_cols, drop = FALSE])
    used <- do.call(paste, matched_keys[, key_cols, drop = FALSE])
    all_df[!idx %in% used, , drop = FALSE]
  }

  # level 1: YEAR + REGION + SPECIES_CODE + SEX + LENGTH
  tmp <- merge(
    freq_df,
    alk_list$alk_y_r_s,
    by = c("YEAR", "REGION", "SPECIES_CODE", "SEX", "LENGTH"),
    all = FALSE
  )
  if (nrow(tmp) > 0) {
    tmp$ALK_LEVEL <- "alk_y_r_s"
    matched[[length(matched) + 1]] <- tmp
    remaining <- get_unmatched(freq_df, unique(tmp[, key_cols, drop = FALSE]))
  } else {
    remaining <- freq_df
  }

  # level 2: REGION + SPECIES_CODE + SEX + LENGTH
  if (nrow(remaining) > 0) {
    tmp <- merge(
      remaining,
      alk_list$alk_r_s,
      by = c("REGION", "SPECIES_CODE", "SEX", "LENGTH"),
      all = FALSE
    )
    if (nrow(tmp) > 0) {
      tmp$ALK_LEVEL <- "alk_r_s"
      matched[[length(matched) + 1]] <- tmp
      remaining <- get_unmatched(remaining, unique(tmp[, key_cols, drop = FALSE]))
    }
  }

  # level 3: SPECIES_CODE + SEX + LENGTH
  if (nrow(remaining) > 0) {
    tmp <- merge(
      remaining,
      alk_list$alk_s,
      by = c("SPECIES_CODE", "SEX", "LENGTH"),
      all = FALSE
    )
    if (nrow(tmp) > 0) {
      tmp$ALK_LEVEL <- "alk_s"
      matched[[length(matched) + 1]] <- tmp
      remaining <- get_unmatched(remaining, unique(tmp[, key_cols, drop = FALSE]))
    }
  }

  # level 4: SPECIES_CODE + LENGTH
  if (nrow(remaining) > 0) {
    tmp <- merge(
      remaining,
      alk_list$alk_all,
      by = c("SPECIES_CODE", "LENGTH"),
      all = FALSE
    )
    if (nrow(tmp) > 0) {
      tmp$ALK_LEVEL <- "alk_all"
      matched[[length(matched) + 1]] <- tmp
      remaining <- get_unmatched(remaining, unique(tmp[, key_cols, drop = FALSE]))
    }
  }

  if (length(matched) == 0) {
    stop("No rows matched any ALK level.")
  }

  dat2 <- data.table::rbindlist(matched, fill = TRUE)
  dat2 <- as.data.frame(dat2)

  if (nrow(remaining) > 0) {
    warning("Some haul-sex-length rows still had no ALK match after fallback.")
  }

  dat2
}