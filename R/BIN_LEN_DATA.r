# adapted/generalized from Steve Barbeaux' files for
# generating SS files for EBS/AI Greenland Turbot
# ZTA, 2013-05-08, R version 2.15.1, 32-bit

#' Bin length observations into Stock Synthesis format
#'
#' Assigns individual fish length observations to discrete length bins defined
#' by \code{len_bins}, following Stock Synthesis (SS) data-file conventions.
#' Each observed length is mapped to the **lower edge** of the bin interval
#' containing it; all lengths greater than or equal to the maximum bin value
#' are assigned to the terminal bin.
#'
#' This function assumes that all length observations are positive integers.
#'
#' @param data A data frame or data.table containing a numeric column named
#'   \code{LENGTH}. All values in \code{LENGTH} must be positive integers.
#' @param len_bins A numeric vector of bin lower edges (e.g.,
#'   \code{seq(10, 140, 2)}), sorted in increasing order. The maximum value
#'   represents the terminal (plus) bin.
#'
#' @return The input \code{data} with an added column \code{BIN}, giving the
#'   assigned Stock Synthesis length bin for each observation.
#'
#' @details
#' Bin assignment follows these rules:
#' \itemize{
#'   \item For intermediate bins \code{k = 1, \dots, n-1},
#'     \code{BIN = len_bins[k]} when
#'     \code{len_bins[k] <= LENGTH < len_bins[k + 1]}.
#'   \item For \code{LENGTH >= max(len_bins)}, \code{BIN = max(len_bins)}.
#' }
#'
#' Internally, the function constructs a lookup table for all integer lengths
#' from 1 through \code{max(LENGTH)} and merges this table back onto the input
#' data. This approach ensures deterministic bin assignment and is efficient
#' for typical fisheries length ranges.
#'
#' @examples
#' len_bins <- seq(10, 20, 2)
#' d <- data.frame(LENGTH = c(10, 11, 12, 18, 20, 21))
#' BIN_LEN_DATA(d, len_bins)
#'
#' @export

BIN_LEN_DATA <- function(data,len_bins=len_bins)
{
    length<-data.frame(LENGTH=c(1:max(data.table(data)[!is.na(LENGTH)]$LENGTH)))
    length$BIN<-max(len_bins)
    n<-length(len_bins)
    for(i in 2:n-1)
    {
       length$BIN[length$LENGTH < len_bins[((n-i)+1)] ]<-len_bins[n-i]
    }
    data<-merge(data,length,all.x=T,all.y=F,by="LENGTH")
    data
}



