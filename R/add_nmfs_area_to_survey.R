#' Add NMFS area codes to survey data using spatial join
#'
#' Survey data do not contain NMFS area codes. This function assigns NMFS areas
#' by spatially joining haul end locations (LATITUDE_DD_END, LONGITUDE_DD_END)
#' to the NMFS area polygon layer bundled with the DR4SS package.
#'
#' Longitude convention: survey longitudes are positive west of 180 and negative
#' east of 180; values are flipped internally to standard GIS (-180, 180).
#'
#' @param survey_dt data.frame or data.table with survey records.
#' @param lon_col Longitude column name (default "LONGITUDE_DD_END").
#' @param lat_col Latitude column name (default "LATITUDE_DD_END").
#' @param join How to assign areas: "within" or "nearest" (default "nearest").
#'
#' @return data.table with new integer column NMFS_AREA.
#' @export
add_nmfs_area_to_survey <- function(survey_dt,
                                    lon_col = "LONGITUDE_DD_END",
                                    lat_col = "LATITUDE_DD_END",
                                    join = c("within", "nearest")) {

  join <- match.arg(join)

  if (!requireNamespace("sf", quietly = TRUE))
    stop("Package 'sf' is required.", call. = FALSE)
  if (!requireNamespace("data.table", quietly = TRUE))
    stop("Package 'data.table' is required.", call. = FALSE)

  DT <- data.table::as.data.table
  d  <- DT(survey_dt)

  if (!all(c(lon_col, lat_col) %in% names(d))) {
    stop("Survey data must contain ", lon_col, " and ", lat_col, call. = FALSE)
  }

  # ---- locate NMFS area polygons bundled with DR4SS ----
  shape_path <- system.file("extdata", "SHAPE", "NMFS_AREAS.shp", package = "DR4SS")
  if (shape_path == "")
    stop("NMFS_AREAS shapefile not found in DR4SS/extdata.", call. = FALSE)

  polys <- sf::st_read(shape_path, quiet = TRUE)

  # ---- prepare coordinates ----
  suppressWarnings({
    d[, (lon_col) := as.numeric(get(lon_col))]
    d[, (lat_col) := as.numeric(get(lat_col))]
  })

  d <- d[is.finite(get(lon_col)) & is.finite(get(lat_col))]



  pts <- sf::st_as_sf(
    d,
    coords = c(lon_col, lat_col),
    crs = 4326,
    remove = FALSE
  )

  # ensure CRS match
  polys <- sf::st_transform(polys, 4326)

  # ---- spatial join ----
  if (join == "within") {
    j <- sf::st_join(pts, polys["NMFS_AREA"], join = sf::st_within, left = TRUE)
  } else {
    j <- sf::st_join(pts, polys["NMFS_AREA"], join = sf::st_within, left = TRUE)
    miss <- is.na(j$NMFS_AREA)
    if (any(miss)) {
      idx <- sf::st_nearest_feature(j[miss, ], polys)
      j$NMFS_AREA[miss] <- polys$NMFS_AREA[idx]
    }
  }

  out <- data.table::as.data.table(sf::st_drop_geometry(j))
  out[, NMFS_AREA := as.integer(NMFS_AREA)]
  out
}
