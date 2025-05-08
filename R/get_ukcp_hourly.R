#' UKCP18 Data Scraper
#'
#' This script downloads and processes UKCP18 (UK Climate Projections 2018) data from the CEDA archive.
#' The data is downloaded as NetCDF files and converted to HIVE-partitioned parquet files for efficient storage
#' and querying. The data is organized in a hierarchical structure:
#'
#' ukcp_hourly/
#' ├── run={run_id}/           # Different climate model runs
#' │   ├── variable={var}/     # Climate variables (precipitation, temperature)
#' │   │   ├── year={year}/    # Year of the data
#' │   │   │   ├── month={month}/  # Month of the data
#' │   │   │   │   ├── file-1.parquet  # Processed data file
#'
#' @author [Your Name]
#' @date [Current Date]

library(httr2)
library(rvest)
library(stringr)
library(purrr)
library(glue)
library(here)

# Base URL for the CEDA archive containing UKCP18 data
base <- "https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/5km/rcp85/{run}/{variable}/1hr/v20210615"

# Define the climate model runs to process
# Each run represents a different climate model simulation
runs <- c(1, 4, 5, 6, 7, 8, 9, 10)

# Define the climate variables to process
variables <- c(
  "pr", # Precipitation in mm/day
  "tas" # Mean air temperature in degrees Celsius
)

# Main processing loop
for (run in runs) {
  for (variable in variables) {
    # Pad run number with leading zeros for consistent formatting
    run <- stringr::str_pad(run, width = 2, side = "left", pad = "0")

    # Create directory structure for the current run and variable
    file_path <- glue::glue("ukcp_hourly/run={run}/variable={variable}")
    if (!dir.exists(file_path)) {
      dir.create(file_path, recursive = TRUE)
    }

    # Construct the base URL for the current run and variable
    base_url <- glue::glue(base)

    # Fetch the index page containing file listings
    index_resp <- request(base_url) |>
      req_headers(Authorization = paste("Bearer", Sys.getenv("CEDA_TOKEN"))) |>
      req_perform()

    # Extract download URLs from the index page
    urls <- index_resp |>
      resp_body_html() |>
      html_elements("a") |>
      html_attrs() |>
      map(pluck, "href") |>
      unlist()

    # Filter for NetCDF download URLs and take every other one to avoid duplicates
    urls <- urls[str_detect(urls, "\\.nc\\?download=1")]
    urls <- urls[seq(1, length(urls), by = 2)]

    # For testing purposes, limit to first two files
    # urls <- urls[1:2]

    # Process each NetCDF file
    for (url in urls) {
      # Extract date range from filename
      from_to <- stringr::str_extract_all(
        url,
        "_[0-9]{8}-[0-9]{8}"
      )[[1]] |>
        stringr::str_replace("_", "") |>
        stringr::str_split_1("-")

      # Calculate date range for the current file
      from <- as.Date(from_to[1], format = "%Y%m%d")
      to <- from + lubridate::dmonths(1) - lubridate::days(1)

      # Extract year and month for directory structure
      year <- lubridate::year(from)
      month <- lubridate::month(from)

      # Create directory for the current year and month
      file_path <- glue::glue(
        "ukcp_hourly/run={run}/variable={variable}/year={year}/month={month}"
      )

      if (!dir.exists(file_path)) {
        dir.create(
          file_path,
          recursive = TRUE
        )
      }

      # Skip if file already exists
      if (file.exists(here::here(file_path, "file-1.parquet"))) {
        next
      }

      # Download and process the NetCDF file
      tmp <- tempfile()
      resp <- request(url) |>
        req_headers(
          Authorization = paste("Bearer", Sys.getenv("CEDA_TOKEN"))
        ) |>
        req_perform(path = tmp)
      temp_d <- ncdf4::nc_open(tmp)

      # Determine summary function based on variable type
      summary_fn <- dplyr::if_else(
        variable %in% c("pr"),
        "max", # Use maximum for precipitation
        "mean" # Use mean for temperature
      )

      # Extract and summarize the climate data
      summary_data <- ncdf4::ncvar_get(temp_d, variable) |>
        apply(MARGIN = c(1, 2), FUN = summary_fn)

      # Extract latitude and longitude information
      lat <- ncdf4::ncvar_get(temp_d, "latitude")
      long <- ncdf4::ncvar_get(temp_d, "longitude")

      # Create a tidy data frame with the processed data
      out <- tidyr::crossing(
        proj_x = 1:180, # Projection x-coordinate
        proj_y = 1:244 # Projection y-coordinate
      ) |>
        dplyr::arrange(proj_y, proj_x) |>
        dplyr::mutate(
          value = as.numeric(summary_data),
          longitude = as.numeric(long),
          latitude = as.numeric(lat),
          station = dplyr::row_number() # Unique identifier for each grid point
        )

      # Save the processed data as a parquet file
      arrow::write_parquet(
        out,
        here::here(file_path, "file-1.parquet")
      )
    }
  }
}
