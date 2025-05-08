# This code is used to scrape the UKCP18 data from the CEDA archive.
# The data is downloaded from the CEDA archive and saved as a HIVE-partitioned parquet file in the ukcp directory
# The structure of the data is as follows:
# ukcp
# ├── run
# │   ├── variable
# │   │   ├── year
# │   │   │   ├── month
# │   │   │   │   ├── file-1.parquet
library(httr2)
library(rvest)
library(stringr)
library(purrr)
library(glue)
library(here)

base <- "https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/5km/rcp85/{run}/{variable}/{time_scale}/v20210615"
runs <- c(1, 4, 5, 6, 7, 8, 9, 10)
variables <- c(
  "pr", # precipitation (mm/day)
  "tas", # mean air temperatire (degC)
  "tasmax", # maximum air temperature (degC)
  "hurs" # relative humidity (%)
)

for (run in runs) {
  for (variable in variables) {
    run <- stringr::str_pad(run, width = 2, side = "left", pad = "0")
    time_scale <- dplyr::if_else(
      variable %in% c("pr", "tas"),
      "1hr",
      "day"
    )
    # create the directory
    file_path <- glue::glue("ukcp/run={run}/variable={variable}")
    if (!dir.exists(file_path)) {
      dir.create(file_path, recursive = TRUE)
    }

    base_url <- glue::glue(base)

    index_resp <- request(base_url) |>
      req_headers(Authorization = paste("Bearer", Sys.getenv("CEDA_TOKEN"))) |>
      req_perform()

    urls <- index_resp |>
      resp_body_html() |>
      html_elements("a") |>
      html_attrs() |>
      map(pluck, "href") |>
      unlist()

    urls <- urls[str_detect(urls, "\\.nc\\?download=1")]

    urls <- urls[seq(1, length(urls), by = 2)]

    urls <- urls[1:2]

    for (url in urls) {
      from_to <- stringr::str_extract_all(
        url,
        "_[0-9]{8}-[0-9]{8}"
      )[[1]] |>
        stringr::str_replace("_", "") |>
        stringr::str_split_1("-")

      from <- as.Date(from_to[1], format = "%Y%m%d")
      to <- from + lubridate::dmonths(1) - lubridate::days(1)

      year <- lubridate::year(from)
      month <- lubridate::month(from)

      file_path <- glue::glue(
        "ukcp/run={run}/variable={variable}/year={year}/month={month}"
      )

      if (!dir.exists(file_path)) {
        dir.create(
          file_path,
          recursive = TRUE
        )
      }

      if (file.exists(here::here(file_path, "file-1.parquet"))) {
        next
      }

      tmp <- tempfile()
      resp <- request(url) |>
        req_headers(
          Authorization = paste("Bearer", Sys.getenv("CEDA_TOKEN"))
        ) |>
        req_perform(path = tmp)
      temp_d <- ncdf4::nc_open(tmp)

      max_pr <- ncdf4::ncvar_get(temp_d, variable) |>
        apply(MARGIN = c(1, 2), FUN = max)

      lat <- ncdf4::ncvar_get(temp_d, "latitude")
      long <- ncdf4::ncvar_get(temp_d, "longitude")

      out <- tidyr::crossing(
        proj_x = 1:180,
        proj_y = 1:244,
        from_date = from,
        to_date = to
      ) |>
        dplyr::arrange(proj_y, proj_x) |>
        dplyr::mutate(
          {{ variable }} := as.numeric(max_pr),
          longitude = as.numeric(long),
          latitude = as.numeric(lat),
          station = dplyr::row_number()
        )

      arrow::write_parquet(
        out,
        here::here(file_path, "file-1.parquet")
      )
    }
  }
}
