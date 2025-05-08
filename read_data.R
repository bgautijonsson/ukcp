library(tidyverse)
library(duckdb)
con <- dbConnect(duckdb())

d <- tbl(con, "read_parquet('ukcp_hourly/run=01/variable=pr/*/*/*.parquet', hive_partitioning=TRUE)") |>
  filter(
    run == 1,
    variable == "pr"
  ) |>
  # group_by(station, proj_x, proj_y, latitude, longitude, year) |>
  summarise(
    precip = max(value),
    .by = c(station, proj_x, proj_y, latitude, longitude, year)
  ) |>
  # ungroup() |>
  collect()

precip <- d |>
  select(station, year, precip)

stations <- d |>
  distinct(
    station, proj_x, proj_y, latitude, longitude
    )
