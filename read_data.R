library(tidyverse)
library(arrow)

d <- open_dataset("ukcp") |> to_duckdb()
