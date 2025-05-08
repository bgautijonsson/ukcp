# UKCP18 Data Processing

This project downloads and processes UK Climate Projections 2018 (UKCP18) data from the CEDA archive. The data is converted from NetCDF format to HIVE-partitioned parquet files for efficient storage and querying.

## Data Structure

The processed data is organized in a hierarchical structure:
```
ukcp_hourly/
├── run={run_id}/           # Different climate model runs
│   ├── variable={var}/     # Climate variables (precipitation, temperature)
│   │   ├── year={year}/    # Year of the data
│   │   │   ├── month={month}/  # Month of the data
│   │   │   │   ├── file-1.parquet  # Processed data file
```

## Variables

The script processes two main climate variables:
- `pr`: Precipitation (mm/day)
- `tas`: Mean air temperature (degrees Celsius)

## Output

The script generates parquet files containing processed climate data, organized by:
- Climate model run
- Variable type
- Year
- Month

Each parquet file contains:
- Grid coordinates (proj_x, proj_y)
- Latitude and longitude
- Climate variable values
- Station identifiers

## Querying the Data

The data can be queried efficiently using Apache Arrow and DuckDB. Here's a minimal example:

```R
library(tidyverse)
library(arrow)

# Open the dataset and connect to DuckDB for efficient querying
d <- open_dataset("ukcp") |> to_duckdb()

# Example queries:
# Get all data for a specific run and variable
d |> 
  filter(run == "01", variable == "tas") |>
  collect()

# Get average temperature by year
d |>
  filter(variable == "tas") |>
  group_by(year) |>
  summarise(avg_temp = mean(value)) |>
  collect()
```

The Arrow/DuckDB combination allows for efficient querying of the partitioned data without loading everything into memory.
