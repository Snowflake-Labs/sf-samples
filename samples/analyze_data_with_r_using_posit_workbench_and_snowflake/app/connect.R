library(DBI)
library(odbc)
library(dbplyr)
library(dplyr)

# DATABASE CONNECTION ----
# Connect to the database
conn <- 
  DBI::dbConnect(
    odbc::snowflake(), 
    warehouse = "DEVREL_WH_LARGE",
    database = "WEB_TRAFFIC_FOUNDATION_EXPERIMENTAL",
    schema = "CYBERSYN"
  )

# TABLES ----
attributes <- tbl(conn, "WEBTRAFFIC_SYNDICATE_ATTRIBUTES")
domains <- tbl(conn, "COMPANY_DOMAIN_RELATIONSHIPS")
timeseries <- tbl(conn, "WEBTRAFFIC_SYNDICATE_TIMESERIES")
