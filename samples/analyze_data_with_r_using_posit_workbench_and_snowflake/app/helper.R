library(tidyverse)
library(DBI)
library(odbc)
library(dbplyr)
library(gtExtras)

# List of 10 of the top domains
top_domains <-
  c(
    "youtube.com", 
    "google.com", 
    "facebook.com", 
    "tiktok.com", 
    "instagram.com",
    "airbnb.com",
    "vrbo.com",
    "lyft.com",
    "uber.com",
    "linkedin.com"
  )

# Rename and join to make data easier to work with
timeseries <-
  timeseries |> 
  rename_with(str_to_lower) |> 
  left_join(
    attributes |> rename_with(stringr::str_to_lower), 
    by = join_by(variable, variable_name)
  ) 

top_domains_df <-
  timeseries |>  
  filter(domain_id %in% top_domains) |> 
  select(domain_id, date, measure, value) |>  
  pivot_wider(names_from = measure, values_from = value) |> 
  collect()

# Create domain comparison plot
plot_pageviews_comparison <- function(domains, date_range) {
  pageviews_comparison <-
    timeseries |>
    filter(
      measure == "Pageviews",
      domain_id %in% domains
    ) |> 
    collect()
  
  pageviews_comparison |>
    filter(date >= date_range[[1]], date <= date_range[[2]]) |> 
    ggplot(aes(date, value, color = domain_id)) +
    geom_line() +
    scale_x_date(date_breaks = "6 months", date_labels = "%b %y") + 
    scale_y_log10() +
    theme_minimal() +
    theme(legend.position = "bottom") +
    labs(
      x = NULL,
      y = "Pageviews",
      color = NULL
    )
}

# Create GT table
table_comparison <- function(domains, date_range) {
  comparison_for_gt <-
    top_domains_df |>  
    filter(date >= date_range[[1]], date <= date_range[[2]]) |> 
    group_by(domain_id) |>
    summarize(
      across(
        c("Pageviews", "Users", "Sessions"),
        \(x) median(x, na.rm = TRUE),
        .names = "avg_{.col}"
      )
    ) %>%
    ungroup() %>%
    rename_with(str_to_lower) |> 
    arrange(desc(avg_pageviews))
  
  gt_tbl <-
    comparison_for_gt |>
    gt(rowname_col = "domain_id") |>
    fmt_number(suffixing = TRUE, decimals = 0) |>
    cols_label(
      avg_pageviews = "Pageviews",
      avg_users = "Users",
      avg_sessions = "Sessions"
    ) 
  
  if (length(domains) >= 1) {
    gt_tbl <-
      gt_tbl |> 
      gt_highlight_rows(
        rows = (domain_id == sort(domains)[[1]]), 
        fill = "#ffce67"
      )
    
    if (length(domains) == 2) {
      gt_tbl <-
        gt_tbl |> 
        gt_highlight_rows(
          rows = (domain_id == sort(domains)[[2]]), 
          fill = "#78c2ad"
        )
    }
  }
  
  gt_tbl
}

# Calculate value box values
avg_users_sessions <- function(domains, date_range) {
  top_domains_df |> 
    filter(date >= date_range[[1]], date <= date_range[[2]]) |> 
    summarize(
      avg_users = median(Users, na.rm = TRUE),
      avg_sessions = median(Sessions, na.rm = TRUE)
    ) 
}
