library(shiny)
library(bslib)
library(dbplot)
library(gt)
library(bsicons)
library(thematic)

source("connect.R")
source("helper.R")

thematic_shiny()

# UI ----
ui <- page_sidebar(
  title = 
    div(
      img(src = "logo.png", height = "30px"), 
      "Web Traffic Comparison Tool"
    ),
  sidebar = 
    sidebar(
      selectizeInput(
        inputId = "domains",
        label = "Choose two domains to compare:",
        choices = top_domains |> sort(),
        selected = c("google.com", "facebook.com"),
        multiple = TRUE,
        options = list(maxItems = 2)
      ),
      dateRangeInput(
        inputId = "date_range",
        "Select a date range:",
        start = "2021-01-01",
        end = lubridate::today()
      )
    ),
  layout_column_wrap(
    width = NULL,
    style = css(grid_template_columns = "3fr 2fr"),
    card(
      card_header("Pageviews for selected domains"),
      plotOutput("pageviews")
    ),
    layout_column_wrap(
      width = 1,
      heights_equal = "row",
      card(
        card_header("Web traffic for all 10 domains"),
        layout_column_wrap(
          value_box(
            "Average daily users", 
            value = textOutput("box_avg_users"),
            theme = "danger",
            showcase = bs_icon("people-fill")
          ), 
          value_box(
            "Average daily sessions", 
            value = textOutput("box_avg_sessions"),
            theme = "info",
            showcase = bs_icon("laptop")
          )
        ),
        card(
          card_header("Average daily metrics"),
          gt_output("comparison_gt")
        )
      )
    )
  ),
  theme = bs_theme(bootswatch = "minty")
)

server <- function(input, output) {
  
  output$pageviews <- renderPlot({
    plot_pageviews_comparison(input$domains, input$date_range)
  })
  
  output$comparison_gt <- 
    render_gt(expr = table_comparison(input$domains, input$date_range))
  
  output$box_avg_users <- renderText({
    scales::label_number(scale_cut=scales::cut_short_scale())(
      avg_users_sessions(input$domains, input$date_range)$avg_users
    )
  })
  
  output$box_avg_sessions <- renderText({
    scales::label_number(accuracy = 0.1, scale_cut=scales::cut_short_scale())(
      avg_users_sessions(input$domains, input$date_range)$avg_sessions
    )
  })
}

# Run the application 
shinyApp(ui = ui, server = server)