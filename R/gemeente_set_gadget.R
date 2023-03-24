
#' UI to set WBM gemeente
#' @export
set_tenant_menu <- function(){

  ui <- miniUI::miniPage(
    miniUI::gadgetTitleBar("Apollo",
                           left = miniUI::miniTitleBarCancelButton(label = "Annuleren", primary = TRUE),
                           right = NULL),
    miniUI::miniContentPanel(

      shiny::uiOutput("ui_current_klant"),

      shiny::selectInput("sel_klant",  "Maak een keuze", choices = NULL),

      shiny::tags$p("Pas op: huidige this_version.yml wordt overschreven (zonder comments)"),
      shiny::actionButton("btn_set_klant", "Klant instellen",
                          icon = shiny::icon("check"),
                          class = "btn-success btn-lg")

    )
  )

  server <- function(input, output, session){

    shiny::observeEvent(input$cancel, {
      shiny::stopApp("Geannulleerd.")
    })

    output$ui_current_klant <- renderUI({
      shiny::tags$p(HTML(glue::glue("De huidige tenant is <b>{get_tenant()}</b>")),
             style = "font-size: 1.1em;")
    })

    observe({
      updateSelectInput(session, "sel_klant",
                         choices = sort(get_tenant_choices()),
                         selected = get_tenant())
    })


    observeEvent(input$btn_set_klant, {

      set_tenant(input$sel_klant)
      shiny::stopApp(glue::glue("Nieuwe tenant is {input$sel_klant}"))
    })

  }

  shiny::runGadget(ui, server,
                   viewer = shiny::dialogViewer(dialogName = "Apollo - tenant kiezen"),
                   stopOnCancel = FALSE)

}
