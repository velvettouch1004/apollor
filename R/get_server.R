#' Posit connect server config
#' @export
get_server <- function(where = "development",
                       servers = c(
                         development = "devapp.shintolabs.net",
                         production = "app.shintolabs.net",
                         eindhoven_premium = "eindhoven.shintolabs.net"
                       )){
  
  if(!where %in% names(servers)){
    cli::cli_alert_danger("'where' argument must be configured in servers argument list")
    return(NULL)
  }
  
  servers[match(where, names(servers))]
  

  
}
