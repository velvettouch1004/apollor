
#' Make sure database passwords are entered in the conf/config.yml file
#' @description Prompts for missing passwords, have 1Password ready to go!.
#' Also adds required database connections (data_bag, etc.), and prompts for API keys (TinyMCE, etc.)
#' @param tenants List of tenant names, or wbmr::get_tenant_choices()
#' @param where Server to deploy to, must correspond to a name in the 'servers' argument
#' @param db_connections_required List of database connections needed. The name is the entry in the config file, each has a list with 'dbname' and 'dbuser'
#' @importFrom rstudioapi askForPassword
#' @importFrom shintodb add_config_entry
#' @export 
prepare_db_config <- function(tenants = NULL, where = "development",
                              
                              db_connections_required = list(data_bag = list(dbname = "data_bag", dbuser = "nlextract"), 
                                                              data_cbs = list(dbname = "data_cbs", dbuser = "data_cbs"),
                                                              #data_brk = list(dbname = "data_brk", dbuser = "nlextract"),
                                                              shintousers = list(dbname = "shintousers", dbuser = "shintousers"))
                              ){
  
  
  if(is.null(tenants)){
    cli::cli_alert_danger("Please set one or more tenants to prepare the database config file.")
    return(invisible(NULL))
  }
  
  tenant_choices <- get_tenant_choices()
  
  # check for non-tenants in the argument
  non_tenants <- setdiff(tenants,tenant_choices)
  if(length(non_tenants) > 0){
    cli::cli_alert_danger(glue::glue("{paste(non_tenants, collapse=',')} : not a tenant - add to tenant_list.yml"))
    tenants <- intersect(tenants, tenant_choices) 
  }
  
  posit_server <- get_server(where)
  
  if(is.null(posit_server)){
    stop("Something went wrong with setting the server")
  }
  
  # in Juno, the config section is the same as the names of the servers in this list
  # this might be different elsewhere ...
  config_section <- where
  
  # makes a file if it does not exist, otherwise skips
  shintodb::make_config()
  
  
  for(cfg_entry in names(db_connections_required)){
    
    cfg <- db_connections_required[[cfg_entry]]
    
    if(!shintodb::has_config_entry(cfg_entry, where = config_section)){
      shintodb::add_config_entry(cfg_entry, dbname = cfg$dbname, dbuser = cfg$dbuser, where = config_section)
      cli::cli_alert_success(glue::glue("{cfg_entry} successfully added to config"))
    } else {
      cli::cli_alert_success(glue::glue("{cfg_entry} already in config - skipping"))
    }
    
  }
  
  
  
  # mce_apikey!
  if(!shintodb::has_config_entry("mce_apikey", where = "default")){
    
    msg <- "API key voor TinyMCE (1Pass)"
    apikey <- rstudioapi::askForPassword(msg)
    
    conf <- shintodb::read_config()
    conf$default <- c(list(mce_apikey = list(key = apikey)), conf$default)
    
    yaml::write_yaml(conf, "conf/config.yml")
  }
  
  # kvk_apikey!
  if(!shintodb::has_config_entry("kvk_apikey", where = "default")){
    
    msg <- "API key voor KvK API (1Pass)"
    apikey <- rstudioapi::askForPassword(msg)
    
    conf <- shintodb::read_config()
    conf$default <- c(list(kvk_apikey = list(key = apikey)), conf$default)
    
    yaml::write_yaml(conf, "conf/config.yml")
  }
  
  
  # tenants
  for(tenant in tenants){
    
    lis <- yaml::read_yaml("tenant_list.yml")[[tenant]]
    
    if(!shintodb::has_config_entry(tenant, where = config_section)){
      shintodb::add_config_entry(tenant, dbname = lis$dbname, where = config_section,
                                 dbuser = ifelse(is.null(lis$dbuser), lis$dbname, lis$dbuser))
      cli::cli_alert_success(glue::glue("{tenant} successfully added to config"))
    } else {
      cli::cli_alert_success(glue::glue("{tenant} already in config - skipping"))
    }
    
    
  }
  
  
}
