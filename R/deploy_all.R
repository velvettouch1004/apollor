
#' Vectorized and safe version of deploy_now
#' @description If no 'tenants' argument given, will deploy all tenants in the tenant list. If where='production', 
#' only deploys those tenants with production: yes in the tenant list. Also cecks the db config file and prompts for missing passwords,
#' to avoid deploying a tenant without a database configuration.
#' @param tenants vector of tenants to deploy (if missing, will deploy everything!)
#' @param where Refers to config section (development/production/)
#' @param skip_tenants Remove these tenants from the deploy list (for whatever reason)
#' @param confirm If no tenants argument given, prompt to continue unless confirm = FALSE
#' @param ... Further arguments to [deploy_now()], for example `test=TRUE`
#' @export
deploy_all <- function(tenants = NULL, 
                       where = "development",
                       skip_tenants = NULL,
                       confirm = TRUE, 
                       ...){
  
  # not needed here, just for check of spelling / server entry
  posit_server <- get_server(where)
  
  tenant_list <- yaml::read_yaml("tenant_list.yml")
  
  if(is.null(tenants)){
    
    if(confirm){
      prom <- readline(glue::glue("Je gaat *alle tenants* deployen naar {where}, weet je het zeker? (ja/nee)"))
      if(prom != "ja"){
        stop("Geen 'ja' opgegeven, deploy afgebroken.")
      }  
    }
    
    
    if(where == "production"){
      prods <- unlist(sapply(names(tenant_list), has_production_tenant))
      tenants <- names(prods)[prods]
    }
    
    # for dev, deploy everyone
    if(where == "development"){
      tenants <- names(tenant_list)
    }
    
    if(where == "eindhoven_premium"){
      ehvs <- unlist(sapply(names(tenant_list), function(x)value_tenant(x, "eindhoven_premium")))
      tenants <- names(ehvs)[ehvs]
    }
    
  }
  
  # skip these ones
  tenants <- setdiff(tenants, skip_tenants)
  
  prepare_db_config(tenants, where = where)
  
  for(tenant in tenants){
    cli::cli_h1("Deploying {tenant}")
    tm <- try({
      deploy_now(tenant, where = where, ...)  
    })
    if(inherits(tm, "try-error")){
      cli::cli_alert_danger("Deploy for {tenant} not successful!")
    }
    
  }
  
}
