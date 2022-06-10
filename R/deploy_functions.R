
# deploy functions a la WBM
# (was in ApolloSysMethods)


#--- Utils (no export)
config_site_path <- function(tenant){
  glue::glue("config_site/{tenant}/config_site.yml")
}



#--- Functions (exported)

#' Set current tenant in this_version.yml
#' @description Overwrites current this_version.yml!
#' @param tenant Tenant (e.g. 'DEMO')
#' @param path Path where to update the this_version.yml file
#' @export
#' @rdname utils
set_tenant <- function(tenant, path = getwd()){
  cli::cli_alert_success(paste("Apollo - nieuwe tenant:", tenant))
  
  if(!shintodb::has_config_entry(tenant, where = "development")){
    cli::cli_alert_warning("Tenant does not have a (dev) database config entry! Add with shintodb::add_config_entry(...)")
  }
  
  if(!tenant %in% get_tenant_choices()){
    cli::cli_alert_warning("Tenant does not have an entry in tenant_list.yml - add it there or check your spelling!")
  }
  
  yaml::write_yaml(list(tenant = tenant), file.path(path, "this_version.yml"))
}

#' @export
#' @rdname deploy
get_tenant <- function(){
  yaml::read_yaml("this_version.yml")$tenant
}


#' Get available tenants for Apollo
#' @param path Path where to read the tenant_list (normally this working directory)
#' @export
get_tenant_choices <- function(path = getwd()){
  tl <- yaml::read_yaml("tenant_list.yml")
  names(tl)
}

#' Open the config_site for the current tenant
#' @description Open config_site for current gemeente
#' @export
#' @rdname deploy
open_config_site <- function(){
  
  requireNamespace("rstudioapi")  
  
  pth <- config_site_path(get_tenant())
  
  if(file.exists(pth)){
    rstudioapi::navigateToFile(pth)  
  } else {
    message(paste("Could not find file:",pth))
  }
  
}

#' Make a 'deploy project' for Apollo
#' @param tenant If not provided, reads 'gemeente' from this_version
#' @export
#' @rdname deploy
#' @importFrom shintoshiny make_deploy_project
deploy <- function(tenant = NULL, test = FALSE){
  
  cli::cli_alert_danger("Gebruik apollor::deploy_all() om Apollo direct te deployen zonder 'deploy project'")
  
  # # If 'gemeente' specified, ignore this_version,
  # # and set tenant correctly in the deploy project
  # if(is.null(tenant)){
  #   tenant <- get_tenant()
  #   if(is.null(tenant)){
  #     stop("Specify 'gemeente' in this_version.yml")
  #   }
  # } else {
  #   file.copy("this_version.yml", "backup_this_version.yml")
  #   on.exit({
  #     file.copy("backup_this_version.yml", "this_version.yml", overwrite = TRUE)
  #     file.remove("backup_this_version.yml")
  #   })
  #   set_tenant(tenant)
  # }
  # 
  # appname <- make_app_name(tenant)
  # 
  # if(test)appname <- paste0(appname, "_test")
  # 
  # dirs <- c("conf","modules","R","www","preload",
  #           file.path("data_public",tenant),
  #           file.path("data",tenant),
  #           file.path("config_site",tenant))
  # 
  # shintoshiny::make_deploy_project(appname, 
  #                                  directories = dirs,
  #                                  extra_files = c("config_site/help.yml",
  #                                                  "config_site/kvk_risico_branches.csv"))
  
  
  
}


#' @rdname deploy
#' @export
deploy_test <- function(...){
  cli::cli_alert_danger("Gebruik apollor::deploy_all(..., test = TRUE) om Apollo TEST versie direct te deployen zonder 'deploy project'")
  #deploy(..., test = TRUE)
}



