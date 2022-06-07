
# deploy functions a la WBM
# (was in ApolloSysMethods)


#--- Utils (no export)
config_site_path <- function(tenant){
  glue::glue("config_site/{tenant}/config_site.yml")
}

#' @description Make code friendly version of tenant name
normalize_gemeente_name <- function(tenant){
  o <- tolower(tenant)
  o <- gsub("-","_",o)
  o <- gsub(" ","_",o)
  o
}

#' @decription Make apollo appn ame based on gemeente name
make_app_name <- function(tenant){
  paste0(normalize_gemeente_name(tenant), "_apollo")
}



get_current_db_name = function(){
  tenant <- get_tenant()
  
  stopifnot(file.exists("conf/config.yml"))
  config::get(tenant, file = "conf/config.yml")$dbname
}



#--- Functions (exported)

#' Set tenant
#' @description Set current tenant ('gemeente') in this_version.yml. Overwrites current this_version.yml!
#' @export
#' @rdname deploy
#' @importFrom cli cli_alert_success
set_tenant <- function(tenant, path = getwd()){
  cli::cli_alert_success(paste("Apollo - nieuwe tenant:", tenant))
  yaml::write_yaml(list(gemeente = tenant), file.path(path, "this_version.yml"))
}


#' @export
#' @rdname deploy
get_tenant <- function(){
  yaml::read_yaml("this_version.yml")$gemeente
}


#' @description Get available gemeentes for the WBM
#' @export
#' @rdname deploy
get_tenant_choices <- function(path = getwd()){
  fns <- list.dirs(file.path(path, "config_site"), recursive = FALSE)
  sort(basename(fns))
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
  
  # If 'gemeente' specified, ignore this_version,
  # and set tenant correctly in the deploy project
  if(is.null(tenant)){
    tenant <- get_tenant()
    if(is.null(tenant)){
      stop("Specify 'gemeente' in this_version.yml")
    }
  } else {
    file.copy("this_version.yml", "backup_this_version.yml")
    on.exit({
      file.copy("backup_this_version.yml", "this_version.yml", overwrite = TRUE)
      file.remove("backup_this_version.yml")
    })
    set_tenant(tenant)
  }
  
  appname <- make_app_name(tenant)
  
  if(test)appname <- paste0(appname, "_test")
  
  dirs <- c("conf","modules","R","www","preload",
            file.path("data_public",tenant),
            file.path("data",tenant),
            file.path("config_site",tenant))
  
  shintoshiny::make_deploy_project(appname, 
                                   directories = dirs,
                                   extra_files = c("config_site/help.yml",
                                                   "config_site/kvk_risico_branches.csv"))
  
}


#' @rdname deploy
#' @export
deploy_test <- function(...){
  deploy(..., test = TRUE)
}



