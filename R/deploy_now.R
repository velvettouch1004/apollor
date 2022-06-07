

#' Deploy Apollo directly
#' @description Does not make a 'deploy project' but deploys Apollo for a tenant immediately,
#' from a temporary directory
#' @param tenant Tenant name (e.g. 'DEMO')
#' @param appname Name of app on posit connect (e.g. ')
#' @param where The server to deploy to (devapp or app)
#' @param appid Optional: the app ID (looks like a uuid in the url at posit connect). Needed if you
#' are a collaborator on an app and want to update it (the original uploader does not need the ID)
#' @param db_config_file Standard location of the DB config file
#' @param log_deployment If TRUE, attempts to log the deployment in DB 
#' @param launch_browser If TRUE, opens a browser with the app after deployment
#' @param deploy_location App will be deployed from a temp directory, or set a directory here if
#' you want to inspect the pre-deploy files.
#' @param delete_after_deploy If TRUE, deletes all files after deploying. 
#' @param ... Further arguments passed to [rsconnect::deployApp()]
#' @importFrom cli cli_alert_success cli_alert_warning cli_alert_info
#' @importFrom rsconnect accounts deployApp
#' @importFrom R.utils copyDirectory
#' @importFrom uuid UUIDgenerate
#' @importFrom yaml write_yaml
#' @importFrom shintodb decrypt_config_file has_config_entry
#' @importFrom shintoshiny log_rsconnect_deployments connect_db_rsconnect_deployments
#' @importFrom DBI dbDisconnect
#' @export
deploy_now <- function(tenant,
                       appname,
                       where = c("devapp.shintolabs.net","app.shintolabs.net"),
                       appid = NULL,
                       db_config_file = "conf/config.yml",
                       log_deployment = TRUE,
                       launch_browser = TRUE,
                       deploy_location = tempdir(),
                       delete_after_deploy = TRUE,
                       ...
){
  
  where <- match.arg(where)
  
  cli::cli_alert_success(paste("Deploying Apollo for tenant",tenant, "to", where,  "(", Sys.time(), ")"))
  
  # posit connect account / user
  acc <- rsconnect::accounts()
  
  posit_user <- acc$name[acc$server == where]
  if(length(posit_user) == 0){
    stop(paste("Connect a user to", where, "using the Rstudio Posit Connect button"))
  }
  
  cli::cli_alert_success("Posit connect user verified")
  
  # Files
  directories <- c(
    "conf",
    file.path("config_site", tenant),
    file.path("data", tenant),
    "modules",
    "preload",
    "R",
    "www"
  )
  
  lapply(directories, function(p){
    if(dir.exists(p)){
      R.utils::copyDirectory(p, to = file.path(deploy_location, p), overwrite = TRUE)  
    }
  })
  
  root_files <- c("global.R",
                  "NEWS.md",
                  "server.R",
                  "this_version.yml",
                  "ui.R",
                  "VERSION")
  
  file.copy(root_files, deploy_location)
  
  # extra files
  file.copy("config_site/kvk_risico_branches.csv", file.path(deploy_location, "config_site/kvk_risico_branches.csv"))
  
  
  cli::cli_alert_success("Copying files to temporary directory done")
  
  # manifest
  manif <- list(
    timestamp = format(Sys.time()),
    uuid = uuid::UUIDgenerate(),
    git = shintoshiny::read_git_version()
  )
  yaml::write_yaml(manif, file.path(deploy_location, "shintoconnect_manifest.yml"))
  
  
  # set tenant
  apollor::set_tenant(tenant, deploy_location)
  
  #if(shintodb::config_is_encrypted(db_config_file)){
  # unencrypted wordt toch geskipt
  if(file.exists(db_config_file)){
    
    out_conf <- file.path(deploy_location, db_config_file)
    shintodb::decrypt_config_file(out_conf, out_conf)
    
    if(!delete_after_deploy){
      on.exit(
        shintodb::encrypt_config_file(out_conf, out_conf)
      )
    }
  }
  
  # Deploy de app
  resp <- rsconnect::deployApp(
    appDir = deploy_location,
    appTitle = appname,
    appId = appid,
    account = posit_user,
    server = where,
    launch.browser = launch_browser,
    forceUpdate = TRUE,
    ...
  )
  
  if(isTRUE(resp) && log_deployment){
    
    if(!shintodb::has_config_entry("rsconnect_deployments", where = "default")){
      
      cli::cli_alert_warning("No DB connection config found for 'rsconnect_deployments', deployment will not be logged.")
      
    } else {
      
      # Schrijf deployment info naar rsconnect_deployments database
      con <- shintoshiny::connect_db_rsconnect_deployments(db_config_file)
      
      on.exit({
        DBI::dbDisconnect(con)
      })
      
      shintoshiny::log_rsconnect_deployments(con,
                                             appname = appname,
                                             environment = where,
                                             userid = posit_user)
      
    }
    
  }
  
  if(delete_after_deploy){
    unlink(deploy_location)  
    cli::cli_alert_info("Deploy files have been deleted")
  }
  
  cli::cli_alert_success(paste("Apollo deployment -", tenant,"- successful"))
  
}


