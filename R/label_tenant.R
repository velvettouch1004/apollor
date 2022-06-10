
value_tenant <- function(tenant, column){
  key <- yaml::read_yaml("tenant_list.yml")
  
  ii <- match(tenant, names(key))
  
  if(any(is.na(ii))){
    stop("Add missing tenant to tenant_list.yml before proceeding!")
  }
  
  unname(sapply(key[ii], "[[", column, USE.NAMES = FALSE))
}

#' Label a tenant based on the code
#' @param tenant Tenant (e.g. 'DEMO'), can be vectorized
#' @export
label_tenant <- function(tenant){
  
  value_tenant(tenant, "name")
  
}


#' Get app name for a tenant
#' @param tenant Juno tenant - not vectorized!
#' @description Reads appname from tenant_list (new format). 
#' @export
appname_tenant <- function(tenant){
  
  value_tenant(tenant, "appname")
  
}

#' Get app id (posit connect uuid) for a tenant
#' @param tenant Juno tenant - not vectorized!
#' @description Reads id from tenant_list (new format). 
#' @export
appid_tenant <- function(tenant){
  
  value_tenant(tenant, "appid")
  
}


#' Decide whether to deploy to production or not
#' @param tenant Juno tenant - not vectorized!
#' @description Reads id from tenant_list (new format). 
#' @export
has_production_tenant <- function(tenant){
  
  value_tenant(tenant, "production")
  
}



