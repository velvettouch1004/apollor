#' Business logic of the Apollo application
#' @description An R6 object with methods for use in the Shiny application `apollo`. 
#' @importsFrom shintobag shinto_db_connection
#' @importsFrom pool dbPool poolClose
#' @importsFrom R6 R6Class
#' @importsFrom safer encrypt_string decrypt_string
#' @export

LinkItEngine <- R6::R6Class(
  public = list(
    
    con = NULL,
    schema = NULL,
    pool = NULL,
    sel = NULL,
    gemeente = NULL,
    gebruikers = NULL,
    
    initialize = function(gemeente, schema, pool, config_file = "conf/config.yml"){
      
      flog.info("DB Connection", name = "DBR6")
      
      self$gemeente <- gemeente
      self$pool <- pool
      self$schema <- schema
      what <- tolower(gemeente)
      
      cf <- config::get(what, file = config_file)
      print("----CONNECTING TO----")
      print(cf$dbhost)
      
      response <- try({
        shintobag::shinto_db_connection(what = what, 
                                        pool = pool, 
                                        file = config_file)
      })
      
      if(!inherits(response, "try-error")){
        self$con <- response
      }
      
      persoon <- self$read_table("persoon")
      bedrijf <- self$read_table("bedrijf")
      adres <- self$read_table("adres")
      
      
    },
    
    
    #----- General method ----
    list_tables = function(){
      
      DBI::dbGetQuery(self$con,
                      glue("SELECT table_name FROM information_schema.tables
                   WHERE table_schema='{self$schema}'"))
    },
    
    
    close = function(){
      
      if(!is.null(self$con) && dbIsValid(self$con)){
        if(self$pool){
          flog.info("poolClose", name = "DBR6")
          
          poolClose(self$con)
        } else {
          flog.info("dbDisconnect", name = "DBR6")
          
          DBI::dbDisconnect(self$con)
        }
        
      } else {
        flog.info("Not closing an invalid or null connection", name = "DBR6")
      }
      
    },
    read_signals = function(user=NULL){
      
      DBI::dbGetQuery(self$con,
                      glue("SELECT registraties FROM information_schema.tables
                   WHERE table_schema='{self$schema}'"))
      
    }
  )
)