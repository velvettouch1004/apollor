#' Business logic of the Apollo application
#' @description An R6 object with methods for use in the Shiny application `apollo`. 
#' @importsFrom shintobag shinto_db_connection
#' @importsFrom pool dbPool poolClose
#' @importsFrom R6 R6Class
#' @importsFrom dbplyr in_schema collect
#' @importsFrom dplyr tbl left_join 
#' @importsFrom safer encrypt_string decrypt_string
#' @export

ApolloEngine <- R6::R6Class(
  public = list(
    
    con = NULL,
    schema = NULL,
    pool = NULL,
    sel = NULL,
    gemeente = NULL,
    gebruikers = NULL,
    bedrijf = NULL,
    persoon = NULL,
    adres = NULL,
    signals = NULL,
    actions = NULL,
    indicators = NULL,
    
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
      
      self$persoon <- self$read_table("persoon")
      self$bedrijf <- self$read_table("bedrijf")
      self$adres <- self$read_table("adres")
      
      
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
    read_table = function(table, lazy = FALSE){
      
      flog.info(glue("tbl({table})"), name = "DBR6")
      
      if(!is.null(self$schema)){
        table <- dbplyr::in_schema(self$schema, table)
      }
      
      out <- dplyr::tbl(self$con, table)
      
      if(!lazy){
        out <- dplyr::collect(out)
      }
      
      out
      
    },
    read_signals = function(){ 
      self$signals <- self$read_table('registraties') 
      self$signals 
    },
    read_actions = function(){ 
      self$actions <- self$read_table('actielijst') 
      self$actions
    },
    read_indicators = function(){ 
      self$indicators <- self$read_table('indicator') 
      self$indicators
    },
    get_actions = function(update=FALSE){
      if(is.null(self$actions)  || update){
        self$read_actions() 
      } 
      if(is.null(self$signals)  || update){ 
        self$read_signals()
      } 
      
      
       dplyr::left_join( self$actions, self$signals, by=c('registratie_id' = 'id_registratie'), suffix = c(".actie", ".signaal"),)
    }
    
  )
)