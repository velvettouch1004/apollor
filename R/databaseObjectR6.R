#' @importsFrom R6 R6Class
#' @importsFrom pool dbPool poolClose
#' @importsFrom dbplyr in_schema collect
#' @importsFrom dplyr tbl left_join 
databaseObject <- R6::R6Class(
  
  public = list(
    
    con = NULL,
    schema = NULL,
    dbname = NULL,
    dbuser = NULL,
    pool = NULL,
    dbtype = NULL,
    
    initialize = function(config_file = "conf/config.yml", 
                          what,
                          schema = NULL,
                          pool = TRUE){
      
      tictoc::tic("WBM R6 init.")
      
      self$connect_to_database(config_file, schema, what, pool)
      
    },  
    
    connect_to_database = function(config_file = NULL, 
                                   schema = NULL, 
                                   what = NULL, 
                                   pool = TRUE, 
                                   sqlite = NULL){
      
      
      if(!is.null(sqlite)){
        
        if(!file.exists(sqlite)){
          stop("SQlite not found, check path")
        }
        
        self$schema <- NULL
        self$dbname <- sqlite
        self$pool <- pool
        
        if(pool){
          self$con <- dbPool(RSQLite::SQLite(), dbname = sqlite)  
        } else {
          self$con <- dbConnect(RSQLite::SQLite(), dbname = sqlite)  
        }
        
        self$dbtype <- "sqlite"
        
      } else {
        
        self$schema <- schema
        self$dbname <- what
        self$pool <- pool
        
        cf <- config::get(what, file = config_file)
        
        print("----CONNECTING TO----")
        print(cf$dbhost)
        
        self$dbuser <- cf$dbuser
        
        if(pool){
          flog.info("pool::dbPool", name = "DBR6")
          response <- try(pool::dbPool(RPostgres::Postgres(),
                                       dbname = cf$dbname,
                                       host = cf$dbhost,
                                       port = cf$dbport,
                                       user = cf$dbuser,
                                       password = cf$dbpassword,
                                       minSize = 1,
                                       maxSize = 25,
                                       idleTimeout = 60*60*1000))
        } else {
          flog.info("DBI::dbConnect", name = "DBR6")
          response <- try(DBI::dbConnect(RPostgres::Postgres(),
                                         dbname = cf$dbname,
                                         host = cf$dbhost,
                                         port = cf$dbport,
                                         user = cf$dbuser,
                                         password = cf$dbpassword))
        }
        
        if(!inherits(response, "try-error")){
          self$con <- response
        }
        
        
        self$dbtype <- "postgres"
        
      }
      
      
    },
    
    
    close = function(){
      
      if(!is.null(self$con) && dbIsValid(self$con)){
        
        if(self$pool){
          flog.info("poolClose", name = "DBR6")
          
          poolClose(self$con)
        } else {
          flog.info("dbDisconnect", name = "DBR6")
          
          dbDisconnect(self$con)
        }
        
      } else {
        flog.info("Not closing an invalid or null connection", name = "DBR6")
      }
    },
    list_tables = function(){
      
      DBI::dbGetQuery(self$con,
                      glue("SELECT table_name FROM information_schema.tables
                   WHERE table_schema='{self$schema}'"))
    },
    have_column = function(table, column){
      
      qu <- glue::glue(
        "SELECT EXISTS (SELECT 1 
                     FROM information_schema.columns 
                     WHERE table_schema='{self$schema}' AND table_name='{table}' AND column_name='{column}');"  
      )
      
      dbGetQuery(self$con, qu)$exists
      
      
    },
    
    make_column = function(table, column, type = "varchar"){
      
      qu <- glue::glue("alter table {self$schema}.{table} add column {column} {type}")
      dbExecute(self$con, qu)
      
    },
    
    
    
    read_table = function(table, lazy = FALSE){
      
      #tictoc::tic(glue("tbl({table})"))
      
      if(!is.null(self$schema)){
        out <- dplyr::tbl(self$con,  dbplyr::in_schema(self$schema, table))  
      } else {
        out <- dplyr::tbl(self$con, table)
      }
      
      
      if(!lazy){
        out <- dplyr::collect(out)
      }
      
      #tictoc::toc()
      
      out
      
    },
    
    read_spatial_table = function(table, extra_sql = ""){
      
      flog.info(glue("st_read({table})"), name = "DBR6")
      
      try(
        sf::st_read(self$con, 
                    query = glue("select * from {self$schema}.{table} {extra_sql}")) %>%
          sf::st_transform(4326)  
      )
      
    },
    
    append_data = function(table, data){
      
      
      flog.info(glue("append {nrow(data)} rows to '{table}'"), name = "DBR6")
      
      if(!is.null(self$schema)){
        tm <- try(
          dbWriteTable(self$con,
                       name = Id(schema = self$schema, table = table),
                       value = data,
                       append = TRUE)
        )
      } else {
        tm <- try(
          dbWriteTable(self$con,
                       name = table,
                       value = data,
                       append = TRUE)
        )
        
      }
      
      return(invisible(!inherits(tm, "try-error")))
      
    },
    
    
    
    query = function(txt, glue = TRUE, quiet = FALSE){
      
      if(glue)txt <- glue::glue(txt)
      if(!quiet){
        flog.info(glue("query({txt})"), name = "DBR6")  
      }
      
      
      try(
        dbGetQuery(self$con, txt)
      )
      
    },
    execute_query = function(txt, glue = TRUE){
      
      if(glue)txt <- glue::glue(txt)
      
      flog.info(glue("query({txt})"), name = "DBR6")
      
      try(
        dbExecute(self$con, txt)
      )
      
    },
    has_value = function(table, column, value){
      
      if(!is.null(self$schema)){
        out <- self$query(glue("select {column} from {self$schema}.{table} where {column} = '{value}' limit 1"))  
      } else {
        out <- self$query(glue("select {column} from {table} where {column} = '{value}' limit 1"))  
      }
      
      nrow(out) > 0
    },
    
    
    # set verwijderd=1 where naam=gekozennaam.
    # replace_value_where("table", 'verwijderd', 'true', 'naam', 'gekozennaam')
    replace_value_where = function(table, col_replace, val_replace, col_compare, val_compare,
                                   query_only = FALSE, quiet = FALSE){
      
      if(!is.null(self$schema)){
        query <- glue("update {self$schema}.{table} set {col_replace} = ?val_replace where ",
                      "{col_compare} = ?val_compare") %>% as.character()  
      } else {
        query <- glue("update {table} set {col_replace} = ?val_replace where ",
                      "{col_compare} = ?val_compare") %>% as.character()
      }
      
      query <- sqlInterpolate(DBI::ANSI(), 
                              query, 
                              val_replace = val_replace, val_compare = val_compare)
      
      if(query_only)return(query)
      
      if(!quiet){
        flog.info(query, name = "DBR6")  
      }
      
      
      dbExecute(self$con, query)
      
      
    },
    
    # zelfde, met 1 AND statement (dus twee condities: where x=1, y=2)
    # kan generieker natuurlijk
    replace_value_where2 = function(table, col_replace, val_replace, 
                                    col_compare1, val_compare1,
                                    col_compare2, val_compare2){
      
      if(!is.null(self$schema)){
        query <- glue("update {self$schema}.{table} set {col_replace} = ?val_replace where ",
                      "{col_compare1} = ?val_compare1 AND ",
                      "{col_compare2} = ?val_compare2") %>% as.character()  
      } else {
        query <- glue("update {table} set {col_replace} = ?val_replace where ",
                      "{col_compare1} = ?val_compare1 AND ",
                      "{col_compare2} = ?val_compare2") %>% as.character()
      }
      
      
      query <- sqlInterpolate(DBI::ANSI(), 
                              query, 
                              val_replace = val_replace, 
                              val_compare1 = val_compare1,
                              val_compare2 = val_compare2)
      
      flog.info(glue("replace_value_where2({query})"), name = "DBR6")
      
      dbExecute(self$con, query)
      
      
    },
    
    
    table_info = function(table){
      
      if(self$dbtype == "sqlite"){
        message("Not supported for SQLite")
        return(NA)
      }
      
      query <- glue("select * from information_schema.columns ",
                    "where table_schema = '{self$schema}' and ",
                    "table_name = '{table}'")
      
      flog.info(query, name = "DBR6")
      
      try(
        dbGetQuery(self$con, query)  
      )
      
    },
    
    table_columns = function(table, empty_table = FALSE){
      
      if(!is.null(self$schema)){
        query <- glue("select * from {self$schema}.{table} where false")  
      } else {
        query <- glue("select * from {table} where false")
      }
      
      flog.info(query, name = "DBR6")
      
      out <- dbGetQuery(self$con, query)
      
      if(empty_table){
        return(out)
      } else {
        return(names(out))
      }
      
    },
    
    
    delete_rows_where = function(table, col_compare, val_compare){
      
      if(!is.null(self$schema)){
        query <- glue("delete from {self$schema}.{table} where {col_compare}= ?val")  
      } else {
        query <- glue("delete from {table} where {col_compare}= ?val")
      }
      
      query <- sqlInterpolate(DBI::ANSI(), query, val = val_compare)
      flog.info(query, name = "DBR6")
      
      try(
        dbExecute(self$con, query)   
      )
      
    }
  )
  
  
  
)