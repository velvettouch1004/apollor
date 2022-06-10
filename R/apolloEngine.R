#' Business logic of the Apollo application
#' @description An R6 object with methods for use in the Shiny application `apollo`. 
#' @importFrom shintobag shinto_db_connection
#' @importFrom pool dbPool poolClose
#' @importFrom R6 R6Class
#' @importFrom dbplyr in_schema  
#' @importFrom dplyr tbl left_join collect
#' @importFrom plyr join_all 
#' @importFrom safer encrypt_string decrypt_string
#' @importFrom jsonlite toJSON fromJSON
#' @importFrom tibble tibble
#' @export

ApolloEngine <- R6::R6Class(
  inherit = databaseObject,
  
  lock_objects = FALSE,
  public = list(
    
    initialize = function(gemeente, schema, pool, 
                          config_file = getOption("config_file","conf/config.yml"),
                          geo_file = NULL){
      
      flog.info("DB Connection", name = "DBR6")
      
      self$gemeente <- gemeente
      self$pool <- pool
      self$schema <- schema
      what <- gemeente
      
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
      
      self$person <- self$read_table("person")
      self$business <- self$read_table("business")
      self$address <- self$read_table("address")
      self$indicator <- self$read_table("indicator")
      
      
      # Geo data
      if(is.null(geo_file)){
        geo_file <- glue("data_public/{gemeente}/geo_{gemeente}.rds")  
      }
      if(!file.exists(geo_file)){
        self$have_geo <- FALSE
        message("No geo found. Continuing without geo data.")
      } else {
        self$have_geo <- TRUE
        self$geo <- readRDS(geo_file)
      }
      
    },
    
    
    ######################################################################
    # ---------------  GEO UTILITIES ----------------------------------- #
    ######################################################################
    
    assert_geo = function(){
      if(!self$have_geo){
        stop("This method can only be tested when geo data is loaded")
      }
    },
    
    #' @description Translate buurt or wijk codes into names
    #' @param code E.g. "WK022801" or "BU02280105"
    geo_name_from_code = function(code){
      
      self$assert_geo()
      
      co <- substr(code[1],1,2) 
      if(co == "BU"){
        self$geo$buurten$bu_naam[match(code, self$geo$buurten$bu_code)]
      } else if(co == "WK") {
        self$geo$wijken$wk_naam[match(code, self$geo$wijken$wk_code)]
      } else {
        stop("Only use CBS buurt/wijk codes (BU..., WK...)")
      }
      
    },
    
    #' @description Add wijk, gemeente columns to a dataframe with buurt_code_cbs column
    #' @details Existing geo columns will be overwritten
    add_geo_columns = function(data){
      
      self$assert_geo()
      
      if(!"buurt_code_cbs" %in% names(data)){
        stop("buurt_code_cbs must be a column in data")
      }
      
      geo_cols <- c("buurt_naam","wijk_code_cbs","wijk_naam","gemeente_naam")
      have_geo_cols <- intersect(names(data), geo_cols)
      if(length(have_geo_cols)){
        data <- select(data, -all_of(have_geo_cols))
      }
      
      key <- select(self$geo$buurten,
                    buurt_code_cbs = bu_code,
                    buurt_naam = bu_naam,
                    wijk_code_cbs = wk_code,
                    wijk_naam = wk_naam,
                    gemeente_naam = gm_naam)
      
      left_join(data, key, by = "buurt_code_cbs")
                    
      
    },
    
    
    
    ######################################################################
    # ---------------  UTILITIES --------------------------------------- #
    ######################################################################
    
    to_json = function(x){
      jsonlite::toJSON(x, auto_unbox = TRUE)
    },
    
    from_json = function(x){
      jsonlite::fromJSON(x)
    },
    
    
    ######################################################################
    # ---------------  APOLLO SPECIFIC FUNCTIONS ----------------------- #
    ######################################################################
  
    read_signals = function(){
      self$signals <- self$read_table('registrations') 
      invisible(self$signals)
    },
    read_actions = function(){ 
      self$actions <- self$read_table('actionlist') 
      invisible(self$actions)
    },
    read_indicator = function(){ 
      self$indicator <- self$read_table('indicator') 
      invisible(self$indicator)
    },
    read_person = function(){ 
      self$person <- self$read_table('person') 
      invisible(self$person)
    },
    read_business = function(){ 
      self$business <- self$read_table('business') 
      invisible(self$business)
    }, 
    read_address = function(){ 
      self$address <- self$read_table('address') 
      invisible(self$address)
    },
    read_favorites = function(){ 
      self$favorites <- self$read_table('favorites') 
      invisible(self$favorites)
    },
    read_log = function(){ 
      self$user_event_log <- self$read_table('user_event_log') 
      invisible(self$user_event_log)
    },
    read_metadata = function(){ 
      self$metadata <- self$read_table('metadata') 
      invisible(self$metadata)
    },
 
    
    
    ######################################################
    # -------------- INDICATOR FUNCTIONS ----------------#
    ######################################################
    
    #' @description Adds an indicator's metadata to the 'indicator' table
    #' @param column_name Name of the column (indicator)
    #' @param type Indicator type - refers to the table that has this column_name
    #' @param label Short label for the indicator
    #' @param description Long-form text explaining the indicator
    #' @param user_id Who are you?
    #' @param theme Which theme(s) is the indicator used in? e.g. c("mensenhandel","drugs")
    #' @param columns 
    #' @param 
    add_indicator = function(indicator_name, 
                             type = c("address","person"), 
                             label,
                             description, 
                             user_id, 
                             theme, 
                             columns, 
                             weight, 
                             threshold){
      
      type <- match.arg(type)
      stopifnot(is.numeric(weight))
      stopifnot(is.numeric(threshold))
      
      data <- tibble::tibble(
        indicator_name = indicator_name,
        object_type = type,
        label = label,
        description = description,
        user_id = user_id,
        theme = self$to_json(theme),
        columns = self$to_json(columns),
        weight = weight,
        threshold = threshold,
        timestamp = format(Sys.time())
      )
      
      self$append_data("indicator", data)
      
      
    },
    
    #' @description Remove an indicator's metadata from the 'indicator' table0
    remove_indicator = function(indicator_name){ 
      
      self$delete_rows_where("indicator", "indicator_name", indicator_name)
      
    },
    
    
    #' @description Get rows of indicators table for a theme
    #' @details Use this function to find definitions for indicators in a theme.
    get_indicators_theme = function(theme){
    
      out <- self$indicator %>% 
        filter(grepl(!!theme, theme))  
      
      if(nrow(out) == 0){
        stop(paste("Theme",theme,"not found"))
      }
      
      out
      
    },
    
    #' @description Set the weight (riskmodel) for an indicator in a theme
    set_indicator_weight = function(indicator_name, theme, weight){
      
      def <- self$get_indicators_theme(theme)
      indi_id <- dplyr::filter(def, indicator_name == !!indicator_name) %>% 
        dplyr::pull(indicator_id)
      
      self$replace_value_where(table = "indicator", 
                               col_replace = "weight", 
                               val_replace = weight, 
                               col_compare = "indicator_id", 
                               val_compare = indi_id)
      
    },

    #' @description Convert raw indicator data to TRUE/FALSE
    #' @param data Dataframe subset from `indicator` table, read with `$get_indicators_theme`
    #' @param indicator Name of the indicator to convert
    make_boolean_indicator = function(data, indicator){
      
      def <- filter(data, indicator_name == !!indicator)
      
      tab <- self[[def$object_type]]
      if(is.null(tab)){
        stop("object_type must refer to a dataset loaded in the R6 (address, person, business)")
      }
      
      values <- tab[[indicator]]
      if(is.null(values)){
        stop(paste(indicator, "not found in data - problem with config!"))
      }
      
      # for now, only a simple threshold calculation
      values >= def$threshold
      
    },
    
    #' @description Make a table with indicator (boolean) values
    #' @param theme One of the themes in the `indicator` table
    #' @param type One of the [object_type]'s in the `indicator` table
    #' @param id_columns Columns from the base table (e.g. person) that are copied to the indicator table
    make_indicator_table = function(theme, 
                                    type = c("address","person","business"),
                                    id_columns = c("address_id","buurt_code_cbs")){
      
      type <- match.arg(type)
      
      def <- self$get_indicators_theme(theme) %>%
        filter(object_type == !!type)
      
      # Selecteer alleen de adres id en buurt code,
      tab <- self[[type]] %>% select(all_of(!!id_columns))

      # voeg alle boolean indicators toe
      i_data <- lapply(def$indicator_name, function(x){
        self$make_boolean_indicator(data = def, indicator = x)
      })
      
      out <- do.call(cbind, i_data) %>%
        as_tibble(., .name_repair = "minimal") %>%
        setNames(def$indicator_name)
      
      if(nrow(out) != nrow(tab)){
        stop("Fatal error 1 in `make_address_indicator_table`")
      }
      
      out <- cbind(tab, out)
      
      attr(out, "id_columns") <- id_columns
      
      out
    },
    
    
    #' @description Combine address level and person level indicators
    #' @details Make an address-level dataset with indicators. The person indicator table
    #' is summarized so that if at least one person at the address is TRUE for some indicator,
    #' then the address is TRUE. TODO business level indicators
    #' @param indi_address Indicators at address level
    #' @param indi_person Indicators at person level
    combine_indicator_tables = function(indi_address, indi_person){
      
      # Make person indicators into address-based indicators.
      # If one or more person = TRUE, address = TRUE.
      
      # drop extra columns except address_id
      drop_cols <- setdiff(attr(indi_person, "id_columns"), "address_id")
      if(length(drop_cols)){
        indi_person <- select(indi_person, -all_of(drop_cols))
      }
      
      # summarize person indicators to end up with address level indicators
      # If "any" of the persons on the address is TRUE, the address is TRUE
      indi_person <- group_by(indi_person, address_id) %>%
        summarize(across(everything(), any))
      
      out <- left_join(indi_address, indi_person, by = "address_id")
      
      # Missing address from RHS (person) : should always be FALSE
      out[is.na(out)] <- FALSE
      
      out
    },
    
    #' @description Calculate riskmodel at address level
    #' @param data Combined indicator table at address level (made with `combine_indicator_tables`)
    #' @param theme Theme for the indicators; used to get weights from definition table
    calculate_riskmodel = function(data, theme){
      
      # Definition for this theme
      def <- self$get_indicators_theme(theme)
      
      if(!all(def$indicator_name %in% names(data))){
        stop("Some indicator definitions not found in data!")
      }
      
      # Matrix multiply ftw
      m <- as.matrix(data[, def$indicator_name])
      data$riskmodel <- as.vector(m %*% def$weight)
      
      data
    },
    
    
    ######################################################
    # -------------- LIST FUNCTIONS -------------------- #
    ######################################################
    
    list_actions = function(update=FALSE){
      if(is.null(self$actions)  || update){
        self$read_actions() 
      } 
      if(is.null(self$signals)  || update){ 
        self$read_signals()
      }  

       dplyr::left_join( self$actions, self$signals, by=c('registration_id' ), suffix = c(".actie", ".signaal"))

    },
    
    list_favorites = function(user=NULL, update=FALSE){
      if(is.null(self$business)  || update){
        self$read_business() 
      } 
      if(is.null(self$person)  || update){ 
        self$read_person()
      } 
      if(is.null(self$address)  || update){ 
        self$read_address()
      } 
      self$read_favorites()
      A <- dplyr::left_join( dplyr::filter( self$favorites, object_type == 'registration')  , self$signals, by=c('object_id' ='registration_id'), suffix = c("fav", ".signaal"))
      B <- dplyr::left_join( dplyr::filter( self$favorites, object_type == 'person')  , self$person, by=c('object_id'='person_id'), suffix = c("fav", ".person"))
      C <- dplyr::left_join( dplyr::filter( self$favorites, object_type == 'business')  , self$business, by=c('object_id'='business_id'), suffix = c("fav", ".business"))
      D <- dplyr::left_join( dplyr::filter( self$favorites, object_type == 'address')  , self$address, by=c('object_id'='address_id'), suffix = c("fav", ".address"))
       
      plyr::join_all(list(A,B,C,D), by='favorite_id', type='left') 
    
    },
    
    #' @description Get persons based on person or address_id
    #' @param address_id Vector of address IDs
    #' @param person_id Vector of person Ids. Provide either address or person ID, not both!
    list_persons_by_id = function(address_id = NULL, person_id = NULL){
      
      if(!is.null(person_id)){
        dplyr::filter(self$person, 
                      person_id %in% !!person_id) 
      } else {
        dplyr::filter(self$person, 
                      address_id %in% !!address_id)
      }
      
      
    },
    
    ################################################
    # -------------- LOGGING --------------------- #
    ################################################  
    get_log_ping = function(){
      try( 
        self$execute_query(glue("select max(timestamp) from {self$schema}.user_event_log;"))
        
      ) 
    },
    
    
    log_user_event = function(user_id, description){
      
      self$append_data('user_event_log', 
                       data.frame (user_id  =  user_id,
                                   description =  description,
                                   timestamp = Sys.time()))
    },
    
    ###################################################
    # -------------- FAVORITES --------------------- #
    ###################################################
    # add to favorites
    add_favorite = function(user_id, object_id, object_type){
      self$log_user_event(user_id, description=glue("Heeft {object_type} {object_id} aan favorieten toegevoegd"))
      
      try( 
        self$append_data('favorites', 
                         data.frame (user_id =  user_id,
                                     object_id = object_id,
                                     object_type = object_type, 
                                     timestamp = Sys.time()))
      ) 
    },
    # remove from favorites
    remove_favorite = function(user_id, favorite_id){
      self$log_user_event(user_id, description=glue("Heeft {favorite_id} uit favorieten verwijderd"))
      
      try( 
        self$execute_query(glue("DELETE from {self$schema}.favorites WHERE favorite_id = {favorite_id}"))
        
      ) 
    },
    
    
    #######################################################
    # ---------------  ACTIELIJST ----------------------- #
    #######################################################
    
    # add actie to actielijst
    create_action = function(registration_id, user_id, action_name, action_date, description, status){
      self$log_user_event(user_id, description=glue("Heeft actie {action_name} aangemaakt"))
      
      try( 
        self$append_data('actionlist', 
                         data.frame (action_name = action_name,
                                     registration_id  =  registration_id,
                                     user_id =  user_id,
                                     action_date = action_date,
                                     description = description,
                                     status = status,
                                     timestamp = Sys.time()))
      ) 
    },
    # update actie in actielijst
    update_action = function(action_id,action_name,  registration_id, user_id, action_date, description, status){  
      self$log_user_event(user_id, description=glue("Heeft actie {action_name} gewijzigd"))
      
      try( 
        self$execute_query(glue("UPDATE {self$schema}.actionlist SET action_name = '{action_name}', registration_id = '{registration_id}', user_id = '{user_id}', action_date = '{action_date}', description = '{description}', status = '{status}', timestamp = '{Sys.time()}' WHERE action_id = {action_id}"))
      ) 
    },
    # archiveer actie in actielijst
    archive_action = function(action_id, user_id, action_name=NULL){
      
      self$log_user_event(user_id, description=glue("Heeft actie {ifelse(!is.null(action_name), action_name, action_id)} gearchiveerd"))
      
      try( 
        self$execute_query(glue("UPDATE {self$schema}.actionlist SET expired = TRUE, timestamp = '{Sys.time()}' WHERE action_id = {action_id}"))
      ) 
    },
      
    #######################################################
    # ---------------  DETAILPAGINA --------------------- #
    #######################################################
    # Voor person detail pagina
    get_person_from_id = function(person_id){
      dplyr::filter(self$person, (person_id)==(person_id)) 
    },
    get_tags_for_person = function(person_id){
      c('Ondernemerschap', 'Duurzaamheid')
    },
    get_address_from_id = function(address_id){
      dplyr::filter(self$address, address_id==address_id) 
    },
    
    
    
    # voor address detail pagina
    get_address_details = function(addreseerbaarobject){
      
      
    },
    # voor business detailpagina
    get_business_details = function(kvknummer){
      
      
    } 
    
  )
)