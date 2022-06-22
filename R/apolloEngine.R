#' Business logic of the Apollo application
#' @description An R6 object with methods for use in the Shiny application `apollo`. 
#' @importFrom shintobag shinto_db_connection
#' @importFrom pool dbPool poolClose
#' @importFrom R6 R6Class
#' @importFrom dbplyr in_schema  
#' @importFrom dplyr tbl left_join collect bind_rows
#' @importFrom plyr join_all 
#' @importFrom safer encrypt_string decrypt_string
#' @importFrom jsonlite toJSON fromJSON
#' @importFrom tibble tibble
#' @export

ApolloEngine <- R6::R6Class(
  inherit = databaseObject,
  
  lock_objects = FALSE,
  
  public = list(
    
    # variables for recoding events
    recode_icon = c(verhuisd_uit = "person-dash-fill", 
                    verhuisd_naar = "person-plus-fill", 
                    verhuisd_binnen = "arrows-move", 
                    geboorte = "balloon-fill"), 
    recode_title = c(verhuisd_uit = "Verhuisd buiten de gemeente", 
                     verhuisd_naar = "Verhuisd naar de gemeente", 
                     verhuisd_binnen = "Verhuisd binnen gemeente", 
                     geboorte = "Geboren"), 
    recode_icon_status = c(verhuisd_uit = "danger", 
                           verhuisd_naar = "success", 
                           verhuisd_binnen = "info", 
                           geboorte = "success"),
    
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
      
      self$read_person()
      self$read_business()
      self$read_address()
      self$read_indicator()
      self$read_signals()
      
      self$relocations <- self$read_table("brp_verhuis_historie")
      self$model_privacy_protocol <- self$read_table("model_privacy_protocol") 
      
      # BAG connectie
      self$bag_con <- shintobag::shinto_db_connection("data_bag", file = config_file)
      
      # CBS connectie
      self$cbs_con <- shintobag::shinto_db_connection("data_cbs", file = config_file)
      
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
    
    

#----- GEO UTILITIES ----

    
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
    add_geo_columns = function(data, spatial = TRUE){
      
      self$assert_geo()
      
      if(!"buurt_code_cbs" %in% names(data)){
        stop("buurt_code_cbs must be a column in data")
      }
      
      geo_cols <- c("buurt_naam","wijk_code_cbs","wijk_naam","gemeente_naam")
      have_geo_cols <- intersect(names(data), geo_cols)
      if(length(have_geo_cols)){
        data <- select(data, -all_of(have_geo_cols))
      }
      
      geo <- self$geo$buurten
      if(!spatial){
        geo <- sf::st_drop_geometry(geo)
      }
      
      key <- select(geo,
                    buurt_code_cbs = bu_code,
                    buurt_naam = bu_naam,
                    wijk_code_cbs = wk_code,
                    wijk_naam = wk_naam,
                    gemeente_naam = gm_naam)
      
      left_join(data, key, by = "buurt_code_cbs")
                    
      
    },
    
    
    get_cbs_buurt_data = function(buurt_code_cbs){
      
      tbl(self$cbs_con, "cbs_kerncijfers_2013_2021") %>%
        filter(regio_type == "Buurt",
               gwb_code == !!buurt_code_cbs) %>%
        collect
      
    },
    
    get_cbs_buurt_metadata = function(){
      
      tbl(self$cbs_con, "cbs_kerncijfers_2013_2021_metadata") %>%
        collect
      
    },
    

#--------  UTILITIES -----

    
    to_json = function(x){
      jsonlite::toJSON(x, auto_unbox = TRUE)
    },
    
    from_json = function(x){
      jsonlite::fromJSON(x)
    },
    
    

#----  APOLLO SPECIFIC FUNCTIONS ----
  
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
    read_favorites = function(user_id=NULL){ 
      if(is.null(user_id)){
        self$favorites <- self$read_table('favorites') 
      } else {
        self$favorites <- self$query(glue("select * from {self$schema}.favorites where user_id = '{user_id}';")) 
        
      }    
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
    read_mpp = function(registration_id=NULL, archived=FALSE){ 
      if(is.null(registration_id)){
        self$model_privacy_protocol <- self$read_table('model_privacy_protocol') 
      } else {
        self$model_privacy_protocol <- self$query(glue("select * from {self$schema}.model_privacy_protocol where registration_id = '{registration_id}' and archived = {archived};")) 
        
      }    
      invisible(self$model_privacy_protocol)
    },
    
    

#--------- INDICATOR FUNCTIONS ----------

    
    
    #' @description Get labels for indicators
    #' @param indicator_name Vector of indicator names
    label_indicator = function(indicator_name){
      
      self$indicator$label[match(indicator_name, self$indicator$indicator_name)]
      
    },
    
    
    #' @description Adds an indicator's metadata to the 'indicator' table
    #' @param column_name Name of the column (indicator)
    #' @param type Indicator type - refers to the table that has this column_name
    #' @param label Short label for the indicator
    #' @param description Long-form text explaining the indicator
    #' @param user_id Who are you?
    #' @param theme Which theme(s) is the indicator used in? e.g. c("mensenhandel","drugs")
    #' @param columns 
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
    
    
    get_indicators_riskmodel = function(user_id, theme, gemeente){
      
        # Get user settings
        data <- self$read_table("indicator_riskmodel", lazy = TRUE) %>%
          filter(user_id == !!user_id, theme == !!theme) %>%
          collect
        
        # Don't have those? Get gemeente settings.
        if(nrow(data) == 0){
          data <- self$read_table("indicator_riskmodel", lazy = TRUE) %>%
            filter(user_id == !!gemeente, theme == !!theme) %>%
            collect
        }
      
      data
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
                                    id_columns = c("address_id","buurt_code_cbs"),
                                    user_id, gemeente
                                    ){
      
      type <- match.arg(type)
      
      # Get indicators for this theme / type
      def <- self$get_indicators_theme(theme) %>%
        filter(object_type == !!type)
      
      # Get risk parameters for these indicators / this user / this gemeente
      risk <- self$get_indicators_riskmodel(user_id = user_id,
                                            theme = theme, 
                                            gemeente = gemeente) %>%
        filter(indicator_name %in% !!def$indicator_name) %>%
        mutate(object_type = type)  # needed in $make_boolean_indicator
      
      # Selecteer alleen de adres id en buurt code,
      tab <- self[[type]] %>% select(all_of(!!id_columns))

      # voeg alle boolean indicators toe
      i_data <- lapply(def$indicator_name, function(x){
        self$make_boolean_indicator(data = risk, indicator = x)
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
    calculate_riskmodel = function(data, theme, user_id, gemeente){
      
      # Definition for this theme
      def <- self$get_indicators_riskmodel(user_id, theme, gemeente)
      
      if(!all(def$indicator_name %in% names(data))){
        stop("Some indicator definitions not found in data!")
      }
      
      # Matrix multiply ftw
      m <- as.matrix(data[, def$indicator_name])
      data$riskmodel <- as.vector(m %*% def$weight)
      
      data
    },


#---- RISKMODEL ADMIN -----


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


    #' @description Write weight/threshold for an indicator in a theme for a user
    #' @details Written to table indicator_riskmodel. If the entry does not exist for the user,
    #' add a new row for this user. If it does exist, use an UPDATE statement to replace the value.
    write_indicator_riskmodel = function(user_id, indicator_name, theme, weight, threshold){
      
      data_old <- self$read_table("indicator_riskmodel", lazy = TRUE) %>%
        filter(theme == !!theme, user_id == !!user_id, indicator_name == !!indicator_name) %>%
        collect
      
      if(nrow(data_old) == 0){
      # append  
        
        data_new <- data.frame(user_id = user_id,
                               indicator_name = indicator_name,
                               theme = theme,
                               weight = weight,
                               threshold = threshold,
                               timestamp = format(Sys.time()))
        
        self$append_data("indicator_riskmodel", data_new)
        
      } else {
      # update
        
        self$replace_value_where(
          table = "indicator_riskmodel", 
          col_replace = "threshold", 
          val_replace = threshold, 
          col_compare = "risk_id", 
          val_compare = data_old$risk_id
        )
        
        self$replace_value_where(
          table = "indicator_riskmodel", 
          col_replace = "weight", 
          val_replace = weight, 
          col_compare = "risk_id", 
          val_compare = data_old$risk_id
        )
        
        self$replace_value_where(
          table = "indicator_riskmodel", 
          col_replace = "timestamp", 
          val_replace = format(Sys.time()), 
          col_compare = "risk_id", 
          val_compare = data_old$risk_id
        )
        
      }
      
      
      
      
    },
    
    

#--------- LIST FUNCTIONS -----------
    
    #' @description Get the active actions ordered by date
    #' @param
    get_active_actions= function(){
      self$actions %>% filter(expired==FALSE) %>% arrange(desc(action_date))
    },
    list_actions = function(update=FALSE){
      if(is.null(self$actions)  || update){
        self$read_actions() 
      } 
      if(is.null(self$signals)  || update){ 
        self$read_signals()
      }  

       dplyr::left_join( self$actions, self$signals, by=c('registration_id' ), suffix = c(".actie", ".signaal"))

    },
    
    list_favorites = function(user_id=NULL, update=FALSE){
      if(is.null(self$business)  || update){
        self$read_business() 
      } 
      if(is.null(self$person)  || update){ 
        self$read_person()
      } 
      if(is.null(self$address)  || update){ 
        self$read_address()
      } 
      self$read_favorites(user_id)
      #A <- dplyr::left_join( dplyr::filter( self$favorites, object_type == 'registration')  , self$signals, by=c('object_id' = 'registration_id'), suffix = c(".fav", ".signaal"))
      B <- dplyr::left_join( dplyr::filter( self$favorites, object_type == 'person')  , self$person, by=c('object_id'='person_id'), suffix = c(".fav", ".person"))
      #C <- dplyr::left_join( dplyr::filter( self$favorites, object_type == 'business')  , self$business, by=c('object_id'='business_id'), suffix = c(".fav", ".business"))
      #D <- dplyr::left_join( dplyr::filter( self$favorites, object_type == 'address')  , self$address, by=c('object_id'='address_id'), suffix = c(".fav", ".address"))
       B
      #plyr::join_all(list(A,B,C,D), by='favorite_id', type='left') 
    
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
    
    #' @description Get addresses based on address_id
    #' @param address_id Vector of address IDs
    list_addresses_by_id = function(address_id = NULL){
      
      dplyr::filter(self$address, 
                    address_id %in% !!address_id)
    
    },
    
    
    

#--------- LOGGING -------------

    get_log_ping = function(){
      try( 
        self$execute_query(glue("select max(timestamp) from {self$schema}.user_event_log;"))
        
      ) 
    },
    
    #' @description Log an user action
    #' @param user_id User that performed action
    #' @param description The action perfomed
    log_user_event = function(user_id, description){
      
      self$append_data('user_event_log', 
                       data.frame (user_id  =  user_id,
                                   description =  description,
                                   timestamp = Sys.time()))
    },
    

#-------------- FAVORITES -----------

    
    
    #' @description check if object is currently in favorites of user_id 
    #' @param user_id User to check for
    #' @param object_id Identifier of the object
    #' @param object_type Type of object (address, person, business or registration)
    get_favorite = function(user_id, object_id, object_type=NULL) { 
      self$query(glue("select favorite_id from {self$schema}.favorites where user_id='{user_id}' and object_id='{object_id}';")) 
    },
    
    #' @description Add favorite to list
    #' @details Favorites can be addresses, persons, businesses and registrations
    #' @param user_id User to add to
    #' @param object_id Identifier of the object
    #' @param object_type Type of object (address, person, business or registration)
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
    
    #' @description Remove from favorites 
    #' @param user_id User to remove from
    #' @param favorite_id Identifier of the removable
    remove_favorite = function(user_id, favorite_id){
      self$log_user_event(user_id, description=glue("Heeft {favorite_id} uit favorieten verwijderd"))
      
      try( 
        self$execute_query(glue("DELETE from {self$schema}.favorites WHERE favorite_id = {favorite_id} and user_id = '{user_id}'")) 
      ) 
    },
    

#--------- PRIVACY PROTOCOL  -------

    #' @description Create MPP for registration
    create_MPP_for_registration = function(registration_id, user_id, data, createLog=TRUE){
      if(createLog) {self$log_user_event(user_id, description=glue("Heeft het privacy protocol van registratie {registration_id} aangemaakt"))}
      
      data$registration_id <- rep(registration_id, nrow(data))
      data$user_id <- rep(user_id, nrow(data))
      data$timestamp <- rep(Sys.time(), nrow(data))
       
      try( 
        self$append_data('model_privacy_protocol', 
                         data)
      )  
    },
    #' @description Archive mpp for registration
    archive_MPP_for_registration = function(registration_id, user_id, createLog=TRUE){
      if(createLog){ self$log_user_event(user_id, description=glue("Heeft het privacy protocol van registratie {registration_id} gerachiveerd"))}
      
      try( 
        self$execute_query(glue("UPDATE {self$schema}.model_privacy_protocol SET archived = TRUE, timestamp = '{Sys.time()}' WHERE registration_id = '{registration_id}'"))
      ) 
      
    },
    #' @description Update mpp for registration
    updateMPP = function(registration_id, data, user_id){
    
      self$log_user_event(user_id, description=glue("Heeft het privacy protocol van registratie {registration_id} gewijzigd"))  
      data_formatted <- data %>% select(mpp_name, bool_val, text_val)
      self$archive_MPP_for_registration(registration_id, user_id, createLog=FALSE)
      self$create_MPP_for_registration(registration_id, user_id, data_formatted, createLog=FALSE)
      
        
    },
    
    

#---------  ACTIELIJST -----------

    
    
    #' @description Add action to actionlist
    #' @param registration_id Signal where the action relates to
    #' @param user_id User that creates the action
    #' @param action_name Short title
    #' @param action_date Date (yyyy-mm-dd) action occurs 
    #' @param description Content of action 
    #' @param status Current action status 
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
    #' @description Update action in actionlist
    #' @param action_name Short title
    #' @param registration_id Signal where the action relates to
    #' @param user_id User that updates the action 
    #' @param action_date Date (yyyy-mm-dd) action occurs 
    #' @param description Content of action 
    #' @param status Current action status 
    update_action = function(action_id, action_name,  registration_id, user_id, action_date, description, status){  
      self$log_user_event(user_id, description=glue("Heeft actie {action_name} gewijzigd"))
      
      try( 
        self$execute_query(glue("UPDATE {self$schema}.actionlist SET action_name = '{action_name}', registration_id = '{registration_id}', user_id = '{user_id}', action_date = '{action_date}', description = '{description}', status = '{status}', timestamp = '{Sys.time()}' WHERE action_id = {action_id}"))
      ) 
    },
    
    #' @description Archive action in actionlist
    #' @param action_id Action to archive
    #' @param user_id User that archives the action
    #' @param action_name [optional] Short title 
    archive_action = function(action_id, user_id, action_name=NULL){
      
      self$log_user_event(user_id, description=glue("Heeft actie {ifelse(!is.null(action_name), action_name, action_id)} gearchiveerd"))
      
      try( 
        self$execute_query(glue("UPDATE {self$schema}.actionlist SET expired = TRUE, timestamp = '{Sys.time()}' WHERE action_id = {action_id}"))
      ) 
    },
      

#--------  DETAILPAGINA ---------

    
    #' @description Get person details from the identifier
    #' @param person_id Person's identifier FI: (pseudo)bsn 
    get_person_from_id = function(person_id){ 
      self$person[self$person$person_id == person_id, ]
    },
    
    #' @description Hardcoded method to simulate tags
    #' @param person_id Person's identifier FI: (pseudo)bsn 
    get_tags_for_person = function(person_id){
      c('Ondernemerschap', 'Duurzaamheid')
    },
    
    #' @description Get address details from the identifier
    #' @param address_id Address's identifier FI: (pseudo)adresseerbaarobject 
    get_address_from_id = function(address_id){
      self$address[self$address$address_id == address_id, ]
    }, 
    get_residents = function(address_id){
      self$person[self$person$address_id == address_id, ]
    },
    get_businesses_at_address = function(address_id){
      self$business[self$business$address_id == address_id, ]
    },
    get_relocations_for_person = function(person_id){
      self$relocations[self$relocations$person_id == person_id, ]
    }, 
    
    #' @description Create data suitable for timeline plot
    #' @param person_id Person's identifier FI: (pseudo)bsn 
    #' @param add_overlijden Boolean indicating if overlijden should be added as event 
    get_relocations_timeline = function(person_id, add_overlijden=TRUE){ 
      
      timelineData <- self$get_relocations_for_person(person_id) %>% 
        mutate(
          timestamp = event_datum,
          title=recode(event, !!!self$recode_title, .default = 'Onbekende gebeurtenis', .missing = 'Onbekende gebeurtenis'), 
          text=format_event_func(event, buurt_naam, gemeente_inschrijving),
          icon_name=recode(event, !!!self$recode_icon, .default = "bookmark", .missing = 'bookmark'), 
          icon_status=recode(event, !!!self$recode_icon_status, .default = 'warning', .missing = 'warning') 
        )
      if(add_overlijden & nrow(timelineData) > 0 & !is.na(timelineData$datum_overlijden[1])){
        death_row = data.frame(timestamp=timelineData$datum_overlijden[1],
                               title="Overleden",
                               text="Is overleden",
                               icon_name = "person-dash-fill",
                               icon_status="danger")
 
        timelineData <- bind_rows(timelineData, death_row)
      } 
      timelineData %>% distinct(timestamp,title,text, .keep_all = TRUE)
    },
    
    #' @description Create suitable node format for network(Viz)
    create_network_nodes = function(person_data=NULL, 
                                     address_data=NULL, 
                                     resident_data=NULL, 
                                     business_data=NULL, 
                                     registration_data=NULL){
      
      # intitialise node object with person data    
      if(!is.null(person_data)){
        network_nodes <- data.frame(label = person_data$person_id,   
                                  group = c("person"),          
                                  title = person_data$person_id,
                                  level = 0)   %>% 
          add_net_nodes(address_data, 'address_id', 'address_id', 'address', level= 2 )   %>%
          add_net_nodes(resident_data %>% filter(person_id != person_data$person_id), 'person_id', 'person_id', 'resident', level=0)
      }
      # intitialise node object with address data    
      else if(!is.null(address_data)){ 
        network_nodes <- data.frame(label = address_data$address_id,   
                                    group = c("address"),          
                                    title = address_data$address_id,
                                    level = 2)   %>%  
          add_net_nodes(resident_data ,  'person_id', 'person_id', 'resident', level=0)
        
      } else {
        network_nodes <-  data.frame(label = c(),   
                                     group =  c(),          
                                     title =  c(),
                                     level =  c())  
      }
        
      # add subsequent nodes
      network_nodes  %>%
        add_net_nodes(business_data, 'business_id', 'business_id', 'business',level=2) %>%
        add_net_nodes(registration_data, 'registration_id', 'registration_id', 'registration',level=5) %>% 
        mutate(id=row_number())
      
    },
      
    
    #' @description Create suitable edge format for network(Viz)
    create_network_edges = function(person_data=NULL, 
                                      address_data=NULL, 
                                      resident_data=NULL, 
                                      business_data=NULL, 
                                      registration_data=NULL){
        
      # intitialise edge object empty
      network_edges <- data.frame(label = c(), title = c())
      
      # naive -> only add edges if address is available
      if(!is.null(address_data) && nrow(address_data) > 0 && !is.null(person_data) && nrow(person_data) > 0){
        
        network_edges <- network_edges %>% 
          add_net_edges(person_data, 'Woont op', 'Woont op') %>%
          add_net_edges(address_data, 'is', 'is') %>%
          add_net_edges(resident_data %>% filter(person_id != person_data$person_id), 'Woont op', 'Woont op') %>%
          add_net_edges(business_data, 'Gevestigd op', 'Gevestigd op') %>%
          add_net_edges(registration_data, 'Signaal op', 'Signaal op') %>%
          mutate(from = row_number(), to=2) %>% # naive -> connect everything to address 
          filter(from != 2) # remove address identitiy
      }
      
      else if(!is.null(address_data) && nrow(address_data) > 0){
        
        network_edges <- network_edges %>%  
          add_net_edges(address_data, 'is', 'is') %>%
          add_net_edges(resident_data, 'Woont op', 'Woont op') %>%
          add_net_edges(business_data, 'Gevestigd op', 'Gevestigd op') %>%
          add_net_edges(registration_data, 'Signaal op', 'Signaal op') %>%
          mutate(from = row_number(), to=1) %>% # naive -> connect everything to address 
          filter(from != 1) # remove address identitiy
      }  
      
       network_edges 
       
    }
  )  
)