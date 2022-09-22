#' Business logic of the Apollo application
#' @description An R6 object with methods for use in the Shiny application `apollo`. 
#' @importFrom pool dbPool poolClose
#' @importFrom R6 R6Class
#' @importFrom dbplyr in_schema  
#' @importFrom dplyr tbl left_join collect bind_rows
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
    
    # ---- Init
    initialize = function(gemeente, schema, pool, 
                          config_file = getOption("config_file","conf/config.yml"),
                          secret = "",
                          data_files = "", 
                          geo_file = NULL,
                          load_data = TRUE,
                          use_cache = TRUE){
      
      flog.info("DB Connection", name = "DBR6")
      
      self$gemeente <- gemeente
      self$pool <- pool
      self$schema <- schema 
      what <- gemeente
      
      # symmetric encrypt/decrypt
      self$secret <- secret
      
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
      
      self$data_files <- data_files
      
      if(load_data){
        if(self$has_dataset("person") || paste(self$data_files, collapse = '') == ""){
          print("READ PERSON")
          self$read_person(use_cache)
        }
        
        if(self$has_dataset("business") || paste(self$data_files, collapse = '') == ""){
          print("READ BUSINESS")
          self$read_business(use_cache)
        }
        
        if(self$has_dataset("address") || paste(self$data_files, collapse = '') == ""){
          print("READ ADDRESS")
          self$read_address(use_cache)
        }
        
        if(self$has_dataset("indicator") || paste(self$data_files, collapse = '') == ""){
          print("READ INDICATOR")
          self$read_indicator()
        }
        
        if(self$has_dataset("signals") || paste(self$data_files, collapse = '') == ""){
          print("READ SIGNALS")
          self$read_signals()
        }
        
        if(self$has_dataset("metadata") || paste(self$data_files, collapse = '') == ""){
          print("READ METADATA")
          self$read_metadata()
        }
        
        if(self$has_dataset("relocations") || paste(self$data_files, collapse = '') == ""){
          print("READ RELOCATIONS")
          self$read_relocations()
        }
        
        if(self$has_dataset("model_privacy_protocol") || paste(self$data_files, collapse = '') == ""){
          print("READ MPP")
          self$model_privacy_protocol <- self$read_table("model_privacy_protocol")
        }
        
      }
      
      
      # BAG connectie
      self$bag_con <- shintobag::shinto_db_connection("data_bag", file = config_file)
      self$bag_columns <- dbListFields(self$bag_con, Id(schema = "bagactueel", table = "adres_plus"))
      
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
      
      # CBS kerncijfers
      self$cbs_kern_buurten <- self$get_cbs_buurt_data()
      self$cbs_kern_metadata <- self$get_cbs_buurt_metadata()
      
      
      
    },
    
    
    #----- Encrypt/decrypt utilities
    encrypt = function(x){
      if(self$secret != ""){
        out <- shintobag::encrypt(x, secret = self$secret)
        out[is.na(x)] <- NA_character_
      } else {
        out <- x
      }
      out
    },
    
    decrypt = function(x){
      if(self$secret != ""){
        shintobag::decrypt(x, secret = self$secret)
      } else {
        x
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
    #' TODO this is not vectorized!
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
    
    #' @description 
    #' @param buurt_code_cbs If NULL, gets all buurt data for this gemeente, otherwise
    #' only the selected buurt code.s.
    get_cbs_buurt_data = function(buurt_code_cbs = NULL){
      
      if(is.null(buurt_code_cbs)){
        out <- tbl(self$cbs_con, "cbs_kerncijfers_2013_2021") %>%
          filter(regio_type == "Buurt",
                 gm_naam == !!self$gemeente) %>%
          collect
      } else {
        out <- tbl(self$cbs_con, "cbs_kerncijfers_2013_2021") %>%
          filter(regio_type == "Buurt",
                 gwb_code == !!buurt_code_cbs) %>%
          collect  
      }
      
      rename(out, buurt_code_cbs = gwb_code)
      
      
    },
    
    get_cbs_buurt_metadata = function(){
      
      tbl(self$cbs_con, "cbs_kerncijfers_2013_2021_metadata") %>%
        collect
      
    },
    
    street_from_bagid = function(id){
      
      query <- glue::glue(
        "select openbareruimtenaam from bagactueel.adres_plus ",
        "where adresseerbaarobject_id = '{id}'"
      )
      
      DBI::dbGetQuery(self$bag_con, query)[[1]]
      
    },
    
    get_bag_from_bagid = function(id, spatial = FALSE, geo_only = FALSE, request_cols = NULL){
      if(!geo_only){
        if(is.null(request_cols)){
          cols <- "*"
        } else {
          all_cols_correct <- identical(setdiff(request_cols, self$bag_columns), character(0))
          if(!all_cols_correct){
            stop("Not all requested columns are present in bagactueel.adres_plus!")
          }
          cols <- paste0(request_cols, collapse = ",") 
        }
      } else { 
        cols <- "adresseerbaarobject_id, geopunt"
      }
      
      data_out <- data.frame(adresseerbaarobject_id = id)
      
      id_lookup <- unique(id[!is.na(id)])
      if(length(id_lookup) == 0){
        return(NULL) # TODO might need something else here
      }
      
      query <- glue::glue(
        "select {cols} from bagactueel.adres_plus ",
        "where adresseerbaarobject_id in {self$to_sql_string(id_lookup)}"
      )
      
      if(!spatial){
        out <- DBI::dbGetQuery(self$bag_con, query)  
      } else {
        out <- sf::st_read(self$bag_con, query = query) 
        if(nrow(out) > 0)out <- out %>% st_transform(4326)
      }
      
      left_join(data_out, out, by = "adresseerbaarobject_id")
      
    },
    
    join_bag_geometry = function(data, id_column = "adresseerbaarobject_id"){
      
      data_bag <- self$get_bag_from_bagid(data[[id_column]], spatial = TRUE, geo_only = TRUE)
      
      if(!is.null(data_bag)){
        st_as_sf(left_join(data, data_bag, by = setNames("adresseerbaarobject_id",id_column)))  
      } else {
        data
      }
      
      
    },
    
    join_bag = function(data, id_column = "adresseerbaarobject_id", bag_columns = NULL){
      # SQL Injection Sensitive?
      data_bag <- self$get_bag_from_bagid(data[[id_column]], spatial = TRUE, geo_only = FALSE, request_cols = bag_columns)
      
      st_as_sf(left_join(data, data_bag, by = setNames("adresseerbaarobject_id",id_column)))
      
    },
    
    
    #--------  UTILITIES -----
    
    
    to_json = function(x){
      jsonlite::toJSON(x, auto_unbox = TRUE)
    },
    
    from_json = function(x){
      jsonlite::fromJSON(x)
    },
    
    has_dataset = function(what){
      what %in% self$data_files
      
    },
    
    #----  APOLLO SPECIFIC FUNCTIONS ----
    
    is_local = function(){
      Sys.getenv("CONNECT_SERVER") == ""
    },
    
    last_data_update = function(){
      
      db_info <- self$read_table("db_info")
      as.POSIXct(paste(db_info$last_updated_date, db_info$last_updated_time), tz="UTC")
      
    },
    
    # Cache reader
    read_table_cached = function(table, rewrite_cache = FALSE){
      
      cache_path <- ifelse(self$is_local(), 
                           glue::glue("cache/{tolower(self$gemeente)}-ondermijning"), 
                           glue::glue("/data/{tolower(self$gemeente)}-ondermijning"))
      
      if(self$is_local()){
        dir.create(cache_path, showWarnings = FALSE)
      }
      
      rds_name <- paste0(table, ".rds")
      rds_path <- file.path(cache_path, rds_name)
      timestamp_path <- file.path(cache_path, paste0(table,"_timestamp.rds"))
      save_timestamp <- function()saveRDS(as.POSIXct(format(Sys.time()),tz="UTC"), timestamp_path)
      
      cache_is_expired <- file.exists(timestamp_path) && self$last_data_update() > readRDS(timestamp_path)
      
      if(!file.exists(rds_path) || !file.exists(timestamp_path) || cache_is_expired || rewrite_cache){
        flog.info(glue("Reading {table} from Postgres, saving in cache."))
        data <- self$read_table(table)
        saveRDS(data, rds_path)
        
        # timestamp
        save_timestamp()
      } else {
        flog.info(glue("Reading {table} from cache."))
        data <- readRDS(rds_path)
        
        # timestamp not yet saved, data is (old cache)
        if(!file.exists(timestamp_path)){
          save_timestamp()
        }
      }
      
      return(data)
      
    },
    
    
    read_person = function(cache = TRUE){
      if(cache){
        self$person <- self$read_table_cached("person")
      } else {
        self$person <- self$read_table('person')   
      }
      
      invisible(self$person)
    },
    
    read_business = function(cache = TRUE){ 
      if(cache){
        self$business <- self$read_table_cached("business")
      } else {
        self$business <- self$read_table('business')   
      }
      invisible(self$business)
    }, 
    
    read_address = function(cache = TRUE){ 
      if(cache){
        self$address <- self$read_table_cached("address")
      } else {
        self$address <- self$read_table('address')   
      }
      invisible(self$address)
    },
    
    read_relocations = function(cache = TRUE){
      if(cache){
        self$relocations <- self$read_table_cached("brp_verhuis_historie")
      } else {
        self$relocations <- self$read_table('brp_verhuis_historie')   
      }
      invisible(self$relocations)
    },
    
    
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
    
    read_metadata = function(){ 
      self$metadata <- self$read_table('metadata') 
      invisible(self$metadata)
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
    read_mpp = function(registration_id=NULL, archived=FALSE){ 
      if(is.null(registration_id)){
        self$model_privacy_protocol <- self$read_table('model_privacy_protocol') 
      } else {
        self$model_privacy_protocol <- self$query(glue("select * from {self$schema}.model_privacy_protocol where registration_id = '{registration_id}' and archived = {archived};")) 
        
      }    
      invisible(self$model_privacy_protocol)
    },
    
    
    #----- TRANSPARACY ----
    get_indicator = function(indicator_name){
      self$indicator$label[match(indicator_name, self$indicator$indicator_name)]
      
    },
    get_metadata = function(){
      
      self$metadata 
      
      
    },
    set_metadata = function(name, label, timestamp_provided,  owner, depends_on, step, colnames, description, timestamp_processed=Sys.time() ){
      
      
      metadata <- tibble::tibble(
        name=name, 
        label=label, 
        timestamp_provided=timestamp_provided, 
        timestamp_processed=timestamp_processed, 
        owner=owner, 
        depends_on=self$to_json(depends_on), 
        step=step, 
        colnames=self$to_json(colnames), 
        description=description
      )
      
      try( 
        self$append_data('metadata', metadata)
      ) 
      
    },
    
    edit_indicator_filter_transparency = function(id, short_desc, long_desc, depends, def, calc){
      
      if(is.na(depends) && is.null(depends)){
        depends <- "[]" 
      } 
      
      if(is.null(self$schema)){
        qu <- glue("UPDATE indicator SET description = '{short_desc}', description_long = '{long_desc}', depends_on = ?val_depends, definitie = '{def}', berekening = '{calc}', datum_wijziging  = '{now()}' WHERE indicator_id = '{id}'") %>%
          as.character()
      } else {
        qu <- glue("UPDATE {self$schema}.indicator SET description = '{short_desc}', description_long = '{long_desc}', depends_on = ?val_depends, definitie = '{def}', berekening = '{calc}', datum_wijziging  = '{now()}'WHERE indicator_id = '{id}'") %>%
          as.character()
      }
      
      query <- sqlInterpolate(DBI::ANSI(), qu, val_depends = depends)
      
      dbExecute(self$con, query)
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
                             disabled = FALSE){
      
      type <- match.arg(type) 
      
      data <- tibble::tibble(
        indicator_name = indicator_name,
        object_type = type,
        label = label,
        description = description,
        user_id = user_id,
        theme = self$to_json(theme),
        #columns = self$to_json(columns),
        #weight = weight,
        #threshold = threshold,
        timestamp = format(Sys.time()),
        disabled = as.logical(disabled)
      )
      
      self$append_data("indicator", data)
      
      
    },
    
    #' @description Remove an indicator's metadata from the 'indicator' table0
    remove_indicator = function(indicator_name){ 
      
      self$delete_rows_where("indicator", "indicator_name", indicator_name)
      
    },
    
    #' @description Get all rows of indicators table that are currently active
    #' @details Use this function to find definitions for indicators
    get_indicators_all = function(){
      
      out <- self$indicator %>% 
        filter(!disabled, !deleted)
      
      out
      
    },
    
    
    #' @description Get rows of indicators table for a theme
    #' @details Use this function to find definitions for indicators in a theme.
    get_indicators_theme = function(theme){
      
      out <- self$indicator %>% 
        filter(grepl(!!theme, theme),
               !disabled, !deleted)
      
      if(nrow(out) == 0){
        stop(paste("Theme",theme,"not found"))
      }
      
      out
      
    },
    
    get_indicators_riskmodel = function(user_id, theme = NULL, gemeente){
      
      # Get user settings
      data <- self$read_table("indicator_riskmodel", lazy = TRUE)
      
      if(!is.null(theme)){
        out <- data %>%
          filter(user_id == !!user_id, theme == !!theme) %>%
          collect
      } else {
        out <- data %>%
          filter(user_id == !!user_id) %>%
          collect
      }
      
      
      # Don't have those? Get gemeente settings.
      if(nrow(out) == 0){
        
        if(!is.null(theme)){
          out <- data %>%
            filter(user_id == !!gemeente, theme == !!theme) %>%
            collect
        } else {
          out <- data %>%
            filter(user_id == !!gemeente) %>%
            collect
        }
      }
      
      # get indicators that are not disabled 
      # disabled is a global setting; not per user!
      if(!is.null(theme)){
        indi <- self$get_indicators_theme(theme)  
      } else {
        indi <- self$get_indicators_all()
      }
      
      out <- filter(out, indicator_name %in% indi$indicator_name)
      
      out
    },
    
    #' @description Delete riskmmodel settings for a user (so that the gemeente default
    #' is used again)
    delete_user_riskmodel = function(user_id){
      
      # Get user settings
      self$execute_query(glue::glue("DELETE from {self$schema}.indicator_riskmodel WHERE user_id = '{user_id}'"))
      
    },
    
    #' @description Which users have user-specific riskmodel settings for this theme?
    get_users_riskmodel = function(theme){
      
      self$query(glue::glue("select distinct user_id from {self$schema}.indicator_riskmodel where theme = '{theme}'"))[[1]]
    },
    
    
    #' @description Convert raw indicator data to TRUE/FALSE
    #' @param data Dataframe subset from `indicator` table, read with `$get_indicators_theme` or `$get_indicators_all`
    #' @param indicator Name of the indicator to convert
    make_boolean_indicator = function(data, indicator){
      
      def <- filter(data, indicator_name == !!indicator)
      
      # in case $get_indicators_all is used, take only first value (everthing besides theme is the same)
      if(nrow(def)>1){
        def <- def[1,]
      }
      
      if(def$object_type == "person"){
        tab <- self$person
      }
      if(def$object_type == "address"){
        tab <- self$address
      }
      if(def$object_type == "business"){
        tab <- self$business
      }
      
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
    make_indicator_table = function(theme = NULL, 
                                    type = c("address","person","business"),
                                    id_columns = c("address_id","buurt_code_cbs"),
                                    user_id, gemeente
    ){
      
      type <- match.arg(type)
      
      if(!is.null(theme)){
        # Get indicators for this theme / type
        def <- self$get_indicators_theme(theme) %>%
          filter(object_type == !!type)  
      } else {
        # Get indicators for this type
        def <- self$get_indicators_all() %>%
          filter(object_type == !!type)  
      }
      
      
      # Get risk parameters for these indicators / this user / this gemeente
      risk <- self$get_indicators_riskmodel(user_id = user_id,
                                            theme = theme, 
                                            gemeente = gemeente) %>%
        filter(indicator_name %in% !!def$indicator_name) %>%
        mutate(object_type = type)  # needed in $make_boolean_indicator
      
      
      # TODO was cleaner maar werkt niet meer online (????)
      # maak een $get methode.
      if(type == "person"){
        tab <- self$person
      }
      if(type == "address"){
        tab <- self$address
      }
      if(type == "business"){
        tab <- self$business
      }
      
      # Selecteer alleen de adres id en buurt code,
      tab <- tab %>% select(all_of(!!id_columns))
      
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
    calculate_riskmodel = function(data, theme = NULL, user_id, gemeente){
      
      # Definition for this theme
      def <- self$get_indicators_riskmodel(user_id, theme, gemeente)
      
      if(!all(def$indicator_name %in% names(data))){
        
        # disabled indicators are still in the riskmodel table
        def <- filter(def, indicator_name %in% names(data))
      }
      
      # Matrix multiply ftw
      m <- as.matrix(data[, def$indicator_name])
      data$riskmodel <- as.vector(m %*% def$weight)
      
      data
    },
    
    
    #---- RISKMODEL ADMIN -----
    
    #' @description Set a value of a column given an indicator ID in the 
    #' indicator table, e.g. label, description, disabled, deleted,theme
    #'  (json or vector) etc.
    set_indicator_field = function(indicator_id, field, value){
      
      self$replace_value_where(table = "indicator", 
                               col_replace = field, 
                               val_replace = value, 
                               col_compare = "indicator_id", 
                               val_compare = indicator_id)
      
    },
    
    #' @description Set a value of a column given an indicator ID in the 
    #' indicator_riskmodel table, e.g. label, description, disabled, deleted,theme
    #'  (json or vector) etc.
    set_indicator_riskmodel_field = function(indicator_id, field, value){
      
      self$replace_value_where(table = "indicator_riskmodel", 
                               col_replace = field, 
                               val_replace = value, 
                               col_compare = "risk_id", 
                               val_compare = indicator_id)
      
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
                               timestamp = format(Sys.time()),
                               threshold = threshold,
                               weight = weight,
                               theme = theme,
                               indicator_name = indicator_name)
        
        self$append_data("indicator_riskmodel", data_new)
        
      } else {
        # update
        
        self$set_indicator_riskmodel_field(data_old$risk_id, "weight", value = weight)
        self$set_indicator_riskmodel_field(data_old$risk_id, "threshold", value = threshold)
        self$set_indicator_riskmodel_field(data_old$risk_id, "timestamp", value = format(Sys.time()))
        
        
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
      
      
      dplyr::left_join( self$actions, self$signals, by=c('registration_id' ), suffix = c("", ".signaal"))
      
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
    archive_MPP_for_registration_old = function(registration_id, user_id, createLog=TRUE){
      if(createLog){ self$log_user_event(user_id, description=glue("Heeft het privacy protocol van registratie {registration_id} gerachiveerd"))}
      
      try( 
        self$execute_query(glue("UPDATE {self$schema}.model_privacy_protocol SET archived = TRUE, timestamp = '{Sys.time()}' WHERE registration_id = '{registration_id}'"))
      ) 
      
    },
    
    archive_MPP_for_registration = function(registration_id, user_id, mpp_names, createLog=TRUE){
      if(createLog){ self$log_user_event(user_id, description=glue("Heeft het privacy protocol van registratie {registration_id} gerachiveerd"))}
      
      try( 
        self$execute_query(glue("UPDATE {self$schema}.model_privacy_protocol SET archived = TRUE, timestamp = '{Sys.time()}' WHERE mpp_name IN ('{glue_collapse(mpp_names, sep = \"','\" )}') and registration_id = '{registration_id}'"))
      ) 
      
    },
    #' @description Update mpp for registration
    updateMPP = function(registration_id, data, user_id){
      
      self$log_user_event(user_id, description=glue("Heeft het privacy protocol van registratie {registration_id} gewijzigd"))  
      data_formatted <- data %>% select(mpp_name, bool_val, text_val, checklist)
      self$archive_MPP_for_registration_old(registration_id, user_id, createLog=FALSE)
      self$create_MPP_for_registration(registration_id, user_id, data_formatted, createLog=FALSE)
      
    },
    
    update_MPP_for_registration = function(registration_id, data, user_id, registration_name=NULL){
      if(!is.null(registration_name)){
        self$log_user_event(user_id, description=glue("Heeft het privacy protocol velden {paste(data$mpp_name)} van registratie {registration_name} gewijzigd"))  
      } else {
        
        #self$log_user_event(user_id, description=glue("Heeft het privacy protocol velden {paste(data$mpp_name)} van registratie {registration_id} gewijzigd")) 
      }
      
      
      self$archive_MPP_for_registration(registration_id, user_id, data$mpp_name, createLog=FALSE) 
      
      self$create_MPP_for_registration(registration_id, user_id, data, createLog=FALSE)
      
    },
    
    #---------  ACTIELIJST -----------
    
    
    
    #' @description Add action to actionlist
    #' @param uid User that creates the action
    #' @param acdate Date (yyyy-mm-dd) action occurs 
    #' @param desc Content of action 
    #' @param acname Short title
    #' @param rid Signal where the action relates to
    #' @param gid Group ID the action is a part of 
    #' @param gname Group name the action is a part of
    #' @param aid The address the action is registered for
    create_action = function(uid, acdate, desc, stat, acname, rid, gid, gname, aid, uitv){
      self$log_user_event(uid, description=glue("Heeft actie {acname} aangemaakt"))
      
      try( 
        self$append_data('actionlist', 
                         data.frame(user_id =  uid,
                                    action_date = acdate,
                                    description = desc,
                                    status = stat,
                                    timestamp = Sys.time(),
                                    expired = FALSE, 
                                    action_name = acname,
                                    registration_id  =  rid,
                                    group_id = gid,
                                    group_name = gname,
                                    address_id = aid,
                                    uitvoerder = uitv
                         ))
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