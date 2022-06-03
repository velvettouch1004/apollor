#' Business logic of the Apollo application
#' @description An R6 object with methods for use in the Shiny application `apollo`. 
#' @importsFrom shintobag shinto_db_connection
#' @importsFrom pool dbPool poolClose
#' @importsFrom R6 R6Class
#' @importsFrom dbplyr in_schema collect
#' @importsFrom dplyr tbl left_join 
#' @importsFrom plyr join_all 
#' @importsFrom safer encrypt_string decrypt_string
#' @export

ApolloEngine <- R6::R6Class(
  inherit = databaseObject,
  
  lock_objects = FALSE,
  public = list(
    
    initialize = function(gemeente, schema, pool, config_file = "conf/config.yml"){
      
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
      
      self$persoon <- self$read_table("persoon")
      self$bedrijf <- self$read_table("bedrijf")
      self$adres <- self$read_table("adres")
      
      
    }, 
    ######################################################################
    # ---------------  APOLLO SPECIFIC FUNCTIONS ----------------------- #
    ######################################################################
  
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
    read_persoon = function(){ 
      self$persoon <- self$read_table('persoon') 
      self$persoon
    },
    read_bedrijf = function(){ 
      self$bedrijf <- self$read_table('bedrijf') 
      self$bedrijf
    }, 
    read_adres = function(){ 
      self$adres <- self$read_table('adres') 
      self$adres
    },
    read_favourites = function(){ 
      self$favourites <- self$read_table('favorieten') 
      self$favourites
    },
    read_log = function(){ 
      self$user_event_log <- self$read_table('user_event_log') 
      self$user_event_log
    },
    read_data_actualiteit = function(){ 
      self$data_actualiteit <- self$read_table('data_actualiteit') 
      self$data_actualiteit
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
       dplyr::left_join( self$actions, self$signals, by=c('registratie_id' ), suffix = c(".actie", ".signaal"),)
    },
    
    list_favourites = function(user=NULL, update=FALSE){
      if(is.null(self$bedrijf)  || update){
        self$read_bedrijf() 
      } 
      if(is.null(self$persoon)  || update){ 
        self$read_persoon()
      } 
      if(is.null(self$adres)  || update){ 
        self$read_adres()
      } 
      self$read_favourites()
      A <- dplyr::left_join( dplyr::filter( self$favourites, object_type == 'registratie')  , self$signals, by=c('oid' ='registratie_id'), suffix = c("fav", ".signaal"))
      B <- dplyr::left_join( dplyr::filter( self$favourites, object_type == 'persoon')  , self$persoon, by=c('oid'='persoon_id'), suffix = c("fav", ".persoon"))
      C <- dplyr::left_join( dplyr::filter( self$favourites, object_type == 'bedrijf')  , self$bedrijf, by=c('oid'='bedrijf_id'), suffix = c("fav", ".bedrijf"))
      D <- dplyr::left_join( dplyr::filter( self$favourites, object_type == 'adres')  , self$adres, by=c('oid'='adres_id'), suffix = c("fav", ".adres"))
       
      plyr::join_all(list(A,B,C,D), by='favo_id', type='left') 
    
    },
    ################################################
    # -------------- LOGGING --------------------- #
    ################################################  
    get_log_ping = function(){
      try( 
        self$execute_query(glue("select max(timestamp) from {self$schema}.user_event_log;"))
        
      ) 
    },
    
    
    log_user_event = function(username, action){
      
      self$append_data('user_event_log', 
                       data.frame (user  =  username,
                                   action =  action,
                                   timestamp = Sys.time()))
    },
    ###################################################
    # -------------- FAVOURITES --------------------- #
    ###################################################
    # add to favourites
    add_favourite = function(username, oid, object_type){
      self$log_user_event(username, action=glue("Heeft {object_type} {oid} aan favorieten toegevoegd"))
      
      try( 
        self$append_data('favorieten', 
                         data.frame (username =  username,
                                     oid = oid,
                                     object_type = object_type, 
                                     timestamp = Sys.time()))
      ) 
    },
    # remove from favourites
    remove_favourite = function(username, favo_id){
      self$log_user_event(username, action=glue("Heeft {favo_id} uit favorieten verwijderd"))
      
      try( 
        self$execute_query(glue("DELETE from {self$schema}.favorieten WHERE favo_id = {favo_id}"))
        
      ) 
    },
    
    
    #######################################################
    # ---------------  ACTIELIJST ----------------------- #
    #######################################################
    
    # add actie to actielijst
    create_action = function(registratie_id, username, actie_naam, datum_actie, omschrijving, status){
      self$log_user_event(username, action=glue("Heeft actie {actie_naam} aangemaakt"))
      
      try( 
        self$append_data('actielijst', 
                         data.frame (actie_naam = actie_naam,
                                     registratie_id  =  registratie_id,
                                     creator =  username,
                                     datum_actie = datum_actie,
                                     omschrijving = omschrijving,
                                     status = status,
                                     timestamp = Sys.time()))
      ) 
    },
    # update actie in actielijst
    update_action = function(action_id,actie_naam,  registratie_id, username, datum_actie, omschrijving, status){  
      self$log_user_event(username, action=glue("Heeft actie {actie_naam} gewijzigd"))
      
      try( 
        self$execute_query(glue("UPDATE {self$schema}.actielijst SET actie_naam = '{actie_naam}', registratie_id = {registratie_id}, creator = '{username}', datum_actie = '{datum_actie}', omschrijving = '{omschrijving}', status = '{status}', timestamp = '{Sys.time()}' WHERE actie_id = {action_id}"))
      ) 
    },
    # archiveer actie in actielijst
    archive_action = function(action_id, username, actie_naam=NULL){
      
      self$log_user_event(username, action=glue("Heeft actie {ifelse(!is.null(actie_naam), actie_naam, action_id)} gearchiveerd"))
      
      try( 
        self$execute_query(glue("UPDATE {self$schema}.actielijst SET archief = TRUE, timestamp = '{Sys.time()}' WHERE actie_id = {action_id}"))
      ) 
    },
      
    #######################################################
    # ---------------  DETAILPAGINA --------------------- #
    #######################################################
    # Voor persoon detail pagina
    get_person_from_bsn = function(bsn){
      dplyr::filter(self$persoon, bsn==bsn) 
    },
    get_tags_for_person = function(person_id){
      c('Ondernemerschap', 'Duurzaamheid')
    },
    get_adress_from_id = function(adres_id){
      dplyr::filter(self$persoon, adres_id==adres_id) 
    },
    
    
    
    # voor adres detail pagina
    get_adres_details = function(addreseerbaarobject){
      
      
    },
    # voor bedrijf detailpagina
    get_bedrijf_details = function(kvknummer){
      
      
    } 
    
  )
)