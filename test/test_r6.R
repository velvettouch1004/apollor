

library(microbenchmark)
library(tinytest)
library(here)
library(DBI)
library(glue)
library(futile.logger)

library(dplyr)
library(yaml)


# Test het system warehouse object. 
if(FALSE){
  print('-- using custom --') 
  testconf <- read_yaml('conf/testconf.yml') 
} else { 
  print('-- using default --') 
  testconf <- list(
    "apollo specific" = T,
    "logging" = T,
    "favorites" = T,
    "actionlist" = T,
    "list functions" = T,
    "details" = T,
    "timeline" = T,
    "network" =T,
    "indicators" = T,
    "geo"=T,
    "riskmodel" = T,
    "MPP" = T,
    "metadata" = T
  )
}
 

options(config_file = glue("conf/config.yml"))

#library(apollor)
devtools::load_all()

.sys <- ApolloEngine$new(gemeente = "Ede", config_file = "test/conf/config.yml",
                         schema = "ede_ondermijning",  
                         geo_file = "test/geo_Ede.rds", # !!
                         pool = TRUE)





######################################################################
# ---------------  APOLLO SPECIFIC FUNCTIONS ----------------------- #
######################################################################
if(testconf[["apollo specific"]]){
  a <- .sys$read_signals() 
  b <- .sys$read_indicator() 
  c <- .sys$read_actions()  
}
################################################
# -------------- LOGGING --------------------- #
################################################  
if(testconf[["logging"]]){
  print('-- testing log functionality --') 
  print(.sys$get_log_ping())
}
###################################################
# -------------- FAVORITES ---------------------- #
###################################################
if(testconf[["favorites"]]){
  print('-- testing favorieten functionality --') 
  .sys$add_favorite(user_id='apollo_test', object_id='1', object_type='person')
  
  print( .sys$get_favorite('apollo_test', '1'))
  
  .sys$remove_favorite(user_id='apollo_test', favorite_id='3') 
}

#######################################################
# ---------------  ACTIELIJST ----------------------- #
#######################################################
if(testconf[["actionlist"]]){
  print('-- testing actielijst functionality --') 
  .sys$create_action(user_id='apollo_test', action_name= 'Olielek A', registration_id='test_1', description='Surveillance zonder resultaat', action_date='2022-05-31', status='In behandeling')
  
  .sys$update_action(action_id = 3, user_id='apollo_test', action_name= 'Olielek A', registration_id='test_1', description='Surveillance met resultaat', action_date='2022-06-01', status='Afgehandeld')

  .sys$archive_action(action_id = 3, user_id='apollo_test')
}

######################################################
# -------------- LIST FUNCTIONS -------------------- #
######################################################
if(testconf[["list functions"]]){
  print('-- testing list functionality --') 
  d <- .sys$list_actions() 
  e <- .sys$list_favorites(user_id='apollo_test')
}

#######################################################
# ---------------  DETAILPAGINA --------------------- #
#######################################################
if(testconf[["details"]]){
  print('-- testing detail functionality --') 
  person_id <- 'zvOK8AauG'
  pi <- .sys$get_person_from_id(person_id)
  print(pi)
  aa<-  .sys$get_address_from_id(pi$address_id)
  print(aa)
}

###################################################
# -------------- PRIVACY PROTOCOL  -------------- #
###################################################
if(testconf[["MPP"]]){
  print('-- testing privacy functionality --')  
  
  registration_id <- '98c28e39-4419-41f0-8b64-477dbfc2e68f'
  user_id <- "apollo_test"
  x <- .sys$read_mpp(registration_id)
  print(x)
  
  MPP_example <-  data.frame (mpp_name = c("weging1", "weging2a", "weging2b", "tab1conclusie"), 
                user_id =   rep("apollo_test", 4),
                bool_val = c(TRUE, FALSE, TRUE, FALSE),
                text_val = c("Hier volgt een toelichting", "text", "", "Er is voldoende reden"),
                checklist = c("chck_stap1_gr1=NULL;chck_stap1_gr2=NULL;chck_stap1_gr3=NULL;chck_stap1_gr4=NULL;chck_stap1_gr5=NULL;chck_stap1_gr6=NULL;chck_stap2_gr1=NULL;chck_stap2_gr2=NULL;chck_stap3_gr1=NULL;txt_stap3=NULL
", "", "", ""))
  
  .sys$create_MPP_for_registration(registration_id, user_id, data=MPP_example)
  #.sys$archive_MPP_for_registration(registration_id, user_id ) 
  x <- .sys$read_mpp(registration_id)
  print(x)
  #.sys$updateMPP(registration_id, user_id, data=MPP_example)
  
  .sys$update_MPP_for_registration(registration_id, MPP_example, user_id)
  #.sys$archive_MPP_for_registration(registration_id='1', user_id='1', mpp_names=c("A", "B", "C"), createLog=FALSE)
  
}

################################################### 
# ---------------  TIMELINE --------------------- #
################################################### 
if(testconf[["timeline"]]){
  print('-- testing timeline --') 
  person_id <- "6fmWQbsVF"#"JUGSZ1nWK"#'U8Cx3IcEd'
  pi <- .sys$get_relocations_timeline(person_id) 
  print(pi) 
}


################################################### 
# ---------------  NETWORK ---------------------  #
################################################### 
if(testconf[["network"]]){
  print('-- testing network --') 
  #person_id <- "6fmWQbsVF" 
  person_id <- "U8Cx3IcEd" 
  
  person_data <- .sys$get_person_from_id(person_id)
  address_data <- .sys$get_address_from_id(person_data$address_id)
  resident_data <- .sys$get_residents(address_data$address_id)
  business_data <- .sys$get_businesses_at_address(address_data$address_id)
  
  print('person netw')
  print(.sys$create_network_nodes(person_data,address_data,resident_data,business_data))
  print(.sys$create_network_edges(person_data,address_data,resident_data,business_data))
  
  print('address netw')
  print(.sys$create_network_nodes(NULL,address_data,resident_data,business_data))
  print(.sys$create_network_edges(NULL,address_data,resident_data,business_data))
 
  
}
######################################################
# -------------- INDICATOR FUNCTIONS ----------------#
######################################################
if(testconf[["indicators"]]){
  
  # Add indicator to metadata table
  .sys$add_indicator(indicator_name = "test_indicator", 
                     type = "person", 
                     label = "Test 1",
                     description = "Dit is een test indicator, negeer", 
                     user_id = "apollo_test", 
                     theme = c("mensenhandel","milieu"), 
                     columns = "")#, 
                     #weight = 1, 
                     #threshold = 2)
  
  .sys$remove_indicator("test_indicator")
  
  
  
  # Convert a raw indicator data column to a vector of TRUE/FALSE
  indic <- .sys$get_indicators_theme("drugs")
  
  
  .sys$make_boolean_indicator(indic, "actief_wmo") %>%
    table
  
  # Make a table of TRUE/FALSE indicator values for a theme of a type
  dat_ad <- .sys$make_indicator_table("mensenhandel", type = "address")
  dat_pers <- .sys$make_indicator_table("mensenhandel", type = "person", id_columns = "address_id")
  
  # Combine indicator tables
  system.time(
    dat_all <- .sys$combine_indicator_tables(dat_ad, dat_pers)  
  )

}

#######################################################
# ---------------  GEO FUNCTIONALITY ---------------- #
#######################################################

if(testconf[["geo"]]){
  # buurt codes omzetten
  .sys$geo_name_from_code(dat_ad$buurt_code_cbs[1:10])
  
  # alle geo kolommen toevoegen aan een dataframe met buurt_code_cbs
  microbenchmark(
    dat_ad_2 = dat_ad %>% .sys$add_geo_columns(.)
  )
}


############################################# 
# -------------- risk model ----------------#
#############################################

if(testconf[["riskmodel"]]){
  # set a weight
  .sys$set_indicator_weight("vroegtijdig_schoolverlater", theme = "mensenhandel", weight = 2.1)
  .sys$read_indicators()
  
  
  # Add 'risk_model' column to the indicator table
  dat_all <- .sys$calculate_riskmodel(dat_all, "mensenhandel")
  
  
  
  risk <- .sys$get_indicators_riskmodel("remko", "mensenhandel", "Ede")
  
  # .sys$write_indicator_riskmodel("Remko", "vroegtijdig_schoolverlater", "drugs", 1,1)
  # .sys$write_indicator_riskmodel("Remko", "vroegtijdig_schoolverlater", "drugs", 2,1)
  # .sys$execute_query("delete from indicator_riskmodel where risk_id = 17")
  
  
  
  
}

############################################# 
# -------------- metadata ----------------#
#############################################

if(testconf[["metadata"]]){
  .sys$set_metadata(name= "data_bag", label="BAG", 
                    timestamp_provided = Sys.time(),
                    owner="shintoLabs", 
                    depends_on=c(), 
                    step=1, 
                    colnames=c("straatnaam", "huisnummer", "huisletter", "huisnummertoevoeging", "postcode"), 
                    description="Basisregistratie personen en adressen geleverd door NLExtract")
  .sys$set_metadata(name= "brp", label="BRP", 
                    timestamp_provided = Sys.time(),
                    owner="Ede", 
                    depends_on=c(), 
                    step=1, 
                    colnames=c("bsn", "anr", "anr_ouder1", "anr_ouder2"), 
                    description="Basisregistratie Personen geleverd door gemeente Ede")
  .sys$set_metadata(name= "nhr", label="NHR", 
                    timestamp_provided = Sys.time(),
                    owner="Ede", 
                    depends_on=c(), 
                    step=1, 
                    colnames=c("kvknummer", "handelsnaam", "hoofdactiviteit", "nevenactiviteit"), 
                    description="Nationaal Handelsregister geleverd door PinkRoccade")
  .sys$set_metadata(name= "huishoudens_huidig", label="Huidige huishoudens", 
                    timestamp_provided = Sys.time(),
                    owner="shintoLabs", 
                    depends_on=c("brp", "bag"), 
                    step=2, 
                    colnames=c("bsn", "anr", "gezinsverhouding", "aantal_huwelijken"), 
                    description="Berekende staat van huidige huishoudens")
  .sys$set_metadata(name= "brp_verhuis_historie", label="Verhuis historie", 
                    timestamp_provided = Sys.time(),
                    owner="shintoLabs", 
                    depends_on=c("brp"), 
                    step=2, 
                    colnames=c("person_id", "datum_geboorte", "event", "event_datum"), 
                    description="Geschiedenis van verhuisbewegingen")
  .sys$set_metadata(name= "address", label="Adres", 
                    timestamp_provided = Sys.time(),
                    owner="shintoLabs", 
                    depends_on=c("brp_verhuis_historie"), 
                    step=3, 
                    colnames=c("address_id", "aantal_inwoners"), 
                    description="Gepseudonimiseerde adres tabel")
  .sys$set_metadata(name= "person", label="Personen", 
                    timestamp_provided = Sys.time(),
                    owner="shintoLabs", 
                    depends_on=c("huishoudens_huidig"), 
                    step=3, 
                    colnames=c("person_id", "address_id", "aantal_huwelijken", "leeftijdcategorie_5jr"), 
                    description="Gepseudonimiseerde persoon-indicator tabel")
  .sys$set_metadata(name= "business", label="Bedrijven", 
                    timestamp_provided = Sys.time(),
                    owner="shintoLabs", 
                    depends_on=c("nhr"), 
                    step=3, 
                    colnames=c("business_id", "hoofdactiviteit", "address_id" ), 
                    description="Gepseudonimiseerd NHR")
}
 

print('done')







