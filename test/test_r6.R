

# Test het system warehouse object.
testconf <- list(
  "apollo specific" = F,
  "logging" = F,
  "favorites" = F,
  "actionlist" = F,
  "list functions" = F,
  "details" = T,
  "indicators" = F
)




library(microbenchmark)
library(tinytest)
library(here)
library(DBI)
library(glue)
library(futile.logger)
library(dplyr)

options(config_file = glue("conf/config.yml"))
#options(config_file = glue("c:/repos/apollo-ondermijning/conf/config.yml"))

#library(apollor)
devtools::load_all()

.sys <- ApolloEngine$new(gemeente = "Ede", 
                         schema = "ede_ondermijning",  
                         geo_file = "test/geo_Ede.rds", # !!
                         pool = TRUE)

cat("######################################################################\
# ---------------  APOLLO SPECIFIC FUNCTIONS ----------------------- #\
######################################################################")
if(testconf[["apollo specific"]]){
  a <- .sys$read_signals() 
  b <- .sys$read_indicators() 
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
  e <- .sys$list_favorites()
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



######################################################
# -------------- INDICATOR FUNCTIONS ----------------#
######################################################
if(testconf[["indicators"]]){
  print("-- testing calculating indicatoren --")

  # Add indicator to metadata table
  .sys$add_indicator(indicator_name = "test_indicator", 
                   type = "person", 
                   label = "Test 1",
                   description = "Dit is een test indicator, negeer", 
                   user_id = "apollo_test", 
                   theme = c("mensenhandel","milieu"), 
                   columns = "", 
                   weight = 1, 
                   threshold = 2)

  .sys$remove_indicator("test_indicator")



  # Convert a raw indicator data column to a vector of TRUE/FALSE
  indic <- .sys$get_indicators_theme("drugs")

  #make_boolean_indicator(indic, "actief_wmo") %>%
  #  table


  # Make a table of TRUE/FALSE indicator values for a theme of a type
  dat1 <- .sys$make_indicator_table("mensenhandel", type = "address")
  dat2 <- .sys$make_indicator_table("mensenhandel", type = "person")

  # buurt codes omzetten
  .sys$geo_name_from_code(dat1$buurt_code_cbs[1:10])

  # alle geo kolommen toevoegen aan een dataframe met buurt_code_cbs
  microbenchmark(
    dat1_2 = dat1 %>% .sys$add_geo_columns(.)
  )
}




print('-- Done --')





