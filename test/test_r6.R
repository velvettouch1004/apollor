

# Test het system warehouse object.

library(microbenchmark)
library(tinytest)
library(here)
library(DBI)
library(glue)
library(futile.logger)

library(dplyr)

library(data.table)

#options(config_file = glue("conf/config.yml"))
options(config_file = glue("c:/repos/apollo-ondermijning/conf/config.yml"))

#library(apollor)
devtools::load_all()

.sys <- ApolloEngine$new(gemeente = "Ede", 
                         schema = "ede_ondermijning",  
                         geo_file = "test/geo_Ede.rds", # !!
                         pool = TRUE)


#expect_true(
print('-- testing generic apollo functions --')
a <- .sys$read_signals() 
b <- .sys$read_indicators() 
c <- .sys$read_actions()  



print('-- testing log functionality --') 
print(.sys$get_log_ping())

print('-- testing favorieten functionality --') 
.sys$add_favorite(username='apollo_test', object_id='1', object_type='person')
.sys$remove_favorite(username='apollo_test', favorite_id='3')

print('-- testing actielijst functionality --') 
.sys$create_action(username='apollo_test', action_name= 'Olielek A', registration_id='test_1', description='Surveillance zonder resultaat', action_date='2022-05-31', status='In behandeling')

.sys$update_action(action_id = 3, username='apollo_test', action_name= 'Olielek A', registration_id='test_1', description='Surveillance met resultaat', action_date='2022-06-01', status='Afgehandeld')

.sys$archive_action(action_id = 3, username='apollo_test')
 
print('-- testing list functionality --') 
d <- .sys$list_actions() 
e <- .sys$list_favorites()


print('-- testing detail functionality --') 

bsn <- 'dTyI6jJgE'
pi <- .sys$get_person_from_id(bsn)
print(pi)
aa<-  .sys$get_adress_from_id(pi$adres_id)
print(aa)




print("-- testing calculating indicatoren --")

# Add indicator to metadata table
.sys$add_indicator(indicator_name = "test_indicator", 
                   type = "person", 
                   label = "Test 1",
                   description = "Dit is een test indicator, negeer", 
                   creator = "apollo_test", 
                   theme = c("mensenhandel","milieu"), 
                   columns = "", 
                   weight = 1, 
                   threshold = 2)

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



# buurt codes omzetten
.sys$geo_name_from_code(dat_ad$buurt_code_cbs[1:10])

# alle geo kolommen toevoegen aan een dataframe met buurt_code_cbs
microbenchmark(
  dat_ad_2 = dat_ad %>% .sys$add_geo_columns(.)
)


# risk model
head(dat1)


print('done')





