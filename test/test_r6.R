

# Test het system warehouse object.

library(microbenchmark)
library(tinytest)
library(here)
library(DBI)
library(glue)
library(futile.logger)

options(config_file = glue("c:/repos/apollo-ondermijning/conf/config.yml")) 
#source(file.path(here(), "preload/load_packages.R")) 

#library(apollor)
devtools::load_all()

.sys <- ApolloEngine$new(gemeente = "Ede", 
                         schema = "ede_ondermijning",  
                         pool = TRUE)


#expect_true(
print('-- testing generic apollo functions --')
a <- .sys$read_signals() 
b <- .sys$read_indicators() 
c <- .sys$read_actions()  



print('-- testing log functionality --') 
print(.sys$get_log_ping())

print('-- testing favorieten functionality --') 
.sys$add_favourite(username='apollo_test', oid=1, object_type='persoon')
.sys$remove_favourite(username='apollo_test', favo=3)

print('-- testing actielijst functionality --') 
.sys$create_action(username='apollo_test', actie_naam= 'Olielek A', registratie_id=1, omschrijving='Surveillance zonder resultaat', datum_actie='2022-05-31', status='In behandeling')

.sys$update_action(action_id = 3, username='apollo_test', actie_naam= 'Olielek A', registratie_id=1, omschrijving='Surveillance met resultaat', datum_actie='2022-06-01', status='Afgehandeld')

.sys$archive_action(action_id = 3, username='apollo_test')
 
print('-- testing list functionality --') 
d <- .sys$list_actions() 
e <- .sys$list_favourites()


print('-- testing detail functionality --') 

bsn <- 'dTyI6jJgE'
pi <- .sys$get_persoon_from_bsn(bsn)
print(pi)
aa<-  .sys$get_adress_from_id(pi$adres_id)
print(aa)




print("-- testing calculating indicatoren --")


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

print('done')





