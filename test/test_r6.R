

# Test het system warehouse object.

library(microbenchmark)
library(tinytest)
library(here)
library(DBI)
library(glue)
library(futile.logger)

options(config_file = glue("../conf/config.yml")) 
#source(file.path(here(), "preload/load_packages.R")) 

library(apollor)

.sys <- ApolloEngine$new(gemeente = "ede", 
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

print('done')

