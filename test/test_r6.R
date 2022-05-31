

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
  
a <- .sys$read_signals() 
b <- .sys$read_indicators() 
c <- .sys$read_actions() 
d <- .sys$get_actions() 
print(d)
print('done')