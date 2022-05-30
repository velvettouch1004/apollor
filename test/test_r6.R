

# Test het system warehouse object.

library(microbenchmark)
library(tinytest)
library(here)

options(config_file = glue("{here()}/conf/config.yml")) 
source(file.path(here(), "preload/load_packages.R")) 

library(apollor)

.sys <- LinkItEngine$new(gemeente = "Ede", 
                         schema = "linkit", 
                         config_file = getOption("config_file"),
                         pool = TRUE)
