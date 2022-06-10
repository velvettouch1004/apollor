#' Read the application version
#' @export
read_version <- function(){
  fn <- "VERSION"
  if(!file.exists(fn)){
    ""
  } else {
    readLines(fn)[1]
  }
  
}

