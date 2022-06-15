
format_event_func <- function(event, buurt, gemeente){ 
  case_when(event %in% c("verhuisd_naar", "verhuisd_binnen") & !is.na(buurt) ~ glue("Verhuisd naar buurt {buurt}"),
            event %in% c("verhuisd_naar", "verhuisd_binnen", "verhuisd_uit") & !is.na(gemeente)~ glue("Verhuisd naar gemeente {gemeente}"),  
            event %in% c("verhuisd_naar", "verhuisd_binnen", "verhuisd_uit")  ~ "Verhuisd",
            !is.na(event) ~ event, 
            !is.na(gemeente) ~ glue("In de gemeente {gemeente}"), 
            TRUE ~ 'Onbekend' ) 
  
}
 
add_net_nodes <- function(., data, label, title, group){
  if(!is.null(data) && nrow(data) > 0){
    new_nodes <- data.frame(label = data[[label]],  
                            group = group,              
                            title = data[[title]])      
    
    . <- rbind(., new_nodes)
  }
  return(.)
} 

add_net_edges <- function(., data, label, title){ 
  if(!is.null(data) && nrow(data) > 0){
    new_edges <- data.frame(label = rep(label, nrow(data)), title = rep(title, nrow(data)))      
    
    . <- rbind(., new_edges)
  }
  return(.)
}