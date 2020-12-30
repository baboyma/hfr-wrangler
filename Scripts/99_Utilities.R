#' Set service value
#' @param service 
#' @param user
#' @param pwd
#' 
set_service <- function(service, user, pwd = NULL, keyr = NULL){
  
  if (is.null({{pwd}})) {
    keyring::key_set(service = {{service}},
                     username = {{user}},
                     keyring = {{keyr}})
  }
  else {
    keyring::key_set_with_value(service = {{service}},
                                username = {{user}},
                                password = {{pwd}},
                                keyring = {{keyr}})
  }
}


#' Check service availability
#' @param service
#' 
check_service <- function(service) {
  {{service}} %in% keyring::key_list()$service
}

#' Service error message
#' @service
#' 
service_error <- function(service) {
  usethis::ui_oops({{service}})
  usethis::ui_stop("Service not available.")
}

#' Get account username
#' @param service
#' 
get_user <- function(service) {
  
  svc <- {{service}}
  
  if (!check_service(svc)) {
    usethis::ui_oops(sprintf("Service named '%s' is not available", svc))
    return(NULL)
  }
  
  # Return row=1, col=2
  keyring::key_list(svc)[1,2]
}


#' Get account usernames
#' @param service
#' 
get_users <- function(service) {
  
  svc <- {{service}}
  
  if (!check_service(svc)) {
    usethis::ui_oops(sprintf("Service named '%s' is not available", svc))
    return(NULL)
  }
  
  # Return row=all, col=2
  keyring::key_list(svc)[,2]
}

#' Get account password
#' @param service 
#' 
get_key <- function(service, user = NULL) {
  
  # Service name
  svc <- {{service}}
  
  if (!check_service(svc)) {
    usethis::ui_oops(sprintf("Service named '%s' is not available", svc))
    return(NULL)
  }
  
  # Service username
  if (is.null({{user}})) {
    user <- get_user(svc)
  }
  
  # Service password
  key <- keyring::key_get(service = svc, 
                          username = user)
  
  # value
  return(key)
}




