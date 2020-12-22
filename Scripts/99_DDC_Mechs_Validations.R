## 
## HFR Data - Errors Validation
## Date: 2020-12-21
## 

## LIBRARIES ----

  library(tidyverse)
  library(Wavelength)
  library(glamr)

## GLOBALS ----
  
  # Creds
  load_secrets()

  # paths
  glamr::si_path(type = "path_datim")

  # Data
  gs_id <- NULL
  
  # Datim URL
  baseurl <- "https://final.datim.org/"
  
  # Site List
  sites <- "HFR_FY21_GLOBAL_sitelist_20201119.csv"
  
  
## DATA ----

  # Site list
  hfr_sites <- list.files(
      path = si_path(type = "path_datim"),
      pattern = sites,
      full.names = TRUE
    ) %>% 
    vroom::vroom()
  
  hfr_sites %>% glimpse()
  
  # OU Details
  ou <- identify_ouuids(username = datim_user(),
                        password = datim_pwd())
  
  ou %>% glimpse()
  
  ou_id <- ou %>% 
    filter(country == "Dominican Republic") %>% 
    pull(uid)
  
  # Load lookups tables: ims, orgs, valid_dates
  load_lookups(datim_path = gs_id, local = F)
  
  # Data Mechanisms
  mechanisms <- pull_mech() 
  
  # Data Orgs
  df_cntry_orgs1 <- pull_hierarchy(ou_uid = ou_id,
                                 username = datim_user(),
                                 password = datim_pwd())

  
  
  df_orgs <- ou %>% 
    filter(type == "OU") %>% 
    pull(uid) %>% 
    map_dfr(.x, .f = ~ pull_hierarchy(ou_uid = .x,
                                      username = datim_user(),
                                      password = datim_pwd()))
  
  df_orgs %>% glimpse()
  orgs %>% glimpse()
  
  #hfr_export(df_orgs, si_path(type = "path_datim"), type = "orghierarchy")
  
## Validations ----

  # QUERIES:
  # select * from cntry_ou_mechanisms
  # where mech_code in (17549, 81936);
  
  ## Mech codes ----
  mechs <- c(17549, 81928, 81936)
  
  # Mechs from local file
  ims %>% 
    filter(mech_code %in% mechs) %>% 
    prinf()
  
  # Mechs from datim
  mechanisms %>% 
    filter(mech_code %in% mechs) %>% 
    pull(operatingunit)
  
  # Mechs from sitelist
  hfr_sites %>% 
    filter(mech_code %in% mechs) %>% 
    prinf()
  
  hfr_sites %>% 
    filter(operatingunit %in% c("Eswatini", "Lesotho")) %>% View()
    distinct(operatingunit, mech_code) %>% 
    prinf()
  
  
  ## Orgunituids ----
  orguids <- c("x3pmbo3X2Ln", "ltAqcBVf0mC")
  orgunits <- c("Aid For Aids", "Hospital Alejo Martinez")

  orgs %>% 
    filter(orgunituid %in% orguids) %>% 
    prinf()
  
  orgs %>% 
    filter(str_detect(orgunit, paste0(orgunits, collapse = "|"))) %>% 
    prinf()
  
  # Does not exist in local files
  hfr_orgunit_search(orgs, orgunit_name = orgunits[1])
  hfr_orgunit_search(orgs, orgunit_name = orgunits[2])
  
  # Exist in Datim
  df_cntry_orgs1 %>% 
    filter(orgunituid %in% orguids) %>% 
    prinf()
  
  df_orgs %>% 
    filter(orgunituid %in% orguids) %>% 
    prinf()
  