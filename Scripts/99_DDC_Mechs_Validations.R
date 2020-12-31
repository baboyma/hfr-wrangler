## 
## HFR Data - Errors Validation
## Date: 2020-12-21
## 

## LIBRARIES ----

  library(tidyverse)
  library(readxl)
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
  
  # Operatingunit
  #cntry <- "Dominican Republic"
  cntry <- "Rwanda"
  
  rwa_sites_file <- "HFR_FY21_SiteValidation_Rwanda_Revised_20201230.xlsx"
  
## DATA ----

  # Sites list
  
  hfr_sites <- list.files(
      path = si_path(type = "path_datim"),
      pattern = sites,
      full.names = TRUE
    ) %>% 
    vroom::vroom()
  
  hfr_sites %>% glimpse()
  
  # Check country sites
  hfr_sites %>% 
    filter(operatingunit == cntry) %>% 
    glimpse()
  
  
  rwa_sites_file %>% 
    file.path("Data", .) %>% 
    excel_sheets()
  
  sites_rwa <- rwa_sites_file %>% 
    file.path("Data", .) %>% 
    read_excel(sheet = 2)
  
  sites_rwa %>% glimpse()
  
  sites_rwa %>% 
    is_ou_valid(orgs) %>% 
    is_orgunituid_valid(orgs) %>% 
    is_orgunituid4ou(orgs) %>% 
    filter(valid_ou == FALSE | valid_uid == FALSE, uid_to_ou == FALSE) %>% 
    glimpse() %>% 
    View()
  
  sites_rwa %>% 
    is_ou_valid(df_cntry_orgs) %>% 
    is_orgunituid_valid(df_cntry_orgs) %>% 
    is_orgunituid4ou(df_cntry_orgs) %>% 
    filter(valid_ou == FALSE | valid_uid == FALSE, uid_to_ou == FALSE) %>% 
    glimpse() %>% 
    View()
  
  
  # OU Details
  ou <- glamr::identify_ouuids(username = datim_user(),
                        password = datim_pwd())
  
  ou %>% glimpse()
  
  ou_id <- ou %>% 
    filter(country == countryy) %>% 
    pull(uid)
  
  # Load lookups tables: ims, orgs, valid_dates
  load_lookups(datim_path = gs_id, local = F)
  
  # Data Mechanisms
  mechanisms <- pull_mech() 
  
  # Data Orgs
  df_cntry_orgs <- pull_hierarchy(ou_uid = ou_id,
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
  #mechs <- c(17549, 81928, 81936) # PNG IMs
  mechs <- c(16857, 16860, 81876)  # RWA IMs
  
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
    filter(operatingunit %in% cntry) %>% 
    distinct(operatingunit, mech_code) %>% 
    prinf()
  
  
  ## Orgunituids ----
  orguids <- c()
  orgunits <- c("Gatsata health center",
                "Legacy Clinic",
                "La Medicale (kwa Kanimba) clinic",
                "Rwampara health center",
                "Nyabinyenga health center",
                "Munyiginya health center")

  orgs %>% 
    filter(orgunituid %in% orguids) %>% 
    prinf()
  
  orgs %>% 
    filter(
      operatingunit == cntry,
      str_detect(orgunit, paste0(orgunits, collapse = "|"))) %>% 
    View()
    prinf()
    
  orgs %>% 
    filter(
      operatingunit == cntry,
      str_detect(str_to_lower(orgunit), 
                 paste0(str_to_lower(orgunits), 
                        collapse = "|"))) %>% 
    View()
    prinf()
    
  df_cntry_orgs %>% 
    filter(
      operatingunit == cntry,
      str_detect(str_to_lower(orgunit), 
                 paste0(str_to_lower(orgunits), 
                        collapse = "|"))) %>% 
    View()
    prinf()
  
  # Does not exist in local files
  hfr_orgunit_search(orgs, orgunit_name = orgunits[1])
  hfr_orgunit_search(orgs, orgunit_name = orgunits[2])
  
  orgunits %>% 
    map_dfc(.x, .f = ~ hfr_orgunit_search(df_cntry_orgs, orgunit_name = .x))
  
  # Exist in Datim
  df_cntry_orgs %>% 
    filter(orgunituid %in% orguids) %>% 
    prinf()
  
  df_cntry_orgs %>% 
    filter(orgunit %in% orgunits) %>% 
    prinf()
  
  df_orgs %>% 
    filter(orgunituid %in% orguids) %>% 
    prinf()
  