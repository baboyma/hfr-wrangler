## PROJECT:  DDC/HFR Data Processing
## AUTHOR:   B.Kagniniwa | USAID
## LICENSE:  MIT
## PURPOSE:  Validate Trifacta Outputs
## Date:     2020-01-06

# LIBRARIES ------

library(tidyverse)
library(glamr)
library(janitor)
library(lubridate)
library(aws.s3)

library(glitr)
library(extrafont)

# QUERIES ----

  # DSE Tables
  dse_tbls <- c("cntry_agg_hfr_prcssd_sbmsn",
                "cntry_agg_hfr_sbmsn",          # 
                "cntry_hfr_err_sbmsn",
                "cntry_hfr_prcssd_sbmsn",       # Processed files + MER Data
                "cntry_hfr_sbmsn",              # Raw submissions files
                "cntry_hfr_sbmsn_status",
                "cntry_ou_hierarchy",           # Orgunits
                "cntry_ou_mechanisms",          # Mechs
                "cntry_ou_trgts",               # MER Targets
                "cntry_sbmsn_evnts",            # Tracks the submissions of HFR data from ingestion through processing
                "cntry_vldtn_chk_evnts",
                "ddc_prcssd_sbmsn")
  
  # Test Queries
  q_sbm_status <- "Select * from cntry_hfr_sbmsn_status;"
  q_sbm_data <- "select * from cntry_hfr_prcssd_sbmsn limit 10;"
  
# DATA ----

  # Global Reference
  df_refs <- glamr::s3_objects(
      bucket = 'gov-usaid',
      prefix = "ddc/uat/processed/hfr/receiving/HFR_FY21_GLOBAL"
    ) %>% 
    glamr::s3_unpack_keys(df_objects = .) 
  
  # Site targets
  df_targets <- df_refs %>% 
    filter(str_detect(key, "DATIM_targets")) %>% 
    arrange(desc(last_modified)) %>% 
    pull(key) %>% 
    first() %>% 
    glamr::s3_read_object(
      bucket = 'gov-usaid',
      object = .
    )
  
  df_targets %>% glimpse()
  
  df_targets %>% 
    clean_agency() %>% 
    count(operatingunit) %>% 
    arrange(desc(n)) %>% 
    prinf()
  
  # Orgs
  df_orgs <- df_refs %>% 
    filter(str_detect(key, "orghierarchy")) %>% 
    arrange(desc(last_modified)) %>% 
    pull(key) %>% 
    first() %>% 
    glamr::s3_read_object(
      bucket = 'gov-usaid',
      object = .
    )
  
  df_orgs %>% glimpse()
  
  df_orgs <- df_orgs %>% 
    mutate_at(vars(ends_with("tude")), as.numeric)
  
  # Site list
  df_sites <- df_refs %>% 
    filter(str_detect(key, "sitelist")) %>% 
    arrange(desc(last_modified)) %>% 
    pull(key) %>% 
    first() %>% 
    glamr::s3_read_object(
      bucket = 'gov-usaid',
      object = .
    )
  
  df_sites %>% glimpse()
  
  
  df_sites <- df_sites %>% 
    mutate_at(
      vars(ends_with(c("reporting", "original"))), 
      funs(as.logical)
    ) %>% 
    filter(expect_reporting == TRUE) %>% 
    select(-c(last_col(), last_col(1))) %>% 
    separate(operatingunit, 
             into = c("operatingunit", "countryname"), 
             sep = "/") %>% 
    mutate(countryname = if_else(
      is.na(countryname), 
      operatingunit, 
      countryname))
  
  df_sites %>% 
    distinct(operatingunit, countryname) %>% 
    pull(operatingunit)

  df_sites %>% 
    filter(operatingunit == 'Namibia') %>% 
    head()
  
  
  df_sites <- df_sites %>% 
    left_join(df_orgs %>% 
                select(orgunituid, longitude, latitude) %>% 
                filter(!is.na(longitude) & !is.na(longitude)),
              by = "orgunituid")
  
  df_sites %>% glimpse()
  
  
  # HFR Processed
  df_procs <- glamr::s3_objects(
      bucket = 'gov-usaid',
      prefix = "ddc/uat/processed/hfr/incoming/HFR_FY21"
    ) %>% 
    glamr::s3_unpack_keys(df_objects = .) 
  
  
  df_procs %>% 
    filter(str_detect(key, ".*.xlsx$")) %>% 
    View()
  
  df_procs %>% 
    filter(str_detect(key, ".*.csv$")) %>% 
    View()


  # Tableau outputs
  df_outputs <- s3_objects(
      bucket = 'gov-usaid',
      prefix = "ddc/uat/processed/hfr/outgoing/hfr"
    ) %>% 
    s3_unpack_keys() 
  
  # Tableau outputs - latest files
  df_hfr <- df_outputs %>%  
    filter(str_detect(sys_data_object, "^hfr_2021.*.csv$")) %>% 
    mutate(hfr_pd = str_extract(sys_data_object, "\\d{4}_\\d{2}")) %>% 
    group_by(hfr_pd) %>% 
    arrange(desc(last_modified)) %>% 
    slice(1) %>% 
    ungroup() %>% 
    pull(key) %>%
    map_dfr(.x, .f = ~ s3_read_object(
      bucket = 'gov-usaid',
      object = .x
    )) 

  df_hfr %>% glimpse()
  
  df_hfr %>% distinct(hfr_freq)
  
  df_hfr %>% 
    distinct(operatingunit, hfr_pd, indicator, hfr_freq) %>% 
    view()
  
  cntry <- "Zambia"
  
  df_hfr %>% 
    filter(operatingunit == cntry) %>% 
    distinct(hfr_pd, indicator, hfr_freq) %>% 
    arrange(hfr_pd, indicator) %>% 
    prinf()

  df_hfr %>% 
    filter(operatingunit == cntry,
           is.na(hfr_freq),
           expect_reporting == T) %>% 
    count(hfr_pd, indicator, hfr_freq, wt = mer_targets) %>% 
    spread(hfr_pd, n) %>% 
    View(title = "sum")
  
  df_hfr_cntry <- df_hfr %>% 
    filter(hfr_pd == "02", expect_reporting == "TRUE") %>% 
    select(operatingunit, mech_code, mech_name, indicator, orgunituid, val) %>%
    mutate(mech_name = if_else(str_detect(mech_name, "\\("), 
                               glamr::extract_text(mech_name),
                               mech_name)) %>% 
    full_join(df_sites %>% 
                filter(!is.na(longitude) & !is.na(longitude)) %>%
                distinct(orgunituid, longitude, latitude), 
              by = "orgunituid") %>% 
    filter(operatingunit == cntry,
           !is.na(longitude) & !is.na(longitude)) %>% 
    mutate(val = as.integer(val),
           completeness = if_else(is.na(val), FALSE, TRUE)) 
  
  
  df_hfr_cntry <- df_hfr_cntry %>% 
    add_count(operatingunit, mech_name, indicator, wt = completeness) %>% 
    group_by(operatingunit, mech_name, indicator) %>% 
    mutate(mech_ind_completeness = round(n / n() * 100)) 
  
  df_hfr_cntry %>% 
    distinct(mech_ind_completeness)
  
  df_hfr_cntry <- df_hfr_cntry %>% 
    mutate(indicator = factor(indicator, labels = ))
  
  comp_labeller <- function() {
    
  }
  
  # Completeness
  ggplot() +
    geom_sf(data = gisr::get_admin0(cntry), fill = NA) +
    geom_point(data = df_hfr_cntry,
               aes(longitude, latitude, fill = completeness),
               shape = 21, size = 2, color = 'white', alpha = .8) +
    scale_fill_si("burnt_sienna") +
    facet_grid(mech_name ~ indicator) +
    labs(title = "ZAMBIA - HFR Sites Reporting FY2021.02 Data") +
    gisr::si_style_map() +
    theme(strip.text.y = element_text(angle = 90))
    
