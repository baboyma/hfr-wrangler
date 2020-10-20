## PROJECT:  HFR Data Quality Assessment
## AUTHOR:   B.Kagniniwa | USAID
## LICENSE:  MIT
## PURPOSE:  Country Specific SQL Views
## Date:     2020-08-03

# LIBRARIES

library(tidyverse)
library(vroom)
library(Wavelength)
library(glamr)
library(janitor)
library(quickview)

# REQUIRED

source("./Scripts/00_Config.R")

# GLOBALS VARS

    user <- "bkagniniwa"
    key <- "datim_myuser"

    valid_age <- c("<15", "15+")
    valid_sex <- c("Female", "Male")

    # country_name = "Democratic Republic of the Congo"
    # country_iso3 = "COD"

    # country_name = "Nigeria"
    # country_iso3 = "NGA"

    country_name = "Ethiopia"
    country_iso3 = "ETH"

    country = c(country_iso3, country_name)

    curr_pd <- 12
    curr_pd <- str_pad(curr_pd, 2, side = "left", pad = "0")

    dir_curr_pd <- paste0("../../HFR-Data/2020.", curr_pd)

    dir_datim <- "../../HFR-Data/Datim"
    dir_mer_targets <- "../../HFR-Data/Datim/FY20Q3"
    dir_sqlviews <- "../../HFR-Data/SQL-Views"

    #file_hfr_curr <- "HFR_2020.10_Tableau_20200728.csv"
    #file_hfr_curr <- "HFR_2020.11_Tableau_20200911.csv"
    #file_hfr_curr <- "HFR_2020.11_Tableau_20200916.csv"

    #file_hfr_curr <- "HFR_2020.12_Tableau_20200922.csv" # HI
    #file_hfr_curr <- "HFR_2020.12_Tableau_20200923.csv" # DDC
    file_hfr_curr <- "HFR_2020.12_Tableau_20200924.csv" # HI


    # FUNCTIONS --------------------------------------------------------------

    source("./Scripts/99_HFR_Utilities.R")

# DATA -------------------------------------------------------------------


    ## Look up tables: orgs, ims, levels
    Wavelength::load_lookups(datim_path = dir_datim)

    orgs %>% glimpse()

    ims %>% glimpse()

    ims <- ims %>%
        select(mech_code, mech_name, primepartner)

    levels <- Wavelength::identify_levels(username = user, 
                                          password = mypwd(key))

    levels %>% glimpse()

    ## Targets data
    hfr_targets <- get_mer_targets(
            dir_targets = dir_mer_targets,
            country = country
        ) %>%
        select(-community) %>%
        relocate(orgunit, indicator, sex, agecoarse, .before = mer_results) %>%
        arrange(mech_code, orgunit, indicator, sex, agecoarse)

    hfr_targets %>% glimpse()


    ## Process data
    hfr_processed <- get_hfr_processed(
            dir_proc = dir_curr_pd,
            country = country,
            period = curr_pd
        ) %>%
        arrange(mech_code, orgunit, indicator, sex, agecoarse)

    hfr_processed %>% glimpse()


    ## Transform data
    # hfr_aug <- augmente_hfr_data(
    #     df_hfr = hfr_processed,
    #     df_trgts = hfr_targets,
    #     df_orgs = orgs,
    #     df_mechs = ims
    # )
    #
    # hfr_aug %>% glimpse()

    hfr_trans <- transform_hfr_processed(
        df_hfr = hfr_processed,
        df_trgts = hfr_targets,
        df_orgs = orgs,
        df_mechs = ims
    )

    hfr_trans %>% glimpse()

    View(hfr_trans)

    hfr_trans %>%
        filter(is.na(fundingagency)) %>%
        distinct(operatingunit, orgunituid) %>%
        nrow()

    hfr_trans %>%
        filter(is.na(date)) %>%
        distinct(operatingunit, mech_code, snu1, psnu, orgunituid) %>%
        prinf()


    ## HFR SQL Data
    hfr_sqlview <- get_hfr_sqlview(
            dir_path = dir_sqlviews,
            period = curr_pd
            ,pattern = file_hfr_curr
        )

    hfr_sqlview %>% glimpse()

    hfr_sqlview %>%
        distinct(hfr_pd) %>%
        arrange() %>%
        pull() %>%
        as.integer()


    ## Country / Period data
    cntry_sqlview <- hfr_sqlview %>%
        filter(
            operatingunit == country_name,
            hfr_pd == curr_pd
        ) %>%
        relocate(mech_code, orgunit, indicator, sex, agecoarse, otherdisaggregate,
                 val, mer_results, mer_targets, .after = primepartner) %>%
        arrange(mech_code, orgunit, indicator, sex, agecoarse)

    cntry_sqlview %>% glimpse()


    ## Confirm processed data
    cntry_validation <- validate_hfr_sqlview(
            df_hfr = hfr_processed,
            df_sqlview = cntry_sqlview
        ) %>% View()


    cntry_validation <- validate_hfr_sqlview(
        df_hfr = hfr_processed,
        df_sqlview = hfr_aug
    ) %>% View()


    ## Confirm all process data
    df_checked <- validate_hfr_files(
        dir_proc = dir_curr_pd,
        df_orglevels = levels,
        df_sqlview = hfr_sqlview
    )

    df_checked %>% glimpse()

    View(df_checked)

    ## Exports
    df_checked %>%
        write_csv(path = paste0("./Dataout/PD", curr_pd, "_Processed_vs_SQLView_Errors_", format(Sys.Date(), "%Y%m%d"), ".csv"), na = "")
