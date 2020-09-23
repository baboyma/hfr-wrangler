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
    file_hfr_curr <- "HFR_2020.12_Tableau_20200923.csv" # DDC



# FUNCTIONS --------------------------------------------------------------

    #' Get SQL View
    #'
    #' @param dir_path
    #' @param period
    #' @return SQLView Data
    #'
    get_hfr_sqlview <- function(dir_path, period = 1) {

        dir <- {{dir_path}}
        hfr_pd <- {{period}}

        hfr_pd <- str_pad(hfr_pd, 2, side = "left", pad = "0")

        hfr_fy <- Wavelength::curr_fy()

        # File name pattern
        file_pattern <- paste0("^HFR_", hfr_fy, ".", hfr_pd, "_Tableau_\\d{8}.csv$")

        # File names(s)
        file_names <- list.files(
                path = dir,
                pattern = file_pattern,
                full.names = TRUE
            ) %>%
            sort() %>%
            last()


        print(file_names)

        # File Content
        df <- file_names %>%
            sort() %>%
            last() %>%
            vroom()

        return(df)
    }


    #' Get MER Targets
    #' @param dir_targets path to target files
    #' @param country OU/Country ISO3 code
    #' @param mech_code Mechanism Code
    #' @return MER Results/Targets
    #'
    get_mer_targets <- function(dir_targets,
                                country = NULL,
                                mech_code = NULL) {

        # Params
        dir <- {{dir_targets}}
        cntry <- {{country}}
        mech <- {{mech_code}}

        # File name pattern
        file_pattern <- "^HFR_FY20Q3_"

        # Country ISO3 CODE
        if ( !is.null(cntry) ) {
            file_pattern <- paste0(file_pattern, toupper(cntry), "_")
        }
        else {
            file_pattern <- paste0(file_pattern, "[A-Z]{3}_")
        }

        # Mech Code
        if ( !is.null(mech) ) {
            file_pattern <- paste0(file_pattern, mech)
        }
        else {
            file_pattern <- paste0(file_pattern, "\\d{1+}")
        }

        file_pattern <- paste0(file_pattern, "_DATIM_\\d{8}.csv$")

        # File names(s)
        file_names <- list.files(
            path = dir,
            pattern = file_pattern,
            full.names = TRUE
        )

        # File Content
        df <- file_names %>%
            map_dfr(vroom)

        return(df)
    }


    #' Read data from HFR Processed files
    #'
    #' @param dir_proc path to processed files
    #' @param country OU/Country ISO3 & Name pair
    #' @param mech_code Mechanism Code
    #' @param period HFR Reporting Period
    #' @return MER Results/Targets
    #'
    get_hfr_processed <- function(dir_proc, country,
                                  mech_code = NULL,
                                  period = NULL) {

        dir <- {{dir_proc}}
        cntry <- {{country}}
        mech <- {{mech_code}}
        hfr_pd <- {{period}}

        ccode <- cntry[1]
        cname <- cntry[2]

        # Identify files
        file_pattern <- "^HFR_2020."

        # PD
        if (is.null(hfr_pd)) {
            file_pattern <- paste0(file_pattern, "\\d{2}_")
        }
        else {
            file_pattern <- paste0(file_pattern, as.character(hfr_pd), "_")
        }

        # Country
        file_pattern <- paste0(file_pattern, toupper(ccode), "_")

        # Mech Code
        if ( is.null(mech) ) {
            file_pattern <- paste0(file_pattern, "\\d{1+}_")
        }
        else {
            file_pattern <- paste0(file_pattern, mech, "_")
        }

        file_pattern <- paste0(file_pattern, "processed_\\d{8}.csv$")

        # Read data from files
        file_names <- list.files(
            path = dir,
            pattern = file_pattern,
            full.names = TRUE
        )

        # Match files
        df_hfr <- file_names %>%
            map_dfr(vroom)

        # return data frame
        return(df_hfr)
    }


    #' Expand / Transform HFR Data
    #'
    #' @param df_hfr HFR Processed data
    #' @param df_trgts MER Tagets
    #' @param df_orgs DATIM Org Hierarchy
    #' @param df_mechs DATIM Mechanism
    #' @return Augmented HFR Data as a Data Frame
    #'
    augmente_hfr_data <- function(df_hfr, df_trgts, df_orgs, df_mechs) {

        ## Variables
        hfr <- {{df_hfr}}
        targets <- {{df_trgts}}
        orgs <- {{df_orgs}}
        mechs <- {{df_mechs}}

        ## Expand orgnuit details => [countryname, snu1, psnuuid]
        hfr_data <- hfr %>%
            left_join(
                orgs %>% distinct(operatingunit, countryname,
                                  snu1, orgunituid, psnuuid),
                by = c("operatingunit", "orgunituid"),
                keep = FALSE
            )

        #hfr_data %>% glimpse()


        ## Append Mech Details => [mech_name, primepartner]
        hfr_data <- hfr_data %>%
            left_join(
                mechs %>% select(mech_code, mech_name, primepartner),
                by = "mech_code",
                keep = FALSE
            ) %>%
            select(-partner)

        #hfr_data %>% glimpse()


        ## Append MER Results & Targets => [fundingagency, mer_results, mer_targets]
        hfr_data <- hfr_data %>%
            left_join(
                targets,
                by = c("operatingunit", "orgunituid", "mech_code",
                       "indicator", "agecoarse", "sex"),
                keep = FALSE
            ) %>%
            select(-ends_with(".y")) %>%
            rename_at(vars(ends_with(".x")), str_remove, ".x")

        #hfr_data %>% glimpse()


        ## Reorder columns ==> arrange cols
        hfr_data <- hfr_data %>%
            select(fy, hfr_pd, date, operatingunit, countryname, snu1, psnu, psnuuid, orgunit, orgunituid,
                   fundingagency, mech_code, mech_name, primepartner, indicator, agecoarse, sex,
                   otherdisaggregate, val, mer_results, mer_targets)


        #hfr_data %>% glimpse()

        ## Return results
        return(hfr_data)
    }


    #' validate OU hfr_sqlview
    #'
    #' @param df_hfr HFR Processed data
    #' @param df_sqlview HFR SQL View
    #' @param check_subm Validate Submitted data? default = FALSE
    #' @return Validated HFR DataFrame
    #'
    validate_hfr_sqlview <- function(df_hfr, df_sqlview, check_subm = FALSE) {

        hfr <- {{df_hfr}}

        sqlview <- {{df_sqlview}}

        hfr_fy <- unique(hfr$fy)
        sql_fy <- unique(sqlview$fy)

        hfr_pd <- unique(hfr$hfr_pd)
        sql_pd <- unique(sqlview$hfr_pd)

        hfr_ou <- unique(hfr$operatingunit)
        sql_ou <- unique(sqlview$operatingunit)

        hfr_mech <- unique(hfr$mech_code)
        sql_mech <- unique(sqlview$mech_code)

        # Overview
        cat("\nFY:", hfr_fy, "|", sql_fy,
            "\nPD: ", hfr_pd, "|", sql_pd,
            "\nOU: ", hfr_ou, "|", sql_ou,
            "\nMech: ", hfr_mech, "|", sql_mech,
            "\nRows: ", nrow(hfr), "|", nrow(sqlview))


        # Unique Sites
        sites <- hfr %>%
            distinct(
                operatingunit, mech_code, partner,
                psnu, orgunit, orgunituid, indicator
            ) %>%
            nrow()

        vsites <- sqlview %>%
            distinct(
                operatingunit, mech_code, mech_name, primepartner,
                countryname, psnu, psnuuid, orgunit, orgunituid, indicator
            ) %>%
            nrow()

        cat("\n\nSites match: ",
            ifelse(vsites != sites, paint_red(vsites), paint_green(vsites)),
            "/",
            paint_green(sites))


        # Check for TX_MMD Indicator
        ind_tx_mmd <- is.element("TX_MMD",  hfr %>% distinct(indicator) %>% pull())

        cat("\n\nTX_MMD: ", ind_tx_mmd, "\n\n")

        # Check submitted / processed data
        if (check_subm == TRUE) {

            hfr <- hfr %>%
                is_ou_valid(df_orgs = orgs) %>%
                is_orgunituid_valid(df_orgs = orgs) %>%
                is_orgunituid4ou(df_orgs = orgs) %>%
                is_mech_valid(df_mechs = ims)
                #is_mech4ou(df_mechs = ims) TODO: Fix this

            # Errors
            ous <- hfr %>% filter(valid_ou == FALSE) %>% distinct(operatingunit) %>% nrow()
            uids <- hfr %>% filter(valid_uid == FALSE) %>% distinct(orgunituid) %>% nrow()
            uids4ou <- hfr %>% filter(uid_to_ou == FALSE) %>% distinct(orgunituid) %>% nrow()
            mechs <- hfr %>% filter(valid_mech == FALSE) %>% distinct(mech_code) %>% nrow()

            # Report errors
            cat("\nnValidations errors",
                "\nOUs:", ous,
                "\nOrgunituids: ", uids,
                "\nUIDs4OU: ", uids4ou,
                "\nMechs: ", mechs,
                "\nRows: ", nrow(hfr),
                "\nSQLView rows: ", nrow(sqlview))

        }

        # Check
        df <- hfr %>%
            left_join(
                sqlview,
                by = c('date', 'fy', "hfr_pd", "orgunit", "orgunituid", "mech_code",
                       "operatingunit", "psnu", "indicator", "sex", "agecoarse", "otherdisaggregate"),
                keep = FALSE
            ) %>%
            rename_at(vars(ends_with(".x")), str_remove, ".x") %>%
            relocate(ends_with(".y"), .after = val) %>%
            mutate(match_val = ifelse(!is.na(val) & val == val.y, TRUE, FALSE))


        return(df)
    }


    #' Validations
    #'
    #'
    validations <- function(f_path, df_sqlview){

        ous <- c("Angola",
                 "Asia Region",
                 "Botswana",
                 "Burundi",
                 "Cameroon",
                 "Cote d'Ivoire",
                 "Democratic Republic of the Congo",
                 "Dominican Republic",
                 "Eswatini",
                 "Ethiopia",
                 "Haiti",
                 "Kenya",
                 "Lesotho",
                 "Malawi",
                 "Mozambique",
                 "Namibia",
                 "Nigeria",
                 "Rwanda",
                 "South Africa",
                 "South Sudan",
                 "Tanzania",
                 "Uganda",
                 "Ukraine",
                 "Vietnam",
                 "West Africa Region",
                 "Western Hemisphere Region",
                 "Zambia",
                 "Zimbabwe")

        file_path <- {{f_path}}

        sqlview <- {{df_sqlview}}

        file_name <- basename(file_path)

        print(file_name)

        file_parts <- parse_submission(file_name)

        pd <- file_parts[1]

        proc_fy <- str_split(pd, "[.]") %>%
            unlist() %>%
            first()

        proc_pd <- str_split(pd, "[.]") %>%
            unlist() %>%
            last()

        code <- file_parts[2]

        ou <- get_operatingunit(code, orglevels = levels)

        ou <- ifelse(is.na(ou) & code == "XCB", "Caribbean Region",
                     ifelse(is.na(ou) & code == "XAM", "Central America Region",
                            ou))

        mech <- file_parts[3]

        pdate <- file_parts[4]

        # Validation data
        df <- data.frame(
            file_name = file_name,
            hfr_fy = proc_fy,
            hfr_pd = proc_pd,
            iso3 = code,
            operatingunit = ou,
            mech_code = mech,
            proc_date = pdate,
            hfr_rows = NA,
            mer_rows = NA,
            sql_rows = NA,
            match_rows = NA,
            hfr_sites = NA,
            mer_sites = NA,
            sql_sites = NA,
            match_sites = NA,
            hfr_indicators = NA,
            mer_indicators = NA,
            sql_indicators = NA,
            match_indicators = NA,
            hfr_invalid_sex = NA,
            sql_invalid_sex = NA,
            hfr_invalid_age = NA,
            sql_invalid_age = NA,
            hfr_invalid_val = NA,
            sql_invalid_val = NA
        )

        # Check OU
        if (!ou %in% ous) {
            return(df)
        }

        # Read hfr process data
        df_hfr <- file_path %>%
            vroom()

        # Read MER Targets data
        df_mer <- get_mer_targets(
                dir_targets = "../../HFR-Data/Datim/FY20Q3",
                country = code,
                mech_code = mech
            )

        # Filter SQL View
        df_im_sqlview <- sqlview %>%
            filter(
                operatingunit == ou,
                fy == proc_fy,
                hfr_pd == proc_pd,
                mech_code == mech,
                !is.na(mer_results) | !is.na(mer_targets)
            )

        # Check data from processed & sql view
        if (nrow(df_mer) == 0 | nrow(df_im_sqlview) == 0) {
            return(df)
        }

        # Rows
        df$hfr_rows <- nrow(df_hfr)

        df$mer_rows <- nrow(df_mer)

        # Unique Sites
        df$hfr_sites <- df_hfr %>%
            distinct(
                operatingunit, mech_code, partner,
                psnu, orgunit, orgunituid, indicator
            ) %>%
            nrow()

        df$mer_sites <- df_mer %>%
            distinct(
                operatingunit, mech_code, mech_name,
                psnu, orgunit, orgunituid, indicator
            ) %>%
            nrow()

        # Unique indicators
        df$hfr_indicators <- df_hfr %>%
            distinct(indicator) %>%
            nrow()

        df$mer_indicators <- df_mer %>%
            distinct(indicator) %>%
            nrow()

        # SQL
        df$sql_rows <- df_im_sqlview %>% nrow()

        df$sql_sites <- df_im_sqlview %>%
            distinct(
                operatingunit, mech_code, mech_name, primepartner,
                countryname, psnu, psnuuid, orgunit, orgunituid, indicator
            ) %>%
            nrow()

        df$sql_indicators <- df_im_sqlview %>%
            distinct(indicator) %>%
            nrow()


        df <- df %>%
            rowwise() %>%
            mutate(
                match_rows = ifelse(mer_rows == sql_rows, TRUE, FALSE),
                match_sites = ifelse(mer_sites == sql_sites, TRUE, FALSE),
                completeness = round(hfr_sites / mer_sites * 100),
                match_indicators = ifelse(mer_indicators == sql_indicators, TRUE, FALSE),
                hfr_invalid_sex = df_hfr %>% filter(!sex %in% c("Female", "Male", NA)) %>% distinct(sex) %>% nrow(),
                sql_invalid_sex = df_im_sqlview %>% filter(!sex %in% c("Female", "Male", NA)) %>% distinct(sex) %>% nrow(),
                hfr_invalid_age = df_hfr %>% filter(!agecoarse %in% c("<15", "15+", NA)) %>% distinct(agecoarse) %>% nrow(),
                sql_invalid_age = df_im_sqlview %>% filter(!agecoarse %in% c("<15", "15+", NA)) %>% distinct(agecoarse) %>% nrow(),
                hfr_invalid_val = df_hfr %>% filter(!is.na(val) & val < 0 | is.character(val)) %>% nrow(),
                sql_invalid_val = df_im_sqlview %>% filter(!is.na(val) & val < 0 | is.character(val)) %>% nrow()
            )

        return(df)
    }


    #' Validate OU SQL Views
    #'
    #' @param dir_proc
    #' @param df_sqlview
    #' @param check_subm
    validate_hfr_files <- function(dir_proc,
                                   df_orglevels,
                                   df_sqlview,
                                   check_subm = FALSE) {

        dir <- {{dir_proc}}

        levels <- {{df_orglevels}}

        sqlview <- {{df_sqlview}}

        check <- {{check_subm}}

        # Get processed filles
        proc_files <- list.files(
            path = dir,
            pattern = "^HFR_\\d{4}.\\d{2}_[A-Z]{3}_\\d{1+}_processed_\\d{8}.csv",
            full.names = TRUE
        )

        # Extract file info
        df_files <- proc_files %>%
            map_dfr(.x, .f = ~ validations(f_path = .x, df_sqlview = sqlview))

        return(df_files)
    }

# DATA -------------------------------------------------------------------


    ## Look up tables
    Wavelength::load_lookups(datim_path = dir_datim)

    orgs %>% glimpse()

    ims <- ims %>%
        select(mech_code, mech_name, primepartner)

    levels <- Wavelength::identify_levels(username = user, password = mypwd(key))

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
    hfr_aug <- augmente_hfr_data(
        df_hfr = hfr_processed,
        df_trgts = hfr_targets,
        df_orgs = orgs,
        df_mechs = ims
    )

    hfr_aug %>% glimpse()


    ## HFR SQL Data
    hfr_sqlview <- get_hfr_sqlview(
            dir_path = dir_sqlviews,
            period = curr_pd
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
