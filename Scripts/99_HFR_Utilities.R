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

# REQUIRED

source("./Scripts/00_Config.R")

# FUNCTIONS -------------------------------------------

files <- list.files(
  path = dir_curr_pd,
  pattern = "^HFR_2020.\\d{2}_[A-Z]{3}_.*.csv$",
  full.names = TRUE
)

#' @title Get Latest files
#' 
#' @param files list of processed file names
#' @return list of latest processed file names
#' 
get_latest <- function(files) {
  
  latest_files <- files %>% 
    file.info() %>%
    tibble::rownames_to_column(var = "filepath") %>% 
    dplyr::filter(size > 0) %>% 
    dplyr::rowwise() %>% 
    dplyr::mutate(
      filename = basename(filepath),
      filename = dplyr::str_remove_all(filename, "HFR_|processed_|.csv")
    ) %>% 
    dplyr::ungroup() %>% 
    tidyr::separate(
      filename, 
      into = c("hfr_pd", "ou_country", "mech_code", "pdate"), 
      sep = "_",
      remove = F
    ) %>% 
    dplyr::group_by(hfr_pd, ou_country, mech_code, pdate) %>% 
    dplyr::filter(pdate == max(pdate)) %>% 
    dplyr::ungroup() %>% 
    dplyr::pull(filepath)
    
  return(latest_files)
}
  




  


#' Get SQL View data
#'
#' @param dir_path Directory path
#' @param fiscal_year Fiscal Year as [\\d{4}]
#' @param period HFR Period as [\\d{1,2}]
#' @param pattern Filename pattern (optional)
#' @return SQLView Data
#'
get_hfr_sqlview <- 
  function(dir_path, 
           fiscal_year = 2020,
           period = 1,
           pattern = NULL) {
  
  # Variables
  dir <- {{dir_path}}
  hfr_fy <- {{fiscal_year}}
  
  hfr_pd <- {{period}}
  hfr_pd <- str_pad(hfr_pd, 2, side = "left", pad = "0")
  
  f_pattern <- {{pattern}}
  
  # File name pattern
  file_pattern <- paste0("^HFR_", hfr_fy, ".", hfr_pd, "_Tableau_\\d{8}.csv$")
  
  # Skip fy & pd params, use specified pattern
  if ( !is.null(f_pattern) ) {
    file_pattern <- f_pattern
  }
  
  # File name(s)
  file_name <- list.files(
      path = dir,
      pattern = file_pattern,
      full.names = TRUE
    ) %>%
    sort() %>%
    last()
  
  if ( is.na(file_name) | is.null(file_name) ) {
    
    cat("\n",
        paint_red("Error: Could not identify targetted sqlview file."),
        "\n")
    
    return(NULL)
  }
  
  # File Content
  df <- file_name %>% vroom()
  
  return(df)
}


#' @title Get MER Targets/Results data
#' @param dir_targets path to target files
#' @param country OU or Country ISO3 code
#' @param mech_code Mechanism Code
#' @return MER Results/Targets data as dataframe
#'
get_mer_targets <- 
  function(dir_targets, 
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
  } else {
    file_pattern <- paste0(file_pattern, "[A-Z]{3}_")
  }
  
  # Mech Code
  if ( !is.null(mech) ) {
    file_pattern <- paste0(file_pattern, mech)
  } else {
    file_pattern <- paste0(file_pattern, "\\d{1+}")
  }
  
  # Processed date
  file_pattern <- paste0(file_pattern, "_DATIM_\\d{8}.csv$")
  
  # File names(s)
  file_names <- list.files(
    path = dir,
    pattern = file_pattern,
    full.names = TRUE
  )
  
  # File Content
  df <- file_names %>% map_dfr(vroom)
  
  return(df)
}


#' @title Get OU/Country HFR Processed data
#'
#' @param dir_proc path to processed files
#' @param country Vector of OU/Country Name & ISO3 Code
#' @param mech_code Mechanism Code
#' @param period HFR Reporting Period
#' @return HFR Processed data as dataframe
#'
get_hfr_processed <- 
  function(dir_proc, 
           country, 
           mech_code = NULL,
           period = NULL) {
  
  # Evaluate params with curly-curly pattern
  dir <- {{dir_proc}}
  cntry <- {{country}}
  mech <- {{mech_code}}
  hfr_pd <- {{period}}
  
  # Country details
  ccode <- cntry[1]
  #cname <- cntry[2]
  
  # Identify files by pattern
  file_pattern <- "^HFR_2020."
  
  # Reporting Period
  if (is.null(hfr_pd)) {
    file_pattern <- paste0(file_pattern, "\\d{2}_")
    
  } else if (is.integer(hfr_pd)) {
    file_pattern <- paste0(file_pattern, 
                           str_pad(hfr_pd, 
                                   width = 2, 
                                   side = "left", 
                                   pad = "0"), "_")
  } else {
    stop(paste0("Invalid reporting period: ", hfr_pd))
  }
  
  # Country ISO Code
  file_pattern <- paste0(file_pattern, 
                         toupper(ccode), 
                         "_")
  
  # Mech Code
  if ( is.null(mech) ) {
    file_pattern <- paste0(file_pattern, 
                           "\\d{1+}",
                           "_")
  
  } else {
    file_pattern <- paste0(file_pattern, 
                           mech, 
                           "_")
  }
  
  # Processed date
  file_pattern <- paste0(file_pattern, "processed_\\d{8}.csv$")
  
  # Read data from files
  file_names <- list.files(
    path = dir,
    pattern = file_pattern,
    full.names = TRUE
  )
  
  # TODO: Get latest files based on processed dates
  
  # Match files
  df_hfr <- file_names %>%
    map_dfr(vroom)
  
  # return data frame
  return(df_hfr)
}


#' @title Expand HFR Processed Data
#'
#' @param df_hfr HFR Processed data
#' @param df_trgts MER Targets/Results data
#' @param df_orgs DATIM Org Hierarchy data
#' @param df_mechs DATIM Mechanisms data
#' @return Augmented HFR Data as a dataframe
#'
augmente_hfr_processed <- 
  function(df_hfr, 
           df_trgts, 
           df_orgs, 
           df_mechs) {
  
  ## Variables
  hfr <- {{df_hfr}}
  targets <- {{df_trgts}}
  orgs <- {{df_orgs}}
  mechs <- {{df_mechs}}
  
  # Expand orgnuit details => [countryname, snu1, psnuuid]
  hfr_data <- hfr %>%
    left_join(
      orgs %>% 
        distinct(operatingunit, 
                 countryname,
                 snu1, 
                 orgunituid,
                 psnuuid),
      by = c("operatingunit", "orgunituid"),
      keep = FALSE
    )
  
  # Append Mech Details => [mech_name, primepartner]
  hfr_data <- hfr_data %>%
    left_join(
      mechs %>% 
        select(mech_code, 
               mech_name, 
               primepartner),
      by = "mech_code",
      keep = FALSE
    ) %>%
    select(-partner)
  
  # Append MER Results & Targets => [fundingagency, mer_results, mer_targets]
  hfr_data <- hfr_data %>%
    left_join(
      targets,
      by = c("operatingunit", 
             "orgunituid", 
             "mech_code",
             "indicator", 
             "agecoarse", 
             "sex"),
      keep = FALSE
    ) %>%
    select(-ends_with(".y")) %>%
    rename_at(vars(ends_with(".x")), str_remove, ".x")

  # Reorder columns ==> arrange cols
  hfr_data <- hfr_data %>%
    select(fy, hfr_pd, date, operatingunit, countryname, 
           snu1, psnu, psnuuid, orgunit, orgunituid,
           fundingagency, mech_code, mech_name, 
           primepartner, indicator, agecoarse, sex,
           otherdisaggregate, val, mer_results, mer_targets)
  
  # Return results
  return(hfr_data)
}


#' Expand / Transform HFR Data
#'
#' @param df_hfr HFR Processed data
#' @param df_trgts MER Targets/Results
#' @param df_orgs DATIM Org Hierarchy
#' @param df_mechs DATIM Mechanisms
#' @return Transformed HFR Data as a dataframe
#'
transform_hfr_processed <- 
  function(df_hfr, 
           df_trgts, 
           df_orgs, 
           df_mechs) {
  
  ## Variables
  hfr <- {{df_hfr}}
  targets <- {{df_trgts}}
  orgs <- {{df_orgs}}
  mechs <- {{df_mechs}}
  
  ## Append MER Results & Targets => [fundingagency, mer_results, mer_targets]
  hfr_data <- hfr %>%
    full_join(targets,
              by = c("fy", 
                     "operatingunit", 
                     "orgunituid", 
                     "mech_code",
                     "indicator", 
                     "agecoarse", 
                     "sex"),
              keep = FALSE
    ) %>% 
    rename_at(vars(ends_with(".x")), str_remove, ".x") %>%
    mutate(
      orgunit = ifelse(is.na(orgunit), orgunit.y, orgunit),
      psnu = ifelse(is.na(psnu), psnu.y, psnu)
    ) %>%
    select(-ends_with(".y"))
  
  ## Expand orgnuit details => [countryname, snu1, psnuuid]
  hfr_data <- hfr_data %>%
    left_join(
      orgs %>% 
        distinct(operatingunit, 
                 countryname, 
                 snu1, 
                 psnuuid, 
                 orgunituid),
      by = c("operatingunit", "orgunituid"),
      keep = FALSE
    ) %>% 
    rename_at(vars(ends_with(".x")), str_remove, ".x") %>% 
    mutate(
      snu1 = ifelse(is.na(snu1), snu1.y, snu1),
      countryname = ifelse(is.na(countryname), countryname.y, countryname)
    ) %>% 
    select(-ends_with(".y"))

  # Orgs errors
  orgs_errors <- hfr_data %>% 
    filter(is.na(snu1)) %>% 
    distinct(operatingunit, orgunituid) %>% 
    nrow()
  
  cat("\nSites with non-valid ou/orgunituid:", 
      ifelse(orgs_errors > 0, 
             paint_red(orgs_errors), 
             paint_green(orgs_errors)), 
      "\n")
  
  # Get target errors
  mer_errors <- hfr_data %>% 
    filter(is.na(mech_name)) %>% 
    distinct(operatingunit, orgunituid) %>% 
    nrow()
  
  cat("\nHFR Sites with no mer-results: ", 
      ifelse(mer_errors > 0, 
             paint_red(mer_errors), 
             paint_green(mer_errors)),
      "\n")
  
  # Get hfr errors
  hfr_errors <- hfr_data %>% 
    filter(is.na(date)) %>% 
    distinct(operatingunit, orgunituid) %>% 
    nrow()
  
  cat("\nMER Sites with no hfr-submissions: ", 
      ifelse(hfr_errors > 0, 
             paint_red(hfr_errors), 
             paint_green(hfr_errors)),
      "\n")
  
  ## Append Mech Details => [mech_name, primepartner]
  hfr_data <- hfr_data %>%
    left_join(
      mechs %>% 
        distinct(mech_code, mech_name, primepartner),
      by = "mech_code",
      keep = FALSE
    ) %>% 
    rename_at(vars(ends_with(".x")), str_remove, ".x") %>% 
    mutate(
      mech_name = ifelse(is.na(mech_name), mech_name.y, mech_name)
    ) %>% 
    select(-mech_name.y)
  
  # Get mechs  errors
  mechs_errors <- hfr_data %>% 
    filter(is.na(mech_name) | is.na(primepartner)) %>% 
    distinct(operatingunit, orgunituid) %>% 
    nrow()
  
  cat("\nMechanisms with non-valid code:", 
      ifelse(mechs_errors > 0, 
             paint_red(mechs_errors), 
             paint_green(mechs_errors)), 
      "\n")
  
  
  ## Reorder columns ==> arrange cols
  hfr_data <- hfr_data %>%
    select(fy, hfr_pd, date, operatingunit, countryname, 
           snu1, psnu, psnuuid, orgunit, orgunituid,
           mech_code, mech_name, primepartner, indicator, agecoarse, sex,
           otherdisaggregate, val, mer_results, mer_targets)
  
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
validate_hfr_sqlview <- 
  function(df_hfr, 
           df_sqlview, 
           check_subm = FALSE) {
  
  # Variables
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
      ifelse(vsites != sites, 
             paint_red(vsites), 
             paint_green(vsites)),
      "/",
      paint_green(sites))
  
  
  # Check for TX_MMD Indicator
  ind_tx_mmd <- is.element("TX_MMD",  hfr %>% distinct(indicator) %>% pull())
  
  cat("\n\nTX_MMD: ", 
      ind_tx_mmd, 
      "\n\n")
  
  # Check submitted / processed data
  if (check_subm == TRUE) {
    
    hfr <- hfr %>%
      is_ou_valid(df_orgs = orgs) %>%
      is_orgunituid_valid(df_orgs = orgs) %>%
      is_orgunituid4ou(df_orgs = orgs) %>%
      is_mech_valid(df_mechs = ims)
    
    # TODO: Fix this
    #hfr %>% is_mech4ou(df_mechs = ims) 
    
    # Errors
    ous <- hfr %>% 
      filter(valid_ou == FALSE) %>% 
      distinct(operatingunit) %>% 
      nrow()
    
    uids <- hfr %>% 
      filter(valid_uid == FALSE) %>% 
      distinct(orgunituid) %>% 
      nrow()
    
    uids4ou <- hfr %>% 
      filter(uid_to_ou == FALSE) %>% 
      distinct(orgunituid) %>% 
      nrow()
    
    mechs <- hfr %>% 
      filter(valid_mech == FALSE) %>% 
      distinct(mech_code) %>% 
      nrow()
    
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
      by = c('date', 'fy', "hfr_pd", 
             "orgunit","orgunituid", "mech_code", 
             "operatingunit", "psnu", "indicator", 
             "sex", "agecoarse", "otherdisaggregate"),
      keep = FALSE
    ) %>%
    rename_at(vars(ends_with(".x")), str_remove, ".x") %>%
    relocate(ends_with(".y"), .after = val) %>%
    mutate(match_val = ifelse(!is.na(val) & val == val.y, TRUE, FALSE))
  
  # Return data frame
  return(df)
}


#' Validations
#'
#' @param f_path Full path of the file
#' @param df_sqlview SQL View data as dataframe
#' @return Errors data as dataframe
#' 
validations <- 
  function(f_path, 
           df_sqlview,
           df_orglevels) {
  
  #Valid OU Names
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
  
  # Variables
  file_path <- {{f_path}}
  sqlview <- {{df_sqlview}}
  levels <- {{df_orglevels}}
  
  # File components
  file_name <- basename(file_path)
  
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
  
  ou <- case_when(
    is.na(ou) & code == "XCB" ~ "Caribbean Region", 
    is.na(ou) & code == "XAM", "Central America Region",
    TRUE ~ ou
  )
  
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
  df_hfr <- file_path %>% vroom()
  
  # Read MER Targets data
  df_mer <- get_mer_targets(
    dir_targets = "../../HFR-Data/Datim/FY20Q3",
    country = code,
    mech_code = mech
  )
  
  # Filter SQL View
  df_im_sqlview <- 
    sqlview %>%
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
      hfr_invalid_sex = df_hfr %>% 
        filter(!sex %in% c("Female", "Male", NA)) %>%
        distinct(sex) %>% 
        nrow(),
      sql_invalid_sex = df_im_sqlview %>% 
        filter(!sex %in% c("Female", "Male", NA)) %>% 
        distinct(sex) %>% 
        nrow(),
      hfr_invalid_age = df_hfr %>% 
        filter(!agecoarse %in% c("<15", "15+", NA)) %>% 
        distinct(agecoarse) %>% 
        nrow(),
      sql_invalid_age = df_im_sqlview %>% 
        filter(!agecoarse %in% c("<15", "15+", NA)) %>% 
        distinct(agecoarse) %>% 
        nrow(),
      hfr_invalid_val = df_hfr %>% 
        filter(!is.na(val) & val < 0 | is.character(val)) %>% 
        nrow(),
      sql_invalid_val = df_im_sqlview %>% 
        filter(!is.na(val) & val < 0 | is.character(val)) %>% 
        nrow()
    ) %>% 
    ungroup()
  
  return(df)
}


#' Validate OU SQL Views
#'
#' @param dir_proc
#' @param df_sqlview
#' @param check_subm
#' 
validate_hfr_files <- function(dir_proc,
                               df_orglevels,
                               df_sqlview,
                               check_subm = FALSE) {
  
  # Variables
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
    map_dfr(.x, .f = ~ validations(f_path = .x, 
                                   df_sqlview = sqlview,
                                   df_orglevels = levels))
  
  return(df_files)
}
