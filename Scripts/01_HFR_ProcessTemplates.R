## PROJECT:  USAID/OHA HFR Data Processing
## AUTHOR:   B.Kagniniwa | USAID
## LICENSE:  MIT
## PURPOSE:  Process Temaplate
## Date:     2020-09-18

# LIBRARIES -----------------------------------------

#devtools::load_all()

library(tidyverse)
library(Wavelength)
library(here)

# DEPENDENCIES

source("./Scripts/00_Config.R")

# GLOBALS --------------------------------------------

dir_sub <- "ou_submissions"
dir_curr <- "2020.12"

dir_sub_curr <- here(dir_data, dir_sub, dir_curr)

dir_proc <- "ou_processed"

dir_proc_curr <- here(dir_dataout, dir_proc, dir_curr)


# FILES ----------------------------------------------

(files <- list.files(
        path = dir_sub_curr, 
        pattern = "Bots|BOTS",
        full.names = TRUE
    ))

# FUNCTIONS ------------------------------------------

get_submissions <- function(dir_files, ou = NULL) {
    
    dir <- {{dir_files}}
    ou <- {{ou}}
    
    if (is.null(ou)) {
        cat(paint_red("\nMissing OU and/or Pattern for OU Files", "\n\n"))
        return(NULL)
    }
    
    files <- list.files(
            path = dir, 
            pattern = ou,
            full.names = TRUE
        )
    
    return(files)
}

# DATA -----------------------------------------------

    ## Load all look up tables: orgs, ims, valid_dates
    load_lookups(datim_path = dir_datim)

    ## Countries
    ## South Sudan
    
    files <- get_submissions(
        dir_files = dir_sub_curr, 
        ou = "GHANA")
    
    files

    ## Check tables for HFR
    purrr::walk(files, is_hfrtab)

    
    purrr::walk(files, hfr_process_template)

    
    purrr::walk(files,
                hfr_process_template,
                round_hfrdate = TRUE, 
                hfr_pd_sel = 12, 
                folderpath_output = dir_proc_curr, 
                datim_path = dir_datim
            )
    

    hfr_orgunit_search(
        orgs, "Nana Benie") %>% 
        #glimpse() %>% 
        View()
        
    #81861




