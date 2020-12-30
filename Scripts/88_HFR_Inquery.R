
# Libraries

library(Wavelength)
library(readxl)


# Vars ----

data <- "./Data"

dataout <- "./Dataout"

in_file <- "PNG HFR raw data_Dec 22.xlsx"

in_tab <- "HFR Raw Data_FY21"


# Data ----

df <- read_excel(
    path = file.path(data, in_file),
    sheet = in_tab,
    col_types = "text"
  ) %>% 
  hfr_gather() %>% 
  hfr_munge_string() %>% 
  hfr_fix_date() %>%
  hfr_assign_pds() %>% 
  hfr_fix_noncompliance() %>% 
  mutate(
    mech_code = str_remove(mech_code, "-.*"),
    mech_code = as.character(mech_code)) 

# Export cleaned data

hfr_export(df, dataout, by_mech = FALSE)
