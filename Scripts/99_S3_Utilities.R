## PROJECT:  HFR S3 Browser
## AUTHOR:   B.Kagniniwa | USAID
## LICENSE:  MIT
## PURPOSE:  Explore S3 Structure / Datasets
## Date:     2020-12-14

# LIBRARIES ------

library(tidyverse)
library(glamr)
library(janitor)
library(aws.s3)

# GLOBALS --------

  # Load Secrets
  glamr::load_secrets()

  # DDC/Dev - S3 Key
  s3_access_key <- NULL
  s3_secret_key <- NULL

  # List Buckets
  bkts <- bucketlist()
    
  bkts %>% glimpse()
  
  bkts %>% View()
  
  # Bucket name
  bkt_name <- "gov-usaid"
  
  # Check buckets
  bucket_exists(bucket = bkt_name)
  
  # Access specific bucket as a list
  #bkt_usaid <- get_bucket(bucket = bkt_name)
  
  # Access specific bucket as a df
  bkt_usaid <- get_bucket_df(bucket = bkt_name,
                             prefix = "/ddc/dev/",
                             #marker = "\\/ddc\\/dev",
                             max = Inf)
  
  bkt_usaid %>% glimpse()
  
  bkt_usaid %>% 
    clean_names() %>% 
    mutate(size = as.numeric(size)) %>% 
    #filter(size > 0) %>% 
    View()
  
  # Get bucket as an object
  obj_usaid <- get_object(object = "Limited_Baseline_no_errors_Limited.xlsx",
                          bucket = bkt_name)
  
  obj_usaid %>% rawToChar()
  