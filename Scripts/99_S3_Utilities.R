## PROJECT:  HFR S3 Browser
## AUTHOR:   B.Kagniniwa | USAID
## LICENSE:  MIT
## PURPOSE:  Explore S3 Structure / Datasets
## Date:     2020-12-14

# LIBRARIES ------

library(tidyverse)
library(glamr)
library(janitor)
library(lubridate)
library(aws.s3)
#library(paws)

# IMPORTS ----
  
  source("./Scripts/99_Utilities.R")

# GLOBALS ----

  # DDC - S3 Storage
  Sys.setenv(
    "AWS_ACCESS_KEY_ID" = get_key("s3", "access key"),
    "AWS_SECRET_ACCESS_KEY" = get_key("s3", "secret key"),
    "AWS_REGION" = "us-east-1"
  )

  # DDC S3 Structure
  s3_sys_envs <- c("dev", "test", "uat", "stg", "prod", "ddc")
  
  s3_data_types <- c("raw", "processed") # + logs & 1 & 2
  
  s3_use_cases <- c("expenditure", "hfr", "supplychain")
  
  s3_data_routes <- c("incoming", "receiving", "intermediate", 
                      "outgoing", "rejects")

# FUNCTIONS ----

  #' @title Get S3 Buckets list
  #' @note Works with R Package: paws::storage
  #' 
  #' @param s3_client
  #' @return S3 Backets list as tibble
  #' 
  ddc_buckets <- function(s3_client) {
    
    # Get all buckets
    s3_buckets <- s3_client$list_buckets()
    
    # convert results into tibble
    s3_buckets$Buckets %>% 
      map_df(~tibble(name = .$Name,
                     creation_date = .$CreationDate))
  }
  
  #' @title Get S3 Buckets list
  #' @note Works with R Package: aws.s3
  #' 
  #' @param access_key
  #' @param secret_key
  #' @return S3 Backets list as tibble
  #' 
  s3_buckets <- function(access_key = NULL,
                         secret_key = NULL) {
    
    # Check keys
    if (is.null(access_key))
      access_key = get_key("s3", "access key")
    
    if (is.null(secret_key))
      secret_key = get_key("s3", "secret key")
    
    # Get S3 Buckets as tibble
    aws.s3::bucket_list_df(
      key = access_key, 
      secret = secret_key
      ) %>% 
      dplyr::as_tibble() %>% 
      janitor::clean_names()
  }
  
  #' @title Get S3 Bucket objects list
  #' @note Works with R Package: paws::storage
  #' 
  #' @param s3_client
  #' @param s3_bucket
  #' 
  #' @return S3 Objects list as tibble
  #' 
  ddc_objects <- function(s3_client, s3_bucket) {
    
    # Get all objects
    s3_objects <- s3_client$list_objects(
      Bucket = s3_bucket
    )
    
    # Convert results into tibble
    s3_objects$Contents %>% 
      map_df(~tibble(key = .$Key,
                     last_modified = .$LastModified,
                     etag = .$ETag,
                     size = .$Size))
  }
  
  
  #' @title Get S3 Bucket objects list
  #' @note Works with R Package: aws.s3
  #' 
  #' @param bucket      S3 Bucket name
  #' @param prefix      Limit response by key
  #' @param n           Max number of record, default = 1000
  #' @param access_key
  #' @param secret_key
  #' 
  #' @return S3 Objects list as tibble
  #' 
  s3_objects <- function(bucket,
                         prefix = "ddc/uat",
                         n = 1000,
                         access_key = NULL,
                         secret_key = NULL) {
    
    # Check keys
    if (is.null(access_key))
      access_key = get_key("s3", "access key")
    
    if (is.null(secret_key))
      secret_key = get_key("s3", "secret key")
    
    # Get & clean objects
    objects <- aws.s3::get_bucket_df(
        bucket = bucket,
        prefix = prefix,
        max = n,
        key = access_key,
        secret = secret_key
      ) %>% 
      dplyr::as_tibble() %>% 
      janitor::clean_names() %>% #glimpse()
      dplyr::rename(etag = e_tag) %>% 
      dplyr::mutate(
        etag = str_remove_all(etag, '\"'),
        last_modified = lubradate::ymd(base::as.Date(last_modified)),
        size = as.integer(size)
      ) %>% 
      dplyr::relocate(bucket, .before = 1)
    
    return(objects)
  }
  
  #' @title Unpack Objects Key
  #' @note Works with R Package: aws.s3
  #' 
  #' @param df_objects      
  #' @param rmv_sysfiles
  #' @param rmv_hidden
  #' 
  #' @return S3 Cleaned Objects list as tibble
  #' 
  s3_unpack_keys <- 
    function(df_objects, 
             rmv_sysfiles = TRUE, 
             rmv_hidden = TRUE) {
    
    # Identify all prefixes (subfolders)
    paths <- df_objects %>% 
      dplyr::rowwise() %>% 
      dplyr::mutate(
        ndir = stringr::str_count(key, "/") + 1
      ) %>% 
      dplyr::distinct(ndir) %>% 
      dplyr::pull(ndir) %>% 
      base::sort()
    
    # Set new colnames
    s3_paths <- paste0("path", seq(1, max(paths), 1))
    
    s3_paths_clean <- c("system", 
                        "sys_env", 
                        "sys_data_type", 
                        "sys_use_case", 
                        "sys_data_route",
                        "sys_data_object")
    
    # Stop here for non ddc sys bucket
    if (length(s3_paths) < length(s3_paths_clean))
      return(df_objects)
    
    # Continue
    s3_paths[1:length(s3_paths_clean)] <- s3_paths_clean
    
    # Separate paths & document
    df_objects <- df_objects %>% 
      separate(key, 
               into = s3_paths, 
               sep = "/",
               remove = FALSE,
               fill = "right") 
    
    # remove system files/folders
    if (rmv_sysfiles) {
      
      data_type <- s3_paths[3]
      
      df_objects <- df_objects %>% 
        dplyr::filter(!!sym(data_type) %in% c("raw", "processed"))
    }
             
    # remove hidden files / path (eg: .trifacta) 
    if (rmv_hidden) {
      
      data_route <- s3_paths[5]
      data_object <- s3_paths[6]
      
      df_objects <- df_objects %>% 
        dplyr::filter(
          !stringr::str_detect(sym({{data_route}}), "^[.]"), 
          !stringr::str_detect(sym({{data_object}}), "^[.]")
        )
    }
    
    return(df_objects)
  }
  
  
  #' @title Read content of S3 Objects
  #' 
  #' @param bucket
  #' @param object_key
  #' @param access_key
  #' @param secret_key
  #' 
  #' 
  s3_read_object <- function(bucket, object_key,
                                access_key = NULL,
                                secret_key = NULL) {
    # Check keys
    if (is.null(access_key))
      access_key = get_key("s3", "access key")
    
    if (is.null(secret_key))
      secret_key = get_key("s3", "secret key")
    
    # Get object as raw data
    object_raw <- aws.s3::get_object(
        bucket = bucket,
        object = object_key,
        key = access_key,
        secret = secret_key
      ) 
    
    # Create connection to raw data
    conn <- base::rawConnection(object_raw, open = "r") 
    
    #base::on.exit({base::close(conn)})
    
    # Read content of raw data as tibble
    df <- vroom::vroom(conn)
    
    return(df)
  }
  
  #' @title Download S3 Object
  #' @note Works with R Package: paws::storage
  #' 
  #' @param s3_client
  #' @param s3_bucket
  #' @param s3_key
  #' @param filename
  #' 
  ddc_download <- 
    function(s3_client, 
             s3_bucket, 
             s3_key, 
             filename = NULL) {
    
    # get object
    obj <- s3_client$get_object(
      Bucket = s3_bucket,
      Key = basename(file_csv)
    )
    
    if (is.null(filename))
      filename <- basename(s3_key)
    
    # write content to file
    writeBin(obj$Body, con = filename)
  }

  #' Transfer file to S3 Object
  #' @param s3_client
  #' @param s3_bucket
  #' @param local_file
  #' @param s3_object_name
  #' 
  hfr_file_transfer <- 
    function(s3_client,  
             s3_bucket, 
             local_file,
             s3_object_name = NULL) {
    
    # Connection to local file
    conn <- base::file(local_file, "rb")
    
    # Close connection on function exit
    base::on.exit({base::close(conn)})
    
    # check file name
    if (is.null(s3_object_name)) {
      s3_object_name <- base::basename(local_file)
    }
    
    # file transfer as binary data
    raw_dta <- base::readBin(conn, what = "raw", n = file.size(local_file))
    
    # transfer file content
    transfer <- s3_client$put_object(
      Body = raw_dta,
      Bucket = s3_bucket,
      Key = s3_object_name
    )
    
    if (!is.null(transfer))
      return(stringr::str_remove_all(transfer$ETag, '\"'))
    
    return(NULL)
  }

  
# AWS.S3 Package ----  

  # Get S3 Buckets list
  
  #aws.s3::bucketlist()
  #aws.s3::bucket_list_df()
  
  bkts <- bucketlist(key = get_key("s3", "access key"),
                     secret = get_key("s3", "secret key"))
  
  bkts <- bkts %>% 
    as_tibble() %>% 
    clean_names() 
  
  bkts %>% prinf()
  
  # Bucket name
  bkt_name <- bkts %>% 
    filter(str_detect(bucket, "^g.*aid$")) %>% 
    pull(bucket)
  
  bkt_name
  
  # Check if bucket existx
  bucket_exists(bucket = bkt_name)
  
  # List of the content of specific s3 bucket 
  bkt_objects <- get_bucket(bucket = bkt_name,
                    #prefix = "ddc/uat/processed/outgoing"
                    max = 10)
  
  # content of bucket as a df
  bkt_objects <- get_bucket_df(bucket = bkt_name,
                               prefix = "ddc/uat",
                               max = Inf)
  
  bkt_objects %>% glimpse()
  
  bkt_objects <- bkt_objects %>% 
    as_tibble() %>% 
    clean_names() %>% #glimpse()
    rename(etag = e_tag) %>% 
    mutate(
      etag = str_remove_all(etag, '\"'),
      last_modified = ymd(as.Date(last_modified)),
      size = as.integer(size)
    ) 
  
  bkt_objects %>% glimpse()
  bkt_objects %>% head()
  
  #bkt_objects %>% View()
  
  #View(bkt_objects)
  
  # Identify all prefixes (subfolders)
  bkt_objects %>% 
    s3_unpack_keys() %>% 
    glimpse()
  
  
  # Get object key
  bkt_obj_key <- bkt_objects %>% 
    filter(sys_data_type == "processed",
           sys_use_case == "hfr",
           sys_data_route == 'outgoing',
           str_detect(str_to_lower(sys_data_object), "^hfr_.*_tableau_.*.csv$")) %>% 
    arrange(desc(last_modified)) %>% 
    pull(key) %>% 
    first()
    
  
  # Get bucket as an object
  bkt_obj <- get_object(
    bucket = bkt_name,
    object = bkt_obj_key
  )
  
  bkt_obj %>% 
    rawConnection(open = "r") %>% 
    vroom::vroom()

  
  # Read data from S3
  s3read_using(FUN = vroom::vroom, 
               bucket = bkt_name, 
               object = "ddc/uat/processed/hfr/outgoing/hfr_2021_02_Tableau_2020-12-17.csv")
  
  
# PAWS - Package for Amazon Web Services in R ----
  
  # Setting Credentials with Custom Service Config
  #svc <- paws::svc()
  
  # S3 Client
  s3 <- paws::s3()

  # Buckets list
  bkts_list <- s3$list_buckets()
  
  bkts_list$Owner
  
  bkts_list$Buckets %>% length()
  
  buckets <- bkts_list$Buckets %>% 
    map_df(~tibble(name = .$Name, 
                   creation_date = .$CreationDate))

  buckets
  
  # Select backet
  bkt_name <- buckets %>% 
    filter(str_detect(name, "^g.*aid$")) %>% 
    pull(name) %>% 
    first()
  
  bkt_name
  
  # Retrieve objects from bucket
  bkt_objects <- s3$list_objects_v2(
    Bucket = bkt_name,
    Prefix = "ddc/uat/processed/hfr/outgoing",
    MaxKeys = 1000)
  
  bkt_objects$Name
    
  bkt_objects <- bkt_objects$Contents %>% 
    map_df(~tibble(key = .$Key,
                   last_modified = .$LastModified,
                   etag = .$ETag,
                   size = .$Size))
  
  bkt_objects %>% glimpse()
  
  bkt_objects <- bkt_objects %>% 
    filter(size > 0) %>% 
    arrange(desc(size)) 
  
  bkt_objects %>% View()
  
  # Download object by key
  obj <- "./ddc/dev/processed/hfr/incoming/Wide_Baseline_No_Errors_Wide.xlsx"
  #obj <- "Wide_Baseline_No_Errors_Wide.xlsx"
  #obj <- "13b4178b4b55dd992e56e1a479842e14"
  
  s3_obj <- s3$get_object(
    Bucket = bkt_name,
    Key = obj
  )
  
  # Create S3 Bucket ----
  
  bkt_new <- "gov-usaid-hfr"

  s3$create_bucket(Bucket = bkt_new)  
  
  s3$list_buckets()$Buckets %>% 
    map_df(~tibble(name = .$Name,
                   creation_date = .$CreationDate)) %>% 
    filter(name == bkt_new)
  
  # Transfer Sample file ----
  
  # Sample data
  df <- tibble(
    id = seq(1, 100, 1),
    indicator = c(rep("hello", 25), rep("world", 50), rep("siei", 25)),
    performance = runif(100, 0, 100)
  )
  
  # file name / location
  file_csv <- "./Data/HFR_Random_Performance.csv"
  
  # Save data to file
  write_csv(df, file_csv)  
  
  # Test file content
  file_dta <- read_csv(file_csv) 
  
  file_dta
  
  # file connection
  con_rb <- file(file_csv, "rb")
  
  # file transfer as binary data
  trans_raw <- readBin(con_rb, what = "raw", n = file.size(file_csv))

  # Transfer file
  s3$put_object(
    Body = trans_raw,
    Bucket = bkt_new,
    Key = basename(file_csv)
  )
  
  # Transfer file with function
  hfr_file_transfer(s3_client = s3, 
                    s3_bucket = bkt_new,
                    local_file = file_csv, 
                    s3_object_name = "HFR_Random_Performance_new.csv")
  
  # Get list of objects
  ddc_objects(
    s3_client = s3, 
    s3_bucket = bkt_new
  ) 
  
  
  # Download files ----
  s3_file <- s3$get_object(
    Bucket = bkt_new,
    Key = basename(file_csv)
  )
  
  s3_file$ContentLength
  s3_file$ETag
  s3_file$LastModified
  s3_file$Body
  
  # write content to file
  writeBin(s3_file$Body, 
           con = file.path(dataout, "HFR_Random_Performance_download.csv"))
  
  # Dowload and write content to file
  ddc_download(s3, bkt_new, basename(file_csv), file.path(dataout, "HFR_Random_Performance_download.csv"))
  
  # Clean up
  
  # delete file
  s3$delete_object(Bucket = bkt_new, Key = sample_file)
  
  # delete bucket
  s3$delete_bucket(Bucket = bkt_new)

  close(file_rb)
  file.remove(sample_file)
  file.remove(sample_file2)
  