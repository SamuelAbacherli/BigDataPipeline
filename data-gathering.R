# set working directory
# setwd("/Users/samu_hugo/Google Drive/Career/Education/Universität St. Gallen/3_Spring 2019/Big Data Analytics for R & Python (4)/Assignment 1/group-examination-localbini/code")
# setwd("C:/Users/Raphael/Desktop/Big Data/group-examination-localbini/code")

# SET UP -----------------
# load packages
library(parallel)
library(data.table)
library(rvest) 
library(httr)
library(pryr)
library(zip) # to use zip() on windows

mem_used()

# fix vars
full.base.url <- "http://datacommons.s3.amazonaws.com/subsets/td-20140324/contributions.fec.1990.csv.zip"
kOutputPath <- "../data/fec.csv"
kStartYear <- 1990
kEndYear <- 2014
kExDir <- "../data"
csv.list <- c()

# BUILD URLS -----------
# parse base url
stripped.base.url <- gsub("1990.csv.zip", "", full.base.url)
# build urls
years.character <- as.character(seq(from = kStartYear, to = kEndYear, by = 2))
data.urls <- paste0(stripped.base.url, years.character, ".csv.zip")


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------


# FETCH AND STACK CSVS ----------------
time.elapsed.fread <- 
  system.time(
    for (url in data.urls) {
      temp.file <- tempfile()
      download.file(url, temp.file)  # download zip file
      csv.file <- unzip(temp.file, exdir = kExDir)  # unzip file
      csv.list <- c(csv.list, csv.file[1])  # save files into list
      unlink(csv.file[2])  # delete README file
      unlink(temp.file)  # delete zip file
    }
  )+
  system.time(
    # distinguishing between Windows and Mac / Linux, since on Windows multiprocessing is not supported in R 
    # so we give a single processor, in which case mclapply calls fall back on lapply:
    mclapply(csv.list, FUN = function(file) fwrite(fread(file, fill = TRUE), file = kOutputPath, append = T), 
             mc.cores = ifelse(Sys.info()["sysname"] == "Windows", 1, ncores))  # parallel processing of read / writing 
  )+
  system.time(
    file.remove(paste0(kExDir, "/contributions.fec.", years.character, ".csv")) # delete csv files
  )

time.elapsed.fread # downloading all files with fread / fwrite took less than 9 minutes

# zip fec.csv file
zip(zipfile = "../data/fec.csv.zip", files = kOutputPath)
mem_used()


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

library(parallel)
library(data.table)


# check memory usage
memory.changed.new <- 
  mem_change(
    mclapply(data.urls, FUN = function(url) fwrite(fread(paste0("curl ", url, " | funzip"), fill = TRUE), file = kOutputPath, append = T), mc.cores = ncores)
  )

ncores <- parallel::detectCores()  # define number of cores

# check time used
time.elapsed.new <- 
  system.time(
    mclapply(data.urls, FUN = function(url) fwrite(fread(paste0("curl ", url, " | funzip"), fill = TRUE), file = kOutputPath, append = T), mc.cores = ncores)
  )

time.elapsed.new # downloading all files with fread/fwrite took about 35 minutes

# zip fec.csv file
zip(zipfile = "../data/fec.csv.zip", files = kOutputPath)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------