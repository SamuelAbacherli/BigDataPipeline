# set working directory
setwd("/Users/samu_hugo/Google Drive/Career/Education/Universität St. Gallen/3_Spring 2019/Big Data Analytics for R & Python (4)/Assignment 1/group-examination-localbini/code")
# setwd("C:/Users/Raphael/Desktop/Big Data/group-examination-localbini/code")

# SET UP --------------------
# load packages
library(DBI)
library(data.table)
library(rvest)
library(RSQLite)
library(pryr)

mem_used()

# create file and initiate the database
con.fec <- dbConnect(RSQLite::SQLite(), "../data/fec.sqlite")

# DOWNLOADING DATA ----------
# scraping transaction type data
dictionary <- read_html("https://classic.fec.gov/finance/disclosure/metadata/DataDictionaryTransactionTypeCodes.shtml")
transactiontypes <- na.omit(dictionary %>%
  html_nodes("table") %>%
  .[[1]] %>%
  html_table(header = TRUE, trim = TRUE, fill = TRUE))
names(transactiontypes) <- c("transaction_type", "transaction_description")

# download industry code data
data_url <- "http://assets.transparencydata.org.s3.amazonaws.com/docs/catcodes.csv"
col.names <- c("source",
               "contributor_category", 
               "name", 
               "industry", 
               "order")
col.classes <- c(V1 = "factor",
                 V2 = "factor",
                 V3 = "character",
                 V4 = "character",
                 V5 = "factor")
industrycodes <- fread(data_url,
                       nrows = 1000000,
                       header = FALSE,
                       col.names = col.names,
                       colClasses = col.classes)
industrycodes <- industrycodes[-1,]

col.names <- colnames(fread("../data/fec.csv", nrows = 0))

# import a few lines of the data, setting the column classes explicitly
class.list <- c("integer",
                 "factor",
                 "factor",
                 "factor",
                 "character",
                 "factor",
                 "factor",
                 "factor",
                 "integer",
                 "date",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor",
                 "factor")

col.classes <- setNames(class.list, col.names)

csv2sqlite <- function(csv.file, sqlite.file, table.name, N = 1000000) {
  
  # Connect to database. Change this line if you want to write to
  # other database backends e.g. RPostgres
  con <- dbConnect(RSQLite::SQLite(), dbname=sqlite.file)
  
  # Read the data from the beginning of the CSV
  # Remember to skip the header
  iter <- 0
  rowsread <- 0
  while(TRUE) {
    cat("Iteration:", iter, "\n")
    # skip = iter*N + 1.  the "+1" is because we can now skip the header
    # The "%>% as.data.frame" is because dbWriteTable is a bit fussy on data structure
    df <- fread(csv.file, 
                header = TRUE, 
                col.names = col.names, 
                colClasses= class.list, 
                skip = iter*N+1, 
                nrows = N) %>% as.data.frame
    # if (nrow(problems(df)) > 0) {
    #   print(problems(df))
    # }
    if (nrow(df) == 0) {
      cat("Out of rows...\n")
      break
    }
    # if ((nrow(df) == 1) & (sum(!(is.na(df))) == 0)) {
    #   # this is a workaround for a bug in readr::read_csv when col_types is specified
    #   # and skip > #rows in file.  readr incorrectly returns a data.frame with a single row
    #   # of all NA values.
    #   cat("Out of rows (bugged)...\n")
    #   break
    # }
    rowsread <- rowsread + nrow(df)
    cat("Rows read:", nrow(df), "  total so far:", rowsread, "\n")
    if (iter == 0) {
      dbWriteTable(con, table.name, df, overwrite=TRUE)
    } else {
      dbWriteTable(con, table.name, df, append=TRUE)
    }
    iter <- iter + 1
  }
  cat("Total Read:", rowsread, "\n")
  
  dbDisconnect(con)
}

# WRITE DATA --------------
# add tables to database
csv2sqlite(csv.file = "../data/fec.csv", sqlite.file = "../data/fec.sqlite", table.name = "donations")
dbWriteTable(con.fec, "transactiontypes", transactiontypes, overwrite = TRUE)
dbWriteTable(con.fec, "industrycodes", industrycodes, overwrite = TRUE)

transactions.index <- "CREATE INDEX idx_transactiontypes ON transactiontypes (transaction_type, transaction_description)"
dbExecute(con.fec, transactions.index)
# transactions.drop <- "DROP INDEX idx_transactiontypes"
# dbExecute(con.fec, transactions.drop)

codes.index <- "CREATE INDEX idx_industrycodes ON industrycodes (contributor_category, industry)"
dbExecute(con.fec, codes.index)
# codes.drop <- "DROP INDEX idx_industrycodes"
# dbExecute(con.fec, codes.drop)

donations.index <- "CREATE INDEX idx_donations ON donations (amount, contributor_type, contributor_category, cycle, recipient_name, recipient_type, seat)"
dbExecute(con.fec, donations.index)
# donations.drop <- "DROP INDEX idx_donations"
# dbExecute(con.fec, donations.drop)

# FINISH UP --------------
# disconnect from the database
dbDisconnect(con.fec)
