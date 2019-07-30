## Data gathering

# SET UP -----------------------------------------------------------------------
# load packages
library(parallel)
library(data.table)
library(pryr)
library(zip)  # to use zip() on windows

# store the memory used by R
startingmemory <- mem_used()

# fixed variables
full.base.url <- "http://datacommons.s3.amazonaws.com/subsets/td-20140324/contributions.fec.1990.csv.zip"
kOutputPath <- "./data/fec.csv"
kStartYear <- 1990
kEndYear <- 2014
kExDir <- "./data"

# BUILD URLS -------------------------------------------------------------------
# parse base url
stripped.base.url <- gsub("1990.csv.zip", "", full.base.url)

# build urls
years.character <- as.character(seq(from = kStartYear, to = kEndYear, by = 2))
data.urls <- paste0(stripped.base.url, years.character, ".csv.zip")

# DOWNLOAD AND APPEND FILES ----------------------------------------------------
# define number of cores
ncores <- parallel::detectCores()

# download files from urls in parallel and append them
time.elapsed <- 
  system.time(
    mclapply(data.urls, FUN = function(url) fwrite(fread(paste0("curl ", url, " | funzip"), fill = TRUE), file = kOutputPath, append = T), mc.cores = ncores)
  )

# profiling
time.elapsed
mem_used() - startingmemory

# zip file
zip(zipfile = "./data/fec.csv.zip", files = kOutputPath, compression_level = 1)


## Data storage and databases

# SET UP -----------------------------------------------------------------------
# load packages
library(DBI)
library(data.table)
library(rvest)
library(RSQLite)
library(pryr)

startingmemory <- mem_used()

# create file and initiate the database
con.fec <- dbConnect(RSQLite::SQLite(), "./data/fec.sqlite")

# DOWNLOADING DATA -------------------------------------------------------------
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

# read column names of the fec data set
col.names <- colnames(fread("./data/fec.csv", nrows = 0))

# define column classes explicitly
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

# DEFINING FUNCTIONS -----------------------------------------------------------

# a function to write csv files to sqlite database
csv2sqlite <- function(csv.file, sqlite.file, table.name, N = 1000000) {
  
  # connect to database
  con <- dbConnect(RSQLite::SQLite(), dbname=sqlite.file)
  
  # read the data from the beginning of the CSV
  iter <- 0
  rowsread <- 0
  while(TRUE) {
    cat("Iteration:", iter, "\n")
    # skip = iter*N + 1.  the "+1" is because we can now skip the header
    # the "%>% as.data.frame" is because dbWriteTable is a bit fussy on data structure
    df <- try(fread(csv.file, 
                    header = TRUE, 
                    col.names = col.names, 
                    colClasses= class.list, 
                    skip = iter*N+1, 
                    nrows = N) %>% as.data.frame, silent = TRUE)
    if (class(df[1] == "try-error") {
      cat("Out of rows...\n")
      break
    }
    if (nrow(df) == 0) {
      cat("Out of rows...\n")
      break
    }
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
  
  # close the open connection again
  dbDisconnect(con)
}

# WRITE DATA -------------------------------------------------------------------
# add tables to database
csv2sqlite(csv.file = "./data/fec.csv", sqlite.file = "./data/fec.sqlite", table.name = "donations")
dbWriteTable(con.fec, "transactiontypes", transactiontypes, overwrite = TRUE)
dbWriteTable(con.fec, "industrycodes", industrycodes, overwrite = TRUE)

# create indicies for all tables on the relevant columns
transactions.index <- "CREATE INDEX idx_transactiontypes ON transactiontypes (transaction_type, transaction_description)"
dbExecute(con.fec, transactions.index)

codes.index <- "CREATE INDEX idx_industrycodes ON industrycodes (contributor_category, industry)"
dbExecute(con.fec, codes.index)

donations.index <- "CREATE INDEX idx_donations ON donations (amount, contributor_type, contributor_category, cycle, recipient_name, recipient_type, seat)"
dbExecute(con.fec, donations.index)

# FINISH UP --------------------------------------------------------------------
# disconnect from the database
dbDisconnect(con.fec)


## Data aggregation 

# SET UP -----------------------------------------------------------------------
# load packages
library(DBI)
library(pryr)
library(data.table)

mem_used()

# connect to database
con.fec <- dbConnect(RSQLite::SQLite(), "./data/fec.sqlite")

# TABLE 1 ----------------------------------------------------------------------
# define queries
query.contribtions.oilgas <- 
  "SELECT amount FROM donations WHERE transaction_type IN (SELECT transaction_type FROM transactiontypes WHERE transaction_description = 'Contribution to political committees (other than Super PACs and Hybrid PACs) from an individual, partnership or limited liability company') AND contributor_category IN (SELECT contributor_category FROM industrycodes WHERE industry = 'OIL & GAS') AND amount > 0"
query.contribtions.all <- 
  "SELECT amount FROM donations WHERE transaction_type IN (SELECT transaction_type FROM transactiontypes WHERE transaction_description = 'Contribution to political committees (other than Super PACs and Hybrid PACs) from an individual, partnership or limited liability company') AND amount > 0"

# add up query results using vectorization
time.oilgas <- 
  system.time(
    dt <- dbGetQuery(con.fec, query.contribtions.oilgas)
  )
contributions.oilgas <- sum(as.numeric(dt$amount))
rm(dt)

time.all <- 
  system.time(
    dt <- dbGetQuery(con.fec, query.contribtions.all)
  )
contributions.all <- sum(as.numeric(dt$amount))
rm(dt)

# create data frame with formatted numbers
query.result <- data.frame(Total = format(contributions.oilgas, scientific = FALSE, digits = 2), Relative = format(contributions.oilgas / contributions.all, scientific = FALSE, digits = 2))

# profiling
time.oilgas
time.all

# create table in database
dbWriteTable(con.fec, "contributions", query.result, overwrite = TRUE)

# cleaning up
rm(query.result, contributions.all, contributions.oilgas, time.all, time.oilgas, query.contribtions.all, query.contribtions.oilgas)

# TABLE 2 ----------------------------------------------------------------------
# predetermine the table dimensions
dt.final <- data.table("1990" = rep(NA, 5),
                       "1992" = rep(NA, 5),
                       "1994" = rep(NA, 5),
                       "1996" = rep(NA, 5),
                       "1998" = rep(NA, 5),
                       "2000" = rep(NA, 5),
                       "2002" = rep(NA, 5),
                       "2004" = rep(NA, 5),
                       "2006" = rep(NA, 5),
                       "2008" = rep(NA, 5),
                       "2010" = rep(NA, 5),
                       "2012" = rep(NA, 5),
                       "2014" = rep(NA, 5))

# define queries
query.all.base <- 
  "SELECT recipient_name, amount, seat, recipient_type, cycle FROM donations WHERE seat = 'federal:president' AND recipient_type == 'P' AND amount > 0 AND cycle == "
query.unique.base <- 
  "SELECT DISTINCT recipient_name, seat, recipient_type, cycle FROM donations WHERE seat = 'federal:president' AND recipient_type == 'P' AND amount > 0 AND cycle == "

# fixed variables
kStartYear <- 1990
kEndYear <- 2014

# build urls
years.character <- as.character(seq(from = kStartYear, to = kEndYear, by = 2))
query.recipients.all <- paste0(query.all.base, years.character)
query.recipients.unique <- paste0(query.unique.base, years.character)

# create the data table
time.loop <- 
  system.time(
    for (i in 1:length(query.recipients.all)) {
      dt.all <- dbGetQuery(con.fec, query.recipients.all[i])
      dt.unique <- dbGetQuery(con.fec, query.recipients.unique[i])
      dt.unique$amount <- 0
      for (x in 1:nrow(dt.unique)) {
        dt.unique[x, 5] <- sum(subset(dt.all, recipient_name == dt.unique[x, 1])$amount)
        dt.unique <- dt.unique[order(-dt.unique$amount), ]
        dt.unique <- dt.unique[1:5,]
        dt.cycle <- dt.unique[1, 4]
        dt.final[, which(colnames(dt.final) == dt.cycle)] <- dt.unique$recipient_name
      }
    }
  )

# profiling
time.loop

# create table in database
dbWriteTable(con.fec, "recipients", dt.final, overwrite = TRUE)

# cleaning up
rm(dt.final, dt.unique, dt.all, query.recipients.all, query.recipients.unique, kStartYear, kEndYear, query.all.base, query.unique.base, i, dt.cycle, x, time.loop, years.character)

# TABLE 3 ----------------------------------------------------------------------
# predetermine the table dimensions
dt.final <- data.table('Year' = c('1990', '1992', '1994', '1996', '1998', '2000', '2002', '2004', '2006', '2008', '2010', '2012', '2014'),
                       'BUSINESS ASSOCIATIONS' = rep(NA, 13),
                       'PUBLIC SECTOR UNIONS' = rep(NA, 13),
                       'INDUSTRIAL UNIONS' = rep(NA, 13),
                       'NON-PROFIT INSTITUTIONS' = rep(NA, 13),
                       'RETIRED' = rep(NA, 13))
dt.final <- as.data.frame(dt.final)

# download data
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
industrycodes <- data.frame(lapply(industrycodes, as.character), stringsAsFactors = FALSE)
industrynames <- c('BUSINESS ASSOCIATIONS', 'PUBLIC SECTOR UNIONS', 'INDUSTRIAL UNIONS', 'NON-PROFIT INSTITUTIONS', 'RETIRED')

# define query
query.all.base <- 
  "SELECT amount, contributor_type, contributor_category, cycle FROM donations WHERE amount < 1000 AND contributor_type == 'I' AND cycle = "

# fixed variables
kStartYear <- 1990
kEndYear <- 2014

# build urls
years.character <- as.character(seq(from = kStartYear, to = kEndYear, by = 2))
query.industry.all <- paste0(query.all.base, years.character)

# performing aggregation
i <- 1
time.industry <- 
  system.time(
    for (i in 1:length(query.industry.all)) {
      dt.all <- dbGetQuery(con.fec, query.industry.all[i])
      for (name in industrynames) {
        list <- industrycodes[which(industrycodes$industry == name), ]
        amount <- 0
        for (x in 1:nrow(list)) {
          code <- list[x,2]
          amount <- amount + sum(dt.all[which(dt.all$contributor_category == code), ]$amount)
        }
        dt.final[i, which(colnames(dt.final) == name)] <- amount
      }
      i <- i + 1
    }
  )

# profiling
time.industry

# create table in database
dt.final <- as.data.table(dt.final)
dbWriteTable(con.fec, "industries", dt.final, overwrite = TRUE)

# cleaning up
rm(col.classes, col.names, data_url, i, industrynames, kEndYear, kStartYear, name, query.all.base, query.industry.all, time.industry, x, years.character, dt.all, dt.final, industrycodes, list, code, amount)

# FINISH UP --------------------------------------------------------------------
# disconnect from the database
dbDisconnect(con.fec)


## Visualization

# SET UP -----------------------------------------------------------------------
# load packages
library(DBI)
library(ggplot2)
library(dplyr)
library(tidyr)
library(knitr)
library(kableExtra)
library(viridis)

con.fec <- dbConnect(RSQLite::SQLite(), "./data/fec.sqlite")

dt <- dbGetQuery(con.fec, "SELECT * FROM industries")

dt <- dt %>% mutate(Amount = log(Amount))

# SPAGHETTI CHART --------------------------------------------------------------
# rearrange the data to long format
dt.long <- gather(dt, Name, Amount, 'BUSINESS ASSOCIATIONS':'RETIRED', factor_key = FALSE)

dt.long %>%
  select(Year, Name, Amount) %>%
  arrange(Name) %>%
  kable() %>%
  kable_styling(bootstrap_options = "striped", full_width = F)

dt.long$Year <- as.numeric(dt.long$Year)

# create plot
spaghettichart <- dt.long %>%
  ggplot( aes(x = Year, y = Amount)) +
  geom_line(data = dt.long %>% dplyr::select(-Name), aes(group = 1), color="grey", size=0.5, alpha=0.5) +
  geom_line(aes(color=Name), color="#69b3a2", size=1.2 )+
  scale_color_viridis(discrete = TRUE) +
  theme_gray() +
  theme(
    legend.position="none",
    plot.title = element_text(size=14),
    panel.grid = element_blank()
  ) +
  ggtitle("A spaghetti chart of industries") +
  facet_wrap(~Name)

# print chart
print(spaghettichart)

# OUTPUTTING PLOT --------------------------------------------------------------
pdf("./visuals/spaghettichart.pdf")
print(spaghettichart)
dev.off()

# FINISH UP --------------------------------------------------------------------
# disconnect from the database
dbDisconnect(con.fec)
