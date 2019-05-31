# set working directory
setwd("/Users/samu_hugo/Google Drive/Career/Education/Universität St. Gallen/3_Spring 2019/Big Data Analytics for R & Python (4)/Assignment 1/group-examination-localbini/code")

# SET UP ----------------
# load packages
library(DBI)
library(pryr)
library(data.table)

mem_used()

# connect to database
con.fec <- dbConnect(RSQLite::SQLite(), "../data/fec.sqlite")

# TABLE 1 -----------
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
time.oilgas
contributions.oilgas <- sum(as.numeric(dt$amount))

mem_change(rm(dt))

time.all <- 
  system.time(
    dt <- dbGetQuery(con.fec, query.contribtions.all)
  )
time.all
contributions.all <- sum(as.numeric(dt$amount))

mem_change(rm(dt))

# create data frame with formatted numbers
query.result <- data.frame(Total = format(contributions.oilgas, scientific = FALSE, digits = 2), Relative = format(contributions.oilgas / contributions.all, scientific = FALSE, digits = 2))

# create table in database
dbWriteTable(con.fec, "contributions", query.result, overwrite = TRUE)

# TABLE 2 -----------
# 

dt.final <- data.table("1990" = character(),
                       "1992" = character(),
                       "1994" = character(),
                       "1996" = character(),
                       "1998" = character(),
                       "2000" = character(),
                       "2002" = character(),
                       "2004" = character(),
                       "2006" = character(),
                       "2008" = character(),
                       "2010" = character(),
                       "2012" = character(),
                       "2014" = character())
dt.final <- rbind(dt.final, c(1,2,3,4,5), fill = TRUE)
dt.final <- dt.final[,-14]


  query.all.base <- 
    "SELECT recipient_name, amount, seat, recipient_type, cycle FROM donations WHERE amount > 0 AND recipient_type == 'P' AND seat = 'federal:president' AND cycle == "
  query.unique.base <- 
    "SELECT DISTINCT recipient_name, seat, recipient_type, cycle FROM donations WHERE amount > 0  AND recipient_type == 'P' AND seat = 'federal:president' AND cycle == "
  
  # fix vars
  kStartYear <- 1990
  kEndYear <- 2014
  
  # BUILD URLS -----------
  # build urls
  years.character <- as.character(seq(from = kStartYear, to = kEndYear, by = 2))
  query.recipients.all <- paste0(query.all.base, years.character)
  query.recipients.unique <- paste0(query.unique.base, years.character)
  
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

# create table in database
dbWriteTable(con.fec, "recipients", dt.final, overwrite = TRUE)

# TABLE 3 -----------

dt.final <- data.table('Year' = c('1990', '1992', '1994', '1996', '1998', '2000', '2002', '2004', '2006', '2008', '2010', '2012', '2014'),
                       'BUSINESS ASSOCIATIONS' = rep(NA, 13),
                       'PUBLIC SECTOR UNIONS' = rep(NA, 13),
                       'INDUSTRIAL UNIONS' = rep(NA, 13),
                       'NON-PROFIT INSTITUTIONS' = rep(NA, 13),
                       'RETIRED' = rep(NA, 13))
dt.final <- as.data.frame(dt.final)

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

query.all.base <- 
  "SELECT amount, contributor_type, contributor_category, cycle FROM donations WHERE amount < 1000 AND contributor_type == 'I' AND cycle = "

# fix vars
kStartYear <- 1990
kEndYear <- 2014

# BUILD URLS -----------
# build urls
years.character <- as.character(seq(from = kStartYear, to = kEndYear, by = 2))
query.industry.all <- paste0(query.all.base, years.character)

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
time.industry

# create table in database
dt.final <- as.data.table(dt.final)
dbWriteTable(con.fec, "industries", dt.final, overwrite = TRUE)

