# setwd("C:/Users/Raphael/Desktop/Big Data/group-examination-localbini/code")
# setwd("/Users/samu_hugo/Google Drive/Career/Education/Universität St. Gallen/3_Spring 2019/Big Data Analytics for R & Python (4)/Assignment 1/group-examination-localbini/code")

# clean the environment 
rm(list = ls())
gc()

# install.packages(c("ff", "ffbase", "biglm"))

# load packages
library(ggplot2)
library(DBI)
library(ff)
library(ffbase)
library(pryr)
library(plyr)
library(base)
library(biglm)
library(kableExtra)
library(knitr)
library(scales)
library(gridExtra)
library(broom)

# directory for ff chunks:
options(fftempdir = "../data/ffchunks")

# read in the data:
donations <- read.table.ffdf(file = "../data/fec.csv", 
                             sep = ",", 
                             dec = ".",
                             VERBOSE = TRUE, 
                             header = TRUE, 
                             next.rows = 100000,
                             fill = NA)

# create subset with relevant variables for further analysis:
subset.donations <- subset.ffdf(donations, select = c("amount",
                                           "date",
                                           "recipient_ext_id",
                                           "recipient_name",
                                           "recipient_party",
                                           "recipient_type",
                                           "candidacy_status",
                                           "seat",
                                           "seat_status",
                                           "seat_result"))

rm(donations)

## PREPARING THE DATA

# select only politicians:
politicians <- sample(ffwhich(subset.donations, subset.donations$recipient_type == "P"))
politicians.ff <- as.ffdf(subset.donations[politicians, ])
rm(politicians)

# remove negative contributions:
droplevels.ff(politicians.ff$amount)
politicians.ff$amount <- clone(politicians.ff$amount, ramclass = "numeric")
positives <- sample(politicians.ff$amount > 0)
as.ffdf(politicians.ff[positives, ])
rm(positives)

# received contributions by recipient ID
contributions <- as.ffdf(ddply(politicians.ff[], ~ recipient_ext_id, summarise, contributions.sum = sum(amount)))
orderofcontributions <- addfforder(politicians.ff$recipient_ext_id) # correct order based on politicians.ff
contributions <- contributions[ffmatch(orderofcontributions, contributions$recipient_ext_id), ]
rm(orderofcontributions)

# removing repetitive entries of politicians and adding the sum of their received contributions:
duplicates <- sample(duplicated(as.character(politicians.ff$recipient_ext_id)))
politicians.ff$amount <- clone(contributions$contributions.sum, ramclass = "numeric")
politicians.ff <- as.ffdf(politicians.ff[!duplicates, ])
rm(duplicates)
gc()

# transforming to numeric dummy variable (seat_result = 1, if seat is one):
politicians.ff$seat_result <- ffifelse(as.character(politicians.ff$seat_result) == "W", 1, 0)

# seat_status = 1, if seat is incumbent:
politicians.ff$seat_status <- ffifelse(as.character(politicians.ff$seat_status) == "I", 1, 0)

# new columns to differntiate between democratic, republican and other participants:
politicians.ff$democrat <- ffifelse(as.character(politicians.ff$recipient_party) == "D", 1, 0)
politicians.ff$republican <- ffifelse(as.character(politicians.ff$recipient_party) == "R", 1, 0)

## DESCIPTIVE STATISTICS

as.character.ff(politicians.ff$date)
politicians.ff$years <- with(politicians.ff[c("date")], substr(date, 1, 4))
politicians.ff$years <- clone(politicians.ff$years, ramclass = "numeric")
yearly.contributions <- as.ffdf(ddply(politicians.ff[], ~ years, summarise, contributions.sum = sum(amount)))
years. <- levels(politicians.ff$years[])

as.character(subset.donations$date)
subset.donations$years <- with(subset.donations[c("date")], substr(date, 1, 4))
subset.donations$years <- clone(subset.donations$years, ramclass = "numeric")
subset.donations$amount <- clone(subset.donations$amount, ramclass = "numeric")
total.contributions <- as.ffdf(ddply(subset.donations[], ~ years, summarise, total.sum = sum(amount)))

# plots 
# 1990 to 2006
early.years <- ggplot(data = yearly.contributions[], aes(years[], contributions.sum[])) + 
                  geom_area(color = "blue", fill = "lightblue") +
                  labs(x = "Year", y = "Contributions", title = "Contributions to Politicians 1989 to 2006") +
                  scale_x_continuous(breaks = yearly.contributions$years[], labels = years., limits = c(1, 18)) +
                  scale_y_continuous(labels = dollar) +
                  theme(axis.text.x = element_text(angle = 90, vjust = 0.5),
                        axis.text.y = element_text(angle = 90, hjust = 0.5))

# overall
overall <- ggplot(data = yearly.contributions[], aes(years[], contributions.sum[])) + 
              geom_area(color = "blue", fill = "lightblue") +
              labs(x = "Year", y = "", title = "Contributions to Politicians 1989 to 2013") +
              scale_x_continuous(breaks = yearly.contributions$years[], labels = years., limits = c(1, 26)) +
              scale_y_continuous(labels = dollar) +
              theme(axis.text.x = element_text(angle = 90, vjust = 0.5),
                    axis.text.y = element_text(angle = 90, hjust = 0.5))

pdf("../data/contributions.pdf", width = 16, height = 5)
grid.arrange(early.years, overall, ncol = 2)
dev.off()

## REGRESSION MODEL

# regression model:
regression <- bigglm.ffdf(seat_result ~ seat_status + amount, 
                          data = politicians.ff,
                          family = binomial(),
                          chunksize = 1000)

# table
out <- tidy(regression)
out %>%
  kable() %>%
  kable_styling()


# ------------------------------------------------------------------------------

# Saving / loading previously saved ffdf files:
dir.create("../data/ffdonations")
save.ffdf(donations, dir = "../data/ffdonations", overwrite = T)

gc()
load.ffdf("../data/ffdonations")


