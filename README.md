# README: Big Data Pipeline - Campaign Contributions

## General remarks

This project aims at developing a simple data pipeline that handles a moderately sized real-world data set.


# Data gathering

The data pipeline processes [campaign finance](https://en.wikipedia.org/wiki/Campaign_finance) data from the [US Federal Election Commission (FEC)](https://www.fec.gov/). The data is given at the level of official individual donation records. That is, the records tell you which person/organization donated when how much money to which campaign/committee. 

First, the pipeline downloads all the individual files by *Contribution Year* for contributions at the federal level.

The script downloads the files, unzips them, deletes the zip-files (and whatever additional files are left over from unzipping), stacks all observations together in one large csv-file `fec.csv`, and deletes the redundant individual csv files. Then, it compresses the `fec.csv` file and stores the resulting `fec.csv.zip`. The script includes appraoches to run as efficient as possible (fast and low memory use, and using profiling functions to measure this).


# Data storage

Subsequently, the data pipeline sets up an SQLite database called `fec.sqlite`. The database contains the following three tables:

- `donations` (based on `fec.csv.zip`)
- `transactiontypes` (based on the FEC's [data dictionary](https://classic.fec.gov/finance/disclosure/metadata/DataDictionaryTransactionTypeCodes.shtml))
- `industrycodes` (based on [this CSV file](http://assets.transparencydata.org.s3.amazonaws.com/docs/catcodes.csv))

The script performs webscraping and takes care of downloading the data for `industrycodes` and `transactiontypes` using the rvest package. The database is optimized by setting the right data types ('field' types) for the columns and by adding indices.


# Data aggregation

The data pipline then proceeds to query data from the `fec.sqlite`-database and generates the following three data aggregation outputs. 

 1. A table that shows the total (sum) amount of contributions from the `OIL & GAS`-industry per year. Once in absolute terms and once in relative terms (contributions from the `OIL & GAS`-industry as percentage of contributions from *all* industries) considering only 'contributions to political committees (other than Super PACs and Hybrid PACs) from an individual, partnership or limited liability company' in this analysis'.
 
 2. A table that shows a ranking of the top-five politicians/candidates in terms of the overall donations received as part of a *presidential* campaign, for each election cycle in the data, excluding negative donation amounts.

 3. A table showing the total number of small donations (< USD1000) from individual contributors (not parties or corporations) associated with one of the following industries (`BUSINESS ASSOCIATIONS`, `PUBLIC SECTOR UNIONS`, `INDUSTRIAL UNIONS`, `NON-PROFIT INSTITUTIONS`, `RETIRED`) per year (years in rows).

All table outputs are dynamically created markdown tables with the `kable()` function provided in the `knitr` package. 


# Data visualization

The aggregated data created in (3) is visualized using the ggplot2 package. The visualization communicates how the number of small contributions from individuals has changed over the years for the five different 'industries', taking into consideration the general recommendations in [Schwabish, Jonathan A. (2014): An Economistʹs Guide to Visualizing Data. Journal of Economic Perspectives. 28(1):209‐234](https://www.aeaweb.org/articles?id=10.1257/jep.28.1.209).
