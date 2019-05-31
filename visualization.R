# set working directory
# setwd("/Users/samu_hugo/Google Drive/Career/Education/Universität St. Gallen/3_Spring 2019/Big Data Analytics for R & Python (4)/Assignment 1/group-examination-localbini/code")

# install packages
# install.packages('ggplot2')
# install.packages('gridExtra')
# install.packages('ggvis')
# install.packages('kableExtra')
# install.packages('viridis')

# SET UP --------------------
# load packages
library(ggplot2)
library(dplyr)
library(tidyr)
library(knitr)
library(kableExtra)
library(viridis)

# Visualize (by using `ggplot2`) the aggregated data created in the last step (3) 
# of the previous task. The visualization should communicate how the number of small
# contributions from individuals has changed over the years for the five different 
# 'industries'. Take into consideration the general recommendations in [Schwabish, 
# Jonathan A. (2014): An Economistʹs Guide to Visualizing Data. Journal of Economic 
# Perspectives. 28(1):209‐234](https://www.aeaweb.org/articles?id=10.1257/jep.28.1.209) 
# and aim for a visualization of the type that Schwabish calls the "Spaghetti Chart" 
# (p. 219ff). Your final figure should be ready to be published in a professionally 
# crafted research report. 

con.fec <- dbConnect(RSQLite::SQLite(), "../data/fec.sqlite")

dt <- dbGetQuery(con.fec, "SELECT * FROM industries")

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# SPAGHETTI CHART

dt.long <- gather(dt, Name, Amount, 'BUSINESS ASSOCIATIONS':'RETIRED', factor_key = FALSE)

dt.long %>%
  select(Year, Name, Amount) %>%
  arrange(Name) %>%
  kable() %>%
  kable_styling(bootstrap_options = "striped", full_width = F)

dt.long$Year <- as.numeric(dt.long$Year)


quartz()
dt.long %>%
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

