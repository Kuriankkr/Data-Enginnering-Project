## Capstone Project

This project takes in two sources of data: Immigration and Temperature. The idea is to take this data sourced from US cities. The data gets transformed in a relational data model that is optimized to analyze to immigration events. The database then can be used to understand immigration patterns with respect to temperature in US cities.

## Data Sources
****Temperature Data**** : The temperature data is sourced from Kaggle and its link is provided below
https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data
- Contains 7 columns  containing information about Average Temperature, City, Country, Latitude and Longitude
- The file contains around 8.5M rows

***Immigration Data***: Airport codes and related cities defined in I94 data description file.
https://travel.trade.gov/research/reports/i94/historical/2016.html
- I94 dataset has SAS7BDAT file per each month of the year (e.g. i94_jan16_sub.sas7bdat).
- Each file contains about 3M rows
- Data has 28 columns containing information about event date, arriving person, airport, airline, etc.


## Database schema


