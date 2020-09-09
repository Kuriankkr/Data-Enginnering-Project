# Introduction

The purpose of this projet is that a startup called Sparkify wants to analyze the data that they have been collecting on songs and user activity on their new music streaming app. Their analytical goals in particularly include understanding what song users are listening to. 

This project extract, transform and loads 5 main informations (tables) from the Sparkify app (an app to listen to your favorite musics) logs:

# Database Schema
This schema used for this project is Star schema. There is one fact table and four dimensions table each with a primary key that has been referenced from the fact table. The reason to use relational database  is because the data is structured, the amount of data is not big enough for big data solutions, joins are also being used.

## Fact Table
- songplays - records in data associated with songplays

## Dimensions Table
- users: This dimension table consists of data about the users
- songs: This consists of data about songs in the database
- artists: This consists of data about the artists in the database
- time: timestamps of records in songplays broken down into specific units

# Project Structure
- data: This has all the data which are in json files
- sql_queries.py: This has all the queries and is imported below
- test.ipynb: This is to cross check whether our inputs are correct and whether our tables are being created 
- create_tables.py: This python script creates and drops tables. This can reset the table each time we run the ETL scripts
- etl.ipynb: This reads and processes a single file from song_data and log_data and loads the data into our tables. This is more of a test file for etl.py
- etl.py: This reads and processes files from song_data and log_data and loads them into our tables.




