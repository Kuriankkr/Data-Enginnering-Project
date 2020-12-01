## Sparkify's Event Logs Data Pipeline

### Introduction
This project consists of a single DAG that implements the entire data pipeline responsible for reading all of Sparkify's event logs, process and create facts and dimensions table as shown in the data schema below

![Dag_Cycle](https://github.com/Kuriankkr/Udacity-Nanodegree-Data-Engineering/blob/master/Data%20Pipelines%20with%20Airflow/Dag_Cycle.JPG)

## ELT Steps
- ***Stage_events:*** : Intermediate table for pulling the data that has meta information about songs/data
- ***Stage_songs:***  : Intermediate table for pulling the data that has information about the log data form the app
- ***Load_songplays_fact_table*** : This would create and load data into the songplays_fact_table
- ***Load_song_dim_table***: This would create and load data into the song_dim_table
- ***Load_user_dim_table***: This would create and load data into the user_dim_table
- ***Load_artist_dim_table***: This would create and load data into the artist_dim_table
- ***Load_time_dim_table***: This would create and load data into the time_dim_table
- ***Run_data_quality_checks***: Check if the tables have been created and data exists. This is by checking the facts/dimensions has atleast one row

## Data Sources
The data that has been sourced from these two sources that reside in S3 buckets
- ***s3://udacity-dend/song_data/*** - JSON files containing meta information about songs/artist data
- ***s3://udacity-dend/log_data/*** - JSON files containing log events from the Sparkify app

## Data Schema
![Udacity_TablesJPG](https://github.com/Kuriankkr/Udacity-Nanodegree-Data-Engineering/blob/master/Data%20Lakes%20with%20Spark/Udacity_TablesJPG.JPG)
