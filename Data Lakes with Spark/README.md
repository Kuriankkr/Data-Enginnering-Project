## Project DataLake

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This project aims to create analytical parquet tables on Amazon S3 using AWS ElasticMapReduce/Spark to extract, load and transform songs data and event logs from the usage of the Sparkify app. We would have to EMR to extract data from S3 buckets that in JSON format, transform it and then load them into new S3 buckets.

### Project Description

Apply the knowledge of Spark and Data Lakes to build and ETL pipeline for a Data Lake hosted on Amazon S3. In this task, we have to build an ETL Pipeline that extracts their data from S3 and process them using Spark and then load back into S3 in a set of Fact and Dimension Tables. This will allow their analytics team to continue finding insights in what songs their users are listening. Will have to deploy this Spark process on a Cluster using AWS.

### ETL Pipeline

1) Load the credentials from dl.cfg

2) Read data from s3

  - Song data
  - Log data
  
3) Process the data using spark
   
   The data should be transformed for creating the following tables
   
   Fact Table:
   songplays - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id,level, song_id, artist_id, session_id, location, user_agent
    
   Dimensions Table:
    Users Table:
    user_id, first_name, last_name, gender, level
    
    Songs Table:
    song_id, title, artist_id, year, duration
    
    Artists Table:
    artist_id, name, location, lattitude, longitude
    
    Time Table:
    start_time, hour, day, week, month, year, weekday

4) Load them back into the new s3 bucket

### To run the scripts
To run this project in local mode, create a file dl.cfg in the root of this project with the following data:

KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY

Create an S3 Bucket where output results will be stored.

Finally, run the following command:
python etl.py