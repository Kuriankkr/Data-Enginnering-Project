# Introduction

This project consists of a notebook that processes data and generates a NOSQL database using Appache Cassandra. The purpose of this project is that a startup called Sparkify wants to analyze the data that they have been collecting on songs and user activity on their new music streaming app. The case study was that this company had been collecting data on user activity from their music streaming application and stored it as CSV files but could not query the data and generate insights out of it.

# Database Design
Aside from the keyspace, 3 tables were generated to allow Sparkify fulfill differnet goals. With Appache Cassandra you model the database tables on the queries you want to run.

- ****song_session****: Returns the artist, song title and song's length in the music app history that was heard for a specific session and item in session
- ****userlib****: Returns the name of artist (wherever artist names are available) , song (sorted by itemInSession) and user (first and last name) for a specific user and session (e.g.userId = 10 AND sessionId = 182)
- ****app_history****:  Return every user name (first and last) in the music app history who listened to a specific song (e.g. All Hands Against His Own)

## Sample Queries
- From ****Table 1:****
  SELECT sessionID, itemInSession, artist, song_title, song_length FROM song_session WHERE  sessionID  = 338 AND itemInSession = 4
