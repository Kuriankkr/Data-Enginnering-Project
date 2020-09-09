import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"


# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
    artist              VARCHAR     NULL,
    auth                VARCHAR     NULL,
    firstName           VARCHAR     NULL,
    gender              VARCHAR     NULL,
    itemInSession       INTEGER     NULL,
    lastName            VARCHAR     NULL,
    length              FLOAT       NULL,
    level               VARCHAR     NULL,
    location            VARCHAR     NULL,
    method              VARCHAR     NULL,
    page                VARCHAR     NULL,
    registration        FLOAT       NULL,
    sessionId           INTEGER     NOT NULL,
    song                VARCHAR     NULL,
    status              INTEGER     NULL,
    ts                  TIMESTAMP   NOT NULL,
    userAgent           VARCHAR     NULL,
    userId              INTEGER     NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs           INTEGER    NULL,
        artist_id           VARCHAR    NOT NULL,
        artist_latitude     FLOAT      NULL,
        artist_longitude    FLOAT      NULL,
        artist_location     VARCHAR    NULL,
        artist_name         VARCHAR    NULL,
        song_id             VARCHAR    NOT NULL, 
        title               VARCHAR    NULL,
        duration            FLOAT      NULL,
        year                INTEGER    NULL
    )
""")


##
songplay_table_create = ("""
    CREATE TABLE songplay(
     songplay_id       INTEGER  IDENTITY(0,1) PRIMARY KEY,   
     start_time        TIMESTAMP    NOT NULL,
     user_id           INTEGER      NOT NULL,
     level             VARCHAR      NULL,
     song_id           VARCHAR      NOT NULL,
     artist_id         VARCHAR      NOT NULL,
     session_id        INTEGER      NOT NULL,
     location          VARCHAR      NULL,
     user_agent        VARCHAR      NULL
     )
""") 



user_table_create = ("""
    CREATE TABLE users(
     user_id             INTEGER    NULL, 
     first_name          VARCHAR    NULL,
     last_name           VARCHAR    NULL,
     gender              VARCHAR    NULL,    
     level               VARCHAR    NULL
     )
""")

song_table_create = ("""
    CREATE TABLE song(
    song_id             VARCHAR     NOT NULL  PRIMARY KEY,
    title               VARCHAR     NULL,
    artist_id           VARCHAR     NOT NULL,
    year                INTEGER     NULL,
    duration            FLOAT       NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
    artist_id            VARCHAR     NOT NULL   PRIMARY KEY,
    name                 VARCHAR     NULL,
    location             VARCHAR     NULL,
    latitude             FLOAT       NULL,
    longitude            FLOAT       NULL
    )
""")

time_table_create = ("""
    CREATE TABLE time_table(
    start_time          TIMESTAMP   NOT NULL PRIMARY KEY,
    hour                INTEGER     NULL,
    day                 INTEGER     NULL,
    week                INTEGER     NULL,
    month               INTEGER     NULL,
    year                INTEGER     NULL,
    weekday             VARCHAR     NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

## Reason for log_path requirement is mentioned in the following link and why we dont need it for song table


staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

## Reason for using format as auto is given in the links in the notepad file

# FINAL TABLES

songplay_table_insert = (""" 
    INSERT INTO songplay (start_time,
                          user_id,
                          level,
                          song_id,
                          artist_id,
                          session_id,
                          location,
                          user_agent)
    SELECT DISTINCT(se.ts)        AS start_time,
                se.userId         AS user_id,
                se.level          AS level,
                ss.song_id        AS song_id,
                ss.artist_id      AS artist_id,
                se.sessionId      AS session_id,
                se.location       AS location,
                se.userAgent      AS user_agent
                
    FROM staging_events se
    JOIN staging_songs ss
    ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
      
    
""")

## There is No "ON CONFLICT (songplay_id) DO NOTHING" no conflict  thingy here that is because that was in Postgre SQL and this is in AWS Redshift


user_table_insert = ("""
    INSERT INTO users  (user_id, 
                       first_name,
                       last_name,
                       gender,
                       level)
    SELECT  DISTINCT(se.userId)       AS user_id,
            se.firstName              AS first_name,
            se.lastName               AS last_name,
            se.gender                 AS gender,
            se.level                  AS level
    FROM staging_events as se
    WHERE se.userId IS NOT NULL
    AND se.page  =  'NextSong';
""")
       
song_table_insert = ("""
    INSERT INTO song (song_id,
                      title,
                      artist_id,
                      year,
                      duration)
    SELECT DISTINCT(ss.song_id)    AS   song_id,
                    ss.title       AS   title,
                    ss.artist_id   AS   artist_id,
                    ss.year        AS   year,
                    ss.duration    AS   duration
    FROM staging_songs as ss
    WHERE ss.song_id is NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time_table (start_time, 
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday)
    SELECT  DISTINCT (se.ts)                  AS start_time,
            EXTRACT(hour FROM start_time)     AS hour,
            EXTRACT(day FROM start_time)      AS day,
            EXTRACT(week FROM start_time)     AS week,
            EXTRACT(month FROM start_time)    AS month,
            EXTRACT(year FROM start_time)     AS year,
            EXTRACT(week FROM start_time)     AS weekday
    FROM staging_events AS se
    WHERE se.ts IS NOT NULL 
    AND se.page = 'NextSong';

""")

# QUERY LISTS

analytics_queries = ['select COUNT(*) AS total FROM artists','select COUNT(*) AS total FROM song','select COUNT(*) AS total FROM time_table','select COUNT(*) AS total FROM users','select COUNT(*) AS total FROM songplay']
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
