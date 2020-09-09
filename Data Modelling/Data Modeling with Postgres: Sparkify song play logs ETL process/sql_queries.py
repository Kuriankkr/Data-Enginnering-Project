# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artists_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"


# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table(
                    songplay_id INT PRIMARY KEY,
                    start_time TIMESTAMP NOT NULL,
                    user_id INT NOT NULL, 
                    level VARCHAR,
                    song_id VARCHAR, 
                    artist_id VARCHAR,
                    session_id INT,
                    location VARCHAR, 
                    user_agent VARCHAR)
                    """)

user_table_create = (""" 
    CREATE TABLE IF NOT EXISTS user_table(
                    user_id INT PRIMARY KEY,
                    first_name VARCHAR NOT NULL, 
                    last_name VARCHAR NOT NULL,
                    gender VARCHAR,
                    level VARCHAR) 
                    """)


song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_table(
                    song_id VARCHAR PRIMARY KEY,
                    title VARCHAR NOT NULL, 
                    artist_id VARCHAR, 
                    year INT, 
                    duration FLOAT)
                    """)

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists_table(
                    artist_id VARCHAR PRIMARY KEY,
                    name VARCHAR NOT NULL, 
                    location VARCHAR, 
                    latitude FLOAT, 
                    longitude FLOAT)
                    """)


time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table(
                    start_time TIMESTAMP PRIMARY KEY,
                    hour INT,
                    day INT, 
                    week INT, 
                    month INT, 
                    year INT,
                    weekday VARCHAR)
                    """)


# INSERT RECORDS

songplay_table_insert = (""" 
    INSERT INTO songplay_table(
                    songplay_id,
                    start_time,
                    user_id,
                    level, 
                    song_id, 
                    artist_id, 
                    session_id, 
                    location,
                    user_agent)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (songplay_id) DO NOTHING;
                    """)

user_table_insert = (""" 
    INSERT INTO user_table(
                    user_id, 
                    first_name,
                    last_name, 
                    gender, 
                    level)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET LEVEL = EXCLUDED.level
                    """)

song_table_insert = ("""
    INSERT INTO song_table(
                    song_id, 
                    title,
                    artist_id,
                    year, 
                    duration)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (song_id) DO NOTHING;
                    """)


artist_table_insert = ("""
    INSERT INTO artists_table(
                    artist_id, 
                    name, 
                    location, 
                    latitude, 
                    longitude)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id) DO NOTHING;
                    """)


time_table_insert = (""" 
    INSERT INTO time_table(
                    start_time,
                    hour,
                    day,
                    week,
                    month, 
                    year ,
                    weekday)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (start_time) DO NOTHING;

                    """)

# FIND SONGS

song_select = ("""
    SELECT song_table.song_id,artists_table.artist_id 
    FROM song_table JOIN artists_table ON
    song_table.artist_id = artists_table.artist_id
    WHERE song_table.title =  %s
    AND artists_table.name = %s
    AND song_table.duration = %s
                """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]