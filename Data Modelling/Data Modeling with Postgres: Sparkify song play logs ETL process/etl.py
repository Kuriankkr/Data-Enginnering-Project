import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """" Reads the data from song file and selects the required fields and inserts them into song_data and artist_data
    Arguments:
    cur(): This specifies the cursor for the sparkifydb database
    filepath: Specifies the file path of the data file
    
    """"
    # Have to read the file from song_data and save it into song_data and artist_data. Essentially its a reapeat of etl.ipynb that I copied and pasted here
    # open song file
    df = pd.read_json(filepath,lines=True)
    artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year = df.values[0]
    
    # insert song record
    song_data = [song_id,title,artist_id,year,duration]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [artist_id,artist_name,artist_location,artist_latitude, artist_longitude]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """" Reads user activity from log file and filters the data rows by Next Song, selectst the required fields, transforms certain columns and and inserts them into 
         time, user and songplay tables
         
    Arguments:
    cur(): This specifies the cursor for the sparkifydb database
    filepath: Specifies the file path of the data file
    
    """
    
    # open log file
    df =  pd.read_json(filepath,lines=True) 

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = []
    for value in t:
        time_data.append([value,value.hour,value.day,value.week,value.month,value.year,value.day_name()])
    column_labels =  ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [index,pd.to_datetime(row.ts, unit = 'ms'),int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """"
    Goes through all the files in each directory and processes in each file
    
    Arguments:
    cur(): This specifies the cursor for the sparkifydb database
    conn(): Connection to the sparkifydb database
    filepath(): Specifies the file path of the data file
    func(): Function to process each data file inside each directory
    
    """
    
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    Function for intitating the process of for extracting, transforming and loading the data into Postgre SQL Database through the creation of tables 
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()