import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType,DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

print(config)

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']




def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    '''
    Loads JSON input data from song_data from input_data, and processes the data to extract song_table and artists_table and stores the data file in paraquet 
    
    Arguments
    spark : references to Spark session
    input_data:  path to the input data for accesing song_data from S3
    output_data: path to location where the data should be stored into S3
    
    Output 
    song_table
    artists_table
    
    '''
    print("--- Starting Song_Data ---")
    
    # get filepath to song data file
    song_data =  input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # created an temporary view for the data to read SQL Queries
    df.createOrReplaceTempView("song_table_data")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
                             SELECT DISTINCT 
                             STD.SONG_ID,
                             STD.ARTIST_ID, 
                             STD.YEAR, 
                             STD.DURATION
                             FROM song_table_data STD
                             WHERE STD.SONG_ID IS NOT NULL
                             
                            """)
 
   
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+'songs_table/')  ##output data is the path for to write into the s3 buckets. Partitioning the data by year,artist_id is eary for filtering 

    # extract columns to create artists table
    artists_table = spark.sql("""
                               SELECT DISTINCT STD.ARTIST_ID as NAME,
                               STD.ARTIST_LOCATION as LOCATION,
                               STD.ARTIST_LATITUDE as LATITUDE,
                               STD.ARTIST_LONGITUDE as LONGITUDE FROM 
                               song_table_data STD
                               WHERE STD.ARTIST_ID IS NOT NULL
                             
                              """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    
    '''
    Arguments:
    spark : references to Spark session
    input_data:  path to the input data for accesing song_data from S3
    output_data: path to location where the data should be stored into S3
    
    Output 
    users_table
    artists_table
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # Creating temporary view
    df.createOrReplaceTempView("log_table_data")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT LTD.userID as user_id,
                            LTD.firstName as first_name,
                            LTD.lastName as last_name,
                            LTD.gender as gender,
                            LTD.level as level
                            FROM  log_table_data LTD
                            WHERE LTD.userId is NOT NULL
                            """
                            ) 
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
 
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(output_data + 'time/')

    # read in song data to use for songplays table 
    df_song = spark.read.json("data/local-Songdata/song_data/*/*/*/*.json")
    df_song.createOrReplaceTempView("song_data_table")
    
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(""" SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(logT.ts/1000) as start_time,
                                month(to_timestamp(logT.ts/1000)) as month,
                                year(to_timestamp(logT.ts/1000)) as year,
                                logT.userId as user_id,
                                logT.level as level,
                                songT.song_id as song_id,
                                songT.artist_id as artist_id,
                                logT.sessionId as session_id,
                                logT.location as location,
                                logT.userAgent as user_agent
                                FROM log_data_table logT
                                JOIN song_data_table songT on logT.artist = songT.artist_name and logT.song = songT.title
                            """)
    
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(
        "year", "month").parquet(output_data + 'songplays/')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dnd-bucket-demo/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
