import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id 


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEY']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEY']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    - Create spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    - reads songs and artists data files
    - converts songs table to parquet files
    - converts artist table to parquet files
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # create schema for song data
    songSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema = songSchema)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                    .filter("song_id is not null")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 
                              col('artist_name').alias("name"), 
                              col('artist_location').alias('location'), 
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')) \
                      .where(col("artist_id").isNotNull())

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists/')

    
def process_log_data(spark, input_data, output_data):
    """
    - reads log data file
    - extracts users, time, songplays tables from log data
    - writes users, time, songplays tables to parquet files
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # create schema for log data
    logSchema = R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("lastName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Int()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Int()),
        Fld("song", Str()),
        Fld("status", Int()),
        Fld("ts", Int()),
        Fld("userAgent", Str()),
        Fld("userId", Int())
    ])
    
    # read log data file
    df = spark.read.json(log_data, schema = logSchema)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'), 
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'), 
                            'gender',
                            'level') \
                    .filter('user_id is not null')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000.0),TimestampType())
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000.0),DateType())
    df = df.withColumn('datetime',get_datetime(df.ts))
    
    # extract columns to create time table
    time_table =  df.select(col("timestamp").alias("start_time"),
                            hour("timestamp").alias("hour"),
                            dayofmonth("timestamp").alias("day"),
                            weekofyear("timestamp").alias("week"),
                            month("timestamp").alias("month"),
                            year("timestamp").alias("year"),
                            dayofweek("timestamp").alias("weekday"))

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, 'left_outer') \
                        .select(col('timestamp').alias("start_time"),
                                col('userId').alias("user_id"),
                                df.level,
                                song_df.song_id,
                                song_df.artist_id,
                                col('sessionid').alias("session_id"),
                                df.location,
                                col('useragent').alias("user_agent"),
                                year('timestamp').alias('year'),
                                month('timestamp').alias('month')) \
                        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + 'time/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mys3udacitybucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
