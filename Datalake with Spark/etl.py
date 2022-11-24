import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, IntegerType



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = './data/song-data'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_name", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.option("header",True) \
            .partitionBy("year","artist_name") \
            .mode("overwrite") \
            .parquet(output_data+"songs_results")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")

    # write artists table to parquet files
    artists_table.repartition(1).write.option("header", True).mode("overwrite").parquet(output_data+"artists_result")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = './data/log-data'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"users_result")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x/1000), IntegerType())
    df = df.withColumn('datetime', get_timestamp('ts'))

    # create datetime column from original timestamp column
    df = df.withColumn('start_time', from_unixtime('datetime'))
    
    # extract columns to create time table
    df = df.withColumn('hour', hour('start_time'))
    df = df.withColumn('day', dayofmonth('start_time'))
    df = df.withColumn('week', weekofyear('start_time'))
    df = df.withColumn('month', month('start_time'))
    df = df.withColumn('year', year('start_time'))
    df = df.withColumn('weekday', dayofweek('start_time'))
    time_table = df.select("start_time",  "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.repartition(1).write.option("header", True).mode("overwrite").partitionBy("year", "month").parquet(output_data+"time_result")

    # read in song data to use for songplays table
    song_df = spark.read.parquet('songs_results/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, 
                              (df.song == song_df.title) & 
                              (df.artist == song_df.artist_name) & 
                              (df.length == song_df.duration), 'left_outer')\
            .select(
                df.start_time,
                col("userId").alias('user_id'),
                df.level,
                song_df.song_id,
                song_df.artist_name,
                col("sessionId").alias("session_id"),
                df.location,
                col("userAgent").alias("user_agent"),
                df.year,
                df.month
            )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.option("header", True).mode("overwrite").partitionBy("year", "month").parquet(output_data+"songplays_results")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
