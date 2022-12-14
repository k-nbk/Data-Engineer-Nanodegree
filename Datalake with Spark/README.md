### Datalake Project with Spark

The goal of the project is to create fact and dimension tables for the project using Apache Spark.

#### Data

**Song Dataset:** "The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song." (c) Udacity

**Log Dataset:** "The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings." (c) Udacity

#### Functions

Function **create_spark_session()** creates a Spark session

Function **process_song_data()** processes data about the songs in 'song-data' folder: 
* load json files from 'song-data' folder
* songs_table table consists of the following columns from json files: "song_id", "title", "artist_name", "year", "duration".
* write songs table to parquet files partitioned by year and artist_name
* create artists table using columns from song_data: "artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"
* write artists table to parquet files

Function **process_log_data()** processes log-data: 
* load json files from 'log-data' folder
* create user table using columns from log_data: "userId", "firstName", "lastName", "gender", "level"
* write user table to parquet files
* create datetime column from original datetime in miliseconds (int)
* extract hour, day, week, month, year, weekday from datetime column. Use them to create time_table.
* write time table to parquet files partitioned by year and month
* extract columns from joined song and log datasets to create songplays table 
* write songplays table to parquet files partitioned by year and month

Function **main()** creates a Spark session, provides path to input data and output folder, and feeds them to process_song_data() and process_log_data().

As a result, 5 tables was created using Spark: songplays as a fact table and songs_table, artists_table, time_table and user_table are dimension tables.
