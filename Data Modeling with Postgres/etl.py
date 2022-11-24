import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """
    - Reads json files from song_data folder
    - Picks up requested columns and loads them to 'songs' and 'artists' tables,
      executing the insert queries from sql_queries.py
    """
    # open song file
    df = pd.read_json(filepath, lines=True) 

    # insert song record
    df_columns=df[["song_id","title","artist_id","year","duration"]]
    df_values=df_columns.values[0]
    song_data = df_values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude","artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads json files from log_data folder
    - Filters by 'NextSong' action
    - Converts ts timestamp column to datetime
    - Extracts time data from datetime, creates a Dataframe with it, and loads this Dataframe to 'time' table
    - Picks up requested columns and loads them to 'users' table, executing the insert queries from sql_queries.py
    - Insert songplay records, using data from log files and songid and artistid, extracted from song and artist tables
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    is_NextSong = df['page'] == 'NextSong'
    df = df[is_NextSong]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    t_hour=t.dt.hour
    t_day=t.dt.day
    t_week=t.dt.week
    t_month=t.dt.month
    t_year=t.dt.year
    t_weekday=t.dt.weekday
    
    time_data = (t, t_hour, t_day, t_week, t_month, t_year,t_weekday)
    column_labels = ('timestamp','hour', 'day', 'week', 'month', 'year', 'weekday')
    t_dict= {column_labels[i]: time_data[i] for i in range(len(column_labels))}
    time_df = pd.DataFrame(data=t_dict) 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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
        ts = pd.to_datetime(row.ts, unit='ms')
        songplay_data = (ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Gets all json files from directory
    - Processes files from directory and print the result
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
    - Connects to sparkifydb database 
    - Calls process_data() with process_song_file() and process_log_file() functions
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()