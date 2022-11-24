
### Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
The database sparkifydb contains information about artists and their songs that are available for the startup from their music streaming app, users who are listening to the songs on the app, and the log of actions on the service. 
These data can be done for following analysis: 
* which song is popular
* preference of song types by gender
* preference of songs by artist location
* which song and artist are popular per each year
etc.
These data could help to provide better songs and service, according to users' taste, thus increase the likability of the app and attract more users. 

### How to run the Python scripts
In the terminal run "python name_of_the_file.py"

### An explanation of the files in the repository
* data folder contains the 2 datasets.
* create_tables.py creates a database sparkifydb, drops and creates all tables.
* etl.ipynb contains the etl process, i.e. inserting data to tables. You write codes in smaller chanks and use the instructions provided.
* etl.py contains ETL pipeline - the whole etl process in 1 file. Just run it, and the data are inserted to corresponding tables.
* sql_queries.py contains all queries (drop table, create table, insert, select), that will be runned by python files.
* test.ipynb provides codes for testing the content of database tables. Moreover, contains the sanity test for checking PK and datatypes of the columns in the tables.

### State and justify your database schema design and ETL pipeline.
Database has a star chema, with *songplays* as a fact table, and *users, artists, songs, time* as dimension tables. *songplays* contains inform about the action that was made in the music app. 
Each dimension table contains data that describes the entity - user information or artist information.
ETL pipeline consists of several functions:
* process_song_file() - read json files from song_data folder, and load corresponding data to songs table and artists table.
* process_log_file() - read json files from log_data folder, filter data by 'NextSong', convert ts to datetime format and extract requested date and time information, that will be loaded to _time_ table. Insert requested fields from dataframe to _users_ table. Get a song_id and artist_id from SQL, and insert them to _songplays_ table along to other required fields. 
* process_data() - process all files in the data folders, using process_song_file() funstion or process_log_file() function. Print if every file has been successfully processed. 
* main() - connect to the database, call process_data() function, close the connection. 