# Project: Data Modeling with Postgres
## Introduction
In this project, data is loading from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Technologies
Python (boto3), Redshift, AWS, Datawarehouse, ETL pipeline, JSON

## Design
### Database Schema
Since we have a simple queries, the star schema has been choosen. This schema is the simplest styl of data mart schema.
Five Tables have been designed, one Fact Table and four Dimension Tables.

#### Staging Tables
##### 1.**staging_events**
+columns:ste_artist, ste_auth, ste_fname, ste_gender, ste_itemsInSessions, ste_lname, ste_length, ste_level,    
         ste_location, ste_method, ste_page,ste_registration, ste_session_id, ste_song, ste_status,ste_ts,  
         ste_userAgent, ste_user_id
##### 2.**staging_songs**
+column:num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title,
        duration, year
#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong
+columns:songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
##### 1.**users** - users in the app
+columns: user_id, first_name, last_name, gender, level
##### 2.**songs** - songs in music database
+columns:song_id, title, artist_id, year, duration
##### 3.**artists** - artists in music database
+columns:artist_id, name, location, latitude, longitude
##### 4.**time** - timestamps of records in songplays broken down into specific units
+columns: start_time, hour, day, week, month, year, weekday

### ETL Pipeline
Pyton has been used to read the songs and logs data from JSON fil, to transforme the read data, and store the processed data in PostgreSQL database tables.

## Implementation

### Input Data
#### LOG_DATA='s3://udacity-dend/log_data'
#### LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
#### SONG_DATA='s3://udacity-dend/song_data'

### File: sql_queries.py
This file contains the queries for creating tables, inserting data into the tables, dropping the tables and searching data in the tables. So the changes on the tables should occur in this file.

### File: create_Tables.py
This file contains python functions, which call the queries defined in the file *sql_queries.py*for creating tables, inserting data into the tables, dropping the tables. By running this files, the staging tables, dimenesional tables and fact table will be created in the redshift database. 

### File: etl.py
This file contains python fuctions for processing songs data and logs data. By running this file songs and logs data will be read from the song_data and log_data located in S3 (see input data), processed and store in the redshift database. The file *create_tables.py* should be successfuly executed prior running the file *etl.py* 

### File: dwh.py
This file contains AWS datawarehouse configurations parameters.

## Run the application
Before running the files in items (1) and (2) make sure that the redshift (cluster) database is available.
The parameters to set the aws resources and clients are in the file dwl.cfg available
### 1.offen a Terminal
### 2.Run the file *create_tables.py* : in the Terminal write *python create_tables.py*
### 3.Run the file *etl.py* : in the Terminal write *python etl.py*
