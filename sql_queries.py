import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events( ste_artist varchar,
        ste_auth varchar,
        ste_fname varchar,
        ste_gender char,
        ste_itemsInSessions int,
        ste_lname varchar,
        ste_length numeric,
        ste_level varchar,
        ste_location varchar,
        ste_method varchar,
        ste_page varchar,
        ste_registration bigint,
        ste_session_id int,
        ste_song varchar,
        ste_status int,
        ste_ts numeric,
        ste_userAgent varchar,
        ste_user_id int);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs(num_songs int,
        artist_id varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year int);
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplay(songplay_id INT IDENTITY(1, 1) PRIMARY KEY,
        start_time TIMESTAMP REFERENCES time(start_time),
        user_id int REFERENCES users(user_id),
        level varchar,
        song_id varchar REFERENCES songs(song_id),
        artist_id varchar REFERENCES artists(artist_id),
        session_id varchar NOT NULL,
        location varchar,
        user_agent varchar,
        UNIQUE(start_time, user_id, session_id));
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar NOT NULL);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar,
        year int,
        duration numeric);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        lattitude varchar,
        longitude varchar);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP PRIMARY KEY,
        ts numeric,
        hour varchar,
        day varchar,
        week varchar,
        month varchar,
        year varchar,
        weekday varchar);
""")

# STAGING TABLES

staging_events_copy = f"""
    COPY staging_events from {config.get("S3", "LOG_DATA")}
    iam_role {config.get("IAM_ROLE", "ARN")}
    format as json {config.get("S3", "LOG_JSONPATH")}
"""

staging_songs_copy = f"""COPY staging_songs from {config.get("S3", "SONG_DATA")}
    iam_role {config.get("IAM_ROLE", "ARN")}
    format as json 'auto';
"""

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay (start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)
        
 SELECT time.start_time         AS start_time,
        ste.ste_user_id         AS user_id,
        ste.ste_level           AS level,
        sts.song_id             AS song_id,
        sts.artist_id           AS artist_id,
        ste.ste_session_id      AS session_id,
        sts.artist_location     AS location,
        ste.ste_userAgent       AS user_agent
        
 FROM staging_events ste
 JOIN staging_songs sts ON ((sts.title=ste.ste_song) AND (sts.artist_name = ste.ste_artist))
 JOIN time ON (time.ts=ste.ste_ts)
 WHERE ste.ste_page='NextSong';
""")

user_table_insert = (""" INSERT INTO users(user_id,
        first_name,
        last_name,
        gender,
        level)
        
 SELECT DISTINCT ste.ste_user_id AS user_id,
        ste.ste_fname            AS first_name,
        ste.ste_lname            AS last_name,
        ste.ste_gender           AS gender,
        ste.ste_level            AS level
        
 FROM staging_events ste
 WHERE ste.ste_user_id IS NOT NULL AND ste.ste_page='NextSong';
""")

song_table_insert = (""" INSERT INTO songs(song_id,
        title,
        artist_id,
        year,
        duration)
 SELECT DISTINCT sts.song_id    AS song_id,
        sts.title               AS title,
        sts.artist_id           AS artist_id,
        sts.year                AS year,
        sts.duration            AS duration
 FROM   staging_songs sts
 WHERE sts.song_id IS NOT NULL;
""")

artist_table_insert = (""" INSERT INTO artists (artist_id,
        name,
        location,
        lattitude,
        longitude )
 SELECT DISTINCT sts.artist_id  AS artist_id,
        sts.artist_name         AS name,
        sts.artist_location     AS location,
        sts.artist_latitude     AS latitude,
        sts.artist_longitude    AS longitude
 FROM   staging_songs sts
 WHERE  sts.artist_name IS NOT NULL;
""")

time_table_insert = (""" INSERT INTO time (start_time,
        ts,
        hour,
        day,
        week,
        month,
        year,
        weekday)
 SELECT DISTINCT TIMESTAMP 'epoch' + ste.ste_ts/1000 * INTERVAL '1 Second' AS start_time,
        ste.ste_ts                        AS ts,
        EXTRACT(hour FROM start_time)     AS hour,
        EXTRACT(day FROM start_time)      AS day,
        EXTRACT(week FROM start_time )    AS week,
        EXTRACT(month FROM start_time)    AS month,
        EXTRACT(year FROM start_time)     AS year,
        EXTRACT(weekday FROM start_time)  AS weekday
        
 FROM   staging_events ste;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [time_table_insert, user_table_insert, song_table_insert, artist_table_insert,songplay_table_insert ]
