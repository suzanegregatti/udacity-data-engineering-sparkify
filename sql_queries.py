# DROP TABLES

temp_log_table_drop = "DROP TABLE IF EXISTS temp_log"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

temp_log_table_create = ("""
    CREATE TABLE IF NOT EXISTS temp_log (
        artist varchar,        
        first_name varchar,
        gender char,        
        last_name varchar, 
        length numeric,
        level varchar,
        location varchar,
        session_id int,
        song varchar,
        ts timestamp,
        user_agent varchar,
        user_id int 
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year int,
    duration numeric
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude numeric,
    longitude numeric
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time timestamp NOT NULL REFERENCES time(start_time),
    user_id int NOT NULL REFERENCES users(user_id),
    level varchar,
    song_id varchar REFERENCES songs(song_id),
    artist_id varchar REFERENCES artists(artist_id),
    session_id int,
    location varchar,
    user_agent varchar
    );
""")

# INSERT RECORDS

songplay_table_insert = (""" 
    WITH song_artist AS (    
        SELECT
            s.song_id, s.artist_id, tl.song, tl.artist, tl.length
        FROM
            songs s
            INNER JOIN artists a ON s.artist_id = a.artist_id
            INNER JOIN temp_log tl ON s.title = tl.song AND a.name = tl.artist AND s.duration = tl.length
    )    
    INSERT INTO
        songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)    
    SELECT
        ts, user_id, level, song_id, artist_id, session_id, location, user_agent    
    FROM
        temp_log tl
        LEFT JOIN song_artist sa ON sa.song = tl.song AND sa.artist = tl.artist AND sa.length = tl.length;    
""")

user_table_insert = ("""
    WITH user_data AS (
        SELECT
            user_id, first_name, last_name, gender, level, ts,
            FIRST_VALUE(level) OVER (PARTITION BY user_id ORDER BY ts) AS last_level
        FROM temp_log
    )    
    INSERT INTO users 
    SELECT DISTINCT user_id, first_name, last_name, gender, last_level as level
    FROM user_data
    ON CONFLICT (user_id)
    DO
        UPDATE SET level = excluded.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, s.artist_id
    FROM songs s
    INNER JOIN artists a ON s.artist_id = a.artist_id
    WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# DELETE RECORDS
temp_log_delete = 'DELETE FROM temp_log'

# QUERY LISTS

create_table_queries = [temp_log_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [temp_log_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]