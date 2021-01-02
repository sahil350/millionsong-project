# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                                    songplay_id SERIAL PRIMARY KEY,
                                                    start_time TIMESTAMP NOT NULL,
                                                    user_id INT NOT NULL,
                                                    level VARCHAR,
                                                    song_id VARCHAR,
                                                    artist_id VARCHAR,
                                                    session_id INT,
                                                    location VARCHAR,
                                                    user_agent VARCHAR)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                            user_id int PRIMARY KEY,
                                            first_name VARCHAR,
                                            last_name VARCHAR,
                                            gender VARCHAR,
                                            level VARCHAR)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                            song_id varchar PRIMARY KEY,
                                            artist_id VARCHAR,
                                            title VARCHAR,
                                            year INT,
                                            duration NUMERIC)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                                artist_id VARCHAR PRIMARY KEY,
                                                name VARCHAR,
                                                location VARCHAR,
                                                latitude NUMERIC,
                                                longitude NUMERIC)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                           start_time TIMESTAMP PRIMARY KEY,
                                           hour INT,
                                           day INT,
                                           month INT,
                                           year INT,
                                           weekday VARCHAR)
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (
                                                    start_time,
                                                    user_id,
                                                    level,
                                                    song_id,
                                                    artist_id,
                                                    session_id,
                                                    location,
                                                    user_agent)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users (
                                           user_id,
                                           first_name,
                                           last_name,
                                           gender,
                                           level)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (user_id) 
                                    DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs(
                                          song_id,
                                          artist_id,
                                          title,
                                          year,
                                          duration)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (song_id) 
                                    DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists(
                                              artist_id,
                                              name,
                                              location,
                                              latitude,
                                              longitude)
                                      VALUES (%s, %s, %s, %s, %s)
                                      ON CONFLICT (artist_id) 
                                      DO NOTHING;
""")


time_table_insert = ("""INSERT INTO time(
                                         start_time,
                                         hour,
                                         day,
                                         month,
                                         year,
                                         weekday)
                                  VALUES (%s, %s, %s, %s, %s, %s)
                                  ON CONFLICT (start_time) 
                                  DO NOTHING;
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, artists.artist_id
                  FROM songs JOIN artists ON songs.artist_id = artists.artist_id
                  WHERE songs.title = %s AND
                        artists.name = %s AND
                        songs.duration = %s
""")

# TEMPORARY TABLES

time_tmp_table = ("""CREATE TEMP TABLE tmp_table (LIKE time INCLUDING DEFAULTS)
                     ON COMMIT DROP;
""")

users_tmp_table = ("""CREATE TEMP TABLE tmp_table (LIKE users INCLUDING DEFAULTS)
                     ON COMMIT DROP;
""")

# INSERT INTO MAIN TABLES FROM TEMPORARY TABLES

time_table_bulk_insert = ("""INSERT INTO time
                             SELECT *
                             FROM tmp_table
                             ON CONFLICT (start_time) DO NOTHING;
""")

users_table_bulk_insert = ("""INSERT INTO users
                             SELECT *
                             FROM tmp_table
                             ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
