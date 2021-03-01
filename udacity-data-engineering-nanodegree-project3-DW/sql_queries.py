import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events
                                 (artist varchar,
                                  auth varchar,
                                  first_name varchar,
                                  gender varchar,
                                  item_in_session int,
                                  last_name varchar,
                                  length numeric,
                                  level varchar,
                                  location varchar,
                                  method varchar,
                                  page varchar,
                                  registration float,
                                  session_id int,
                                  song varchar,
                                  status int,
                                  ts numeric,
                                  user_agent varchar,
                                  user_id int);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                 (num_songs int,
                                  artist_id varchar,
                                  artist_latitude float,
                                  artist_longitude float,
                                  artist_location varchar,
                                  artist_name varchar,
                                  song_id varchar,
                                  title varchar,
                                  duration float,
                                  year int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                            (songplay_id int IDENTITY(0,1) PRIMARY KEY,
                             start_time timestamp NOT NULL,
                             user_id int NOT NULL,
                             level varchar NOT NULL,
                             song_id varchar,
                             artist_id varchar,
                             session_id int NOT NULL,
                             location varchar,
                             user_agent varchar,
                             UNIQUE (start_time, user_id));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                            (user_id int PRIMARY KEY,
                             first_name varchar NOT NULL,
                             last_name varchar NOT NULL,
                             gender varchar,
                             level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                            (song_id varchar PRIMARY KEY,
                             title varchar NOT NULL,
                             artist_id varchar NOT NULL,
                             year int,
                             duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                            (artist_id varchar PRIMARY KEY,
                             name varchar NOT NULL,
                             location varchar,
                             latitude float,
                             longitude float);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                            (start_time timestamp PRIMARY KEY,
                             hour int NOT NULL,
                             day int NOT NULL,
                             week int NOT NULL,
                             month int NOT NULL,
                             year int NOT NULL,
                             weekday int NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
                          FROM {}
                          IAM_ROLE '{}'
                          REGION '{}'
                          FORMAT AS JSON {}""".format(
                                             config.get('S3','LOG_DATA'),
                                             config.get('IAM_ROLE','ARN'),
                                             config.get('DWH','DWH_REGION'),   
                                             config.get('S3','LOG_JSONPATH')
                                            )
                      )


staging_songs_copy = ("""COPY staging_songs
                          FROM {}
                          IAM_ROLE '{}'
                          REGION '{}'
                          FORMAT AS JSON 'auto'""".format(
                                             config.get('S3','SONG_DATA'),
                                             config.get('IAM_ROLE','ARN'),
                                             config.get('DWH','DWH_REGION'),
                                            )
                      )

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays
                            (start_time, user_id,
                             level, song_id, artist_id,
                             session_id, location, user_agent)
                            SELECT
                             timestamp 'epoch' + se.ts / 1000 * interval '1 second' AS start_time,
                             se.user_id,
                             se.level,
                             ss.song_id,
                             ss.artist_id,
                             se.session_id,
                             se.location,
                             se.user_agent
                            FROM staging_events se
                            LEFT JOIN staging_songs ss
                            ON se.song = ss.title AND se.artist = ss.artist_name
                            WHERE se.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users
                        (user_id, first_name, last_name,
                         gender, level)
                        SELECT se.user_id, se.first_name, se.last_name, se.gender, se.level
                        FROM staging_events se
                        JOIN
                        (SELECT max(ts) as ts, user_id
                         FROM staging_events
                         WHERE page = 'NextSong'
                         GROUP BY user_id) sj ON se.user_id = sj.user_id AND se.ts = sj.ts;
""")

song_table_insert = ("""INSERT INTO songs
                        (song_id, title, artist_id,
                         year, duration)
                        SELECT  DISTINCT
                         song_id,
                         title,
                         artist_id,
                         year,
                         duration
                        FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists
                          (artist_id,  name, location,
                           latitude, longitude)
                          SELECT DISTINCT
                           artist_id,
                           artist_name as name,
                           artist_location as location,
                           artist_latitude as latitude,
                           artist_longitude as longitude
                          FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time
                        (start_time, hour, day, week,
                         month, year, weekday)
                         SELECT DISTINCT
                          t.start_time,
                          extract(hour from t.start_time) AS hour,
                          extract(day from t.start_time) AS day,
                          extract(week from t.start_time) AS week,
                          extract(month from t.start_time) AS month,
                          extract(year from t.start_time) AS year,
                          extract(weekday from t.start_time) AS weekday
                          FROM
                          (SELECT DISTINCT
                            timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time
                           FROM staging_events
                           WHERE page = 'NextSong') t;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
