import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
  (
     artist        VARCHAR,
     auth          VARCHAR NOT NULL,
     firstname     VARCHAR,
     gender        VARCHAR,
     iteminsession INTEGER NOT NULL,
     lastname      VARCHAR,
     length        FLOAT,
     level         VARCHAR,
     location      VARCHAR,
     method        VARCHAR,
     page          VARCHAR,
     registration  BIGINT,
     sessionid     INTEGER NOT NULL,
     song          VARCHAR,
     status        INTEGER NOT NULL,
     ts            BIGINT  NOT NULL,
     useragent     VARCHAR,
     userid        INTEGER
  );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
  (
     num_songs        INTEGER NOT NULL,
     artist_id        VARCHAR NOT NULL,
     artist_latitude  FLOAT,
     artist_longitude FLOAT,
     artist_location  VARCHAR,
     artist_name      VARCHAR NOT NULL,
     song_id          VARCHAR NOT NULL,
     title            VARCHAR NOT NULL,
     duration         FLOAT   NOT NULL,
     year             INTEGER
  );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay
(
     songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
     start_time  TIMESTAMP NOT NULL,
     user_id     INTEGER NOT NULL,
     level       VARCHAR NOT NULL,
     song_id     VARCHAR NOT NULL distkey,
     artist_id   VARCHAR NOT NULL,
     session_id  INTEGER NOT NULL,
     location    VARCHAR NOT NULL,
     user_agent  VARCHAR NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user
(
    user_id INTEGER PRIMARY KEY,
    first_name      VARCHAR,
    last_name       VARCHAR,
    gender          VARCHAR,
    level           VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song
(
    song_id     VARCHAR PRIMARY KEY distkey,
    title       VARCHAR,
    artist_id   VARCHAR NOT NULL,
    year        INTEGER,
    duration    FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist
(
    artist_id          VARCHAR PRIMARY KEY,
    name               VARCHAR,
    location           VARCHAR,
    latitude           FLOAT,
    longitude          FLOAT
);

""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    start_time    TIMESTAMP PRIMARY KEY sortkey,
    hour          INTEGER,
    day           INTEGER,
    week          INTEGER,
    month         INTEGER,
    year          INTEGER,
    weekday       INTEGER
);

""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    COMPUPDATE OFF
    JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    COMPUPDATE OFF
    JSON 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay
            (start_time,
             user_id,
             level,
             song_id,
             artist_id,
             session_id,
             location,
             user_agent)
SELECT DISTINCT timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
                e.userid                                            AS user_id,
                e.level                                             AS level,
                s.song_id                                           AS song_id,
                s.artist_id                                         AS artist_id,
                e.sessionid                                         AS session_id,
                e.location                                          AS location,
                e.useragent                                         AS user_agent
FROM   staging_events e
       JOIN staging_songs s
         ON ( s.title = e.song
              AND s.artist_name = e.artist );
""")

user_table_insert = ("""
INSERT INTO dim_user
            (user_id,
             first_name,
             last_name,
             gender,
             level)
SELECT DISTINCT e.userid    AS user_id,
                e.firstname AS first_name,
                e.lastname  AS last_name,
                e.gender    AS gender,
                e.level     AS level
FROM   staging_events e
WHERE  ( page = 'NextSong'
         AND user_id NOT IN (SELECT DISTINCT user_id
                             FROM   dim_user) ); 
""")

song_table_insert = ("""
INSERT INTO dim_song
            (song_id,
             title,
             artist_id,
             year,
             duration)
SELECT DISTINCT s.song_id   AS song_id,
                s.title     AS title,
                s.artist_id AS artist_id,
                s.year      AS year,
                s.duration  AS duration
FROM   staging_songs s
WHERE  song_id NOT IN (SELECT DISTINCT song_id
                       FROM   dim_song);
""")

artist_table_insert = ("""
INSERT INTO dim_artist
            (artist_id,
             name,
             location,
             latitude,
             longitude)
SELECT DISTINCT s.artist_id        AS artist_id,
                s.artist_name      AS name,
                s.artist_location  AS location,
                s.artist_latitude  AS latitude,
                s.artist_longitude AS longitude
FROM   staging_songs s
WHERE  artist_id NOT IN (SELECT DISTINCT artist_id
                         FROM   dim_artist); 

""")

time_table_insert = ("""
INSERT INTO dim_time
            (start_time,
             hour,
             day,
             week,
             month,
             year,
             weekday)
SELECT DISTINCT start_time                       AS start_time,
                Extract(hour FROM start_time)    AS hour,
                Extract(day FROM start_time)     AS day,
                Extract(week FROM start_time)    AS week,
                Extract(month FROM start_time)   AS month,
                Extract(year FROM start_time)    AS year,
                Extract(weekday FROM start_time) AS weekday
FROM   fact_songplay
WHERE  start_time NOT IN (SELECT DISTINCT start_time
                          FROM   dim_time); 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
