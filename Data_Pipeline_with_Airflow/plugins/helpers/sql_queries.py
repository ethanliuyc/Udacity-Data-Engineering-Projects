class SqlQueries:
    songplay_table_insert = ("""
    INSERT INTO {}
                (start_time,
                 userid,
                 level,
                 songid,
                 artistid,
                 sessionid,
                 location,
                 user_agent,
                 playid)
    SELECT DISTINCT timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
                    e.userid                                            AS userid,
                    e.level                                             AS level,
                    s.song_id                                           AS songid,
                    s.artist_id                                         AS artistid,
                    e.sessionid                                         AS sessionid,
                    e.location                                          AS location,
                    e.useragent                                         AS user_agent,
                    md5(e.sessionid || e.ts)                            AS playid
    FROM   staging_events e
           JOIN staging_songs s
             ON ( s.title = e.song
                  AND s.artist_name = e.artist );
""")

    user_table_insert = ("""
    INSERT INTO {}
                (userid,
                 first_name,
                 last_name,
                 gender,
                 level)
    SELECT DISTINCT e.userid    AS userid,
                    e.firstname AS first_name,
                    e.lastname  AS last_name,
                    e.gender    AS gender,
                    e.level     AS level
    FROM   staging_events e
    WHERE (page = 'NextSong'
           AND userid NOT IN (SELECT DISTINCT userid
                             FROM   users)); 
    """)

    song_table_insert = ("""
    INSERT INTO {}
                (songid,
                 title,
                 artistid,
                 year,
                 duration)
    SELECT DISTINCT s.song_id   AS songid,
                    s.title     AS title,
                    s.artist_id AS artistid,
                    s.year      AS year,
                    s.duration  AS duration
    FROM   staging_songs s;
    """)

    artist_table_insert = ("""
    INSERT INTO {}
                (artistid,
                 name,
                 location,
                 lattitude,
                 longitude)
    SELECT DISTINCT s.artist_id        AS artistid,
                    s.artist_name      AS name,
                    s.artist_location  AS location,
                    s.artist_latitude  AS lattitude,
                    s.artist_longitude AS longitude
    FROM   staging_songs s; 
    """)

    time_table_insert = ("""
    INSERT INTO {}
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
    FROM   songplays; 
    """)