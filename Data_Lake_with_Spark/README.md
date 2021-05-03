# Project: Data Lake

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This projects aims to create an extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables for the analytics team to continue finding insights in what songs their users are listening to.

---

## Sample Data

#### Location

S3 links for the datasets for songs and logs:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

#### Song Data

Each file is in JSON fomart date file contains metadata about a song and the artist of that song.

```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

#### Log Data

Each file is in JSON fomart log file contains activity logs from a music streaming app based on specified configurations.

```json
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}## Database Schema
```

Because the key names of log data json files might be different from the columns names of the table to be loaded. We will need an additonal json path file to parse the data.

---

## Database Schema

#### Database Schema Diagram

#### Fact Table

###### songplay

| Column      | Type               |
| ----------- | ------------------ |
| songplay_id | SERIAL PRIMARY KEY |
| start_time  | timestamp          |
| user_id     | int                |
| level       | varchar            |
| song_id     | varchar            |
| artist_id   | varchar            |
| session_id  | int                |
| location    | varchar            |
| user_agent  | varchar            |

#### Dimension Tables

###### **users**

| Column     | Type            |
| ---------- | --------------- |
| user_id    | int PRIMARY KEY |
| first_name | varchar         |
| last_name  | varchar         |
| gender     | varchar         |
| levek      | varchar         |

###### **songs**

| Column    | Type                |
| --------- | ------------------- |
| song_id   | varchar PRIMARY KEY |
| title     | varchar             |
| artist_id | varchar             |
| year      | varchar             |
| duration  | varchar             |

###### **artists**

| Column    | Type                |
| --------- | ------------------- |
| artist_id | varchar PRIMARY KEY |
| name      | varchar             |
| location  | varchar             |
| latitude  | float               |
| longitude | float               |

###### **times**

| Column     | Type                  |
| ---------- | --------------------- |
| start_time | timestamp PRIMARY KEY |
| hour       | int                   |
| day        | int                   |
| week       | int                   |
| month      | int                   |
| year       | int                   |
| weekday    | int                   |

---

## ETL Pipeline

#### Design

The ETL reads log data file from S3, and extracts users, time, songplays tables from log data. The data in created tables will write users, time, songplays tables to parquet files. Tables will be partitioned by following columns:

- songplays:  year, month

- time: year, month

- songs: year, artist_id

#### Execution

To run the ETL, user will need to add IAM role info to `dwh.cfg`.

After completing the config file, execute the following commands:

```shell
python etl.py
```

The sample data in songplays table:

| songplay_id | start_time                 | user_id | level | song_id | artist_id | session_id | location                        | user_agent                                                              |
| ----------- | -------------------------- | ------- | ----- | ------- | --------- | ---------- | ------------------------------- | ----------------------------------------------------------------------- |
| 1           | 2018-11-30 00:22:07.796000 | 91      | free  | None    | None      | 829        | Dallas-Fort Worth-Arlington, TX | Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0) |


