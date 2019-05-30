import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# Example of a staging table row: {"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar(max),
    auth varchar(max),
    firstName varchar(max),
    gender char(1),
    itemInSession numeric(4,0),
    lastName varchar(max),
    length varchar(max),
    level varchar(max),
    location varchar(max),
    method varchar(max),
    page varchar(max),
    registration double precision,
    sessionId numeric(20,0),
    song varchar(max),
    status numeric(4,0),
    ts numeric(20,0),
    userAgent varchar(max),
    userId varchar(max)
)
""")

# Example of a staging table row: {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id varchar(max),
    num_songs varchar(max),
    title varchar(max),
    artist_id varchar(max),
    artist_latitude varchar(max),
    artist_longitude varchar(max),
    artist_location varchar(max),
    artist_name varchar(max),
    duration double precision,
    year numeric(4,0)
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_key int identity(0,1) not null primary key,
    start_time timestamp not null references time(start_time),
    user_key int references users(user_key),
    song_key int references songs(song_key) distkey sortkey,  
    artist_key int references artists(artist_key),
    session_id double precision not null,
    location varchar(60),
    level varchar(12),
    user_agent varchar(max) not null
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_key int identity(0,1) not null primary key sortkey,
    user_id varchar(20) not null unique,
    first_name varchar(60),
    last_name varchar(60),
    level varchar(12),
    gender char(1)
)
diststyle even
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_key int identity(0,1) not null primary key sortkey,
    artist_id varchar(20) not null unique,
    name varchar(250) not null,
    location varchar(max),
    latitude double precision,
    longitude double precision
)
diststyle all
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_key int identity(0,1) primary key distkey sortkey,
    song_id varchar(20) unique,
    num_songs numeric(3,0) not null,
    title character varying(250) not null,
    artist_key int not null references artists(artist_key),
    year numeric(4,0) not null,
    duration double precision
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp not null primary key sortkey,
    hour numeric(2,0) not null,
    day numeric(2,0) not null,
    week numeric(2,0) not null,
    month numeric(2,0) not null,
    year numeric(4,0) not null,
    weekday varchar(20) not null
)
diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {}
    compupdate off region 'us-west-2';
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json {}
    compupdate off region 'us-west-2';
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["SONG_JSONPATH"])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_key, level, song_key, artist_key, session_id, location, user_agent)
SELECT timestamp 'epoch' + ts * interval '0.001 second' start_time, u.user_key, e.level, s.song_key, a.artist_key, e.sessionId, e.location, e.userAgent
  FROM staging_events e 
  LEFT OUTER JOIN artists a ON e.artist = a.name 
  LEFT OUTER JOIN songs s ON e.song = s.title AND s.artist_key = a.artist_key
  LEFT OUTER JOIN users u ON e.userId = u.user_id
 WHERE page = 'NextSong'
""")

user_table_update = ("""
UPDATE users SET level = new_level
  FROM (SELECT userId, MAX(level) new_level FROM staging_events GROUP BY userId) n, users o 
 WHERE n.userId = o.user_id AND new_level <> level
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT userId, MIN(firstName), MIN(lastName), MIN(gender), MAX(level)
  FROM staging_events n
 WHERE NOT EXISTS (SELECT '' FROM users o WHERE n.userId = o.user_id)
 GROUP BY userId
""")

song_table_insert = ("""
INSERT INTO songs(song_id, num_songs, title, artist_key, year, duration)
SELECT song_id, MIN(num_songs), MIN(title), MIN(a.artist_key), MIN(year), MIN(duration)
  FROM staging_songs n LEFT OUTER JOIN artists a ON n.artist_id = a.artist_id
 WHERE NOT EXISTS (SELECT '' FROM songs o WHERE n.song_id = o.song_id)
 GROUP BY song_id
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT artist_id, MIN(artist_name), MIN(artist_location), FLOAT8(MIN(artist_latitude)), FLOAT8(MIN(artist_longitude))
  FROM staging_songs n
 WHERE NOT EXISTS (SELECT '' FROM artists o WHERE n.artist_id = o.artist_id)
 GROUP BY artist_id
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
WITH beg AS (SELECT DISTINCT ts FROM staging_events),
stamps AS (SELECT timestamp 'epoch' + ts * interval '0.001 second' start_time FROM beg)
SELECT start_time, EXTRACT(HOUR FROM start_time), EXTRACT(DAY FROM start_time), EXTRACT(WEEK FROM start_time), EXTRACT(MONTH FROM start_time), EXTRACT(YEAR FROM start_time), EXTRACT(WEEKDAY FROM start_time)
  FROM stamps n
 WHERE NOT EXISTS (SELECT '' FROM time o WHERE o.start_time = n.start_time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [time_table_insert, user_table_update, user_table_insert, artist_table_insert, song_table_insert, songplay_table_insert]