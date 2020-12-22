import configparser

# AWS RedShift DB Developmment References:
# --> dtypes: https://docs.aws.amazon.com/de_de/redshift/latest/dg/c_Supported_data_types.html
# --> CREATE: https://docs.aws.amazon.com/de_de/redshift/latest/dg/r_CREATE_TABLE_examples.html
# primary key(salesid),
# foreign key(listid) references listing(listid),
# foreign key(sellerid) references users(userid),
# foreign key(buyerid) references users(userid),
# foreign key(dateid) references date(dateid))
# distkey(listid)
# compound sortkey(listid,sellerid);

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

# CREATE TABLES --> see RedShift dtypes: https://docs.aws.amazon.com/de_de/redshift/latest/dg/c_Supported_data_types.html
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist          VARCHAR
        , auth          VARCHAR
        , firstName     VARCHAR
        , gender        VARCHAR
        , itenInSession VARCHAR
        , lastName      VARCHAR
        , length        DECIMAL
        , level         VARCHAR
        , location      VARCHAR
        , method        VARCHAR
        , page          VARCHAR
        , registration  VARCHAR
        , sessionId     INTEGER
        , song          VARCHAR
        , status        INTEGER
        , ts            BIGINT
        , userAgent     VARCHAR
        , userId        INTEGER
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs           INTEGER
        , artist_id           VARCHAR
        , artist_latitude     DECIMAL
        , artist_longitude    DECIMAL
        , artist_location     VARCHAR
        , artist_name         VARCHAR
        , song_id             VARCHAR
        , title               VARCHAR
        , duration            DECIMAL
        , year                INTEGER
        )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id     INTEGER IDENTITY(0,1) PRIMARY KEY
        , start_time    TIMESTAMP REFERENCES time(start_time)
        , user_id       INTEGER REFERENCES users(user_id)
        , level         VARCHAR
        , song_id       VARCHAR REFERENCES songs(song_id)
        , artist_id     VARCHAR REFERENCES artists(artist_id)
        , session_id    INTEGER
        , location      VARCHAR
        , user_agent    VARCHAR
    )
""")

user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users(
            user_id         VARCHAR PRIMARY KEY
            , first_name    VARCHAR
            , last_name     VARCHAR
            , gender        VARCHAR
            , level         VARCHAR
        )
        DISTSTYLE ALL
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id     VARCHAR PRIMARY KEY 
        , title     VARCHAR
        , artist_id VARCHAR NOT NULL
        , year      INTEGER
        , duration  DECIMAL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id       VARCHAR PRIMARY KEY
        , name          VARCHAR
        , location      VARCHAR
        , latitude      DECIMAL
        , longitude     DECIMAL
    )
    DISTSTYLE ALL
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time      TIMESTAMP PRIMARY KEY
        , hour          INTEGER
        , day           INTEGER
        , week          INTEGER
        , month         INTEGER
        , year          INTEGER
        , weekday       INTEGER
    )
    DISTSTYLE ALL
""")

# STAGING TABLES
# predefined copy commands parametrized by user data
staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    REGION 'us-west-2'
""").format(config.get('S3','LOG_DATA')
            , config.get('IAM_ROLE','ARN')
            , config.get('S3','LOG_JSONPATH')
            )


staging_songs_copy = ("""
    copy staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2'
""").format(config.get('S3','SONG_DATA')
            , config.get('IAM_ROLE','ARN')
            )

# FINAL TABLES
# ref. epoche (unix timestamp) to timestamp in RedShift: https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time
        , user_id
        , level
        , artist_id
        , song_id
        , session_id
        , location
        , user_agent)
        SELECT DISTINCT TIMESTAMP 'epoch' + tbl_se.ts/1000 * INTERVAL '1 second' AS start_time
            , tbl_se.userId     AS user_id
            , tbl_se.level      AS level
            , tbl_ss.artist_id  AS artist_id
            , tbl_ss.song_id    AS song_id
            , tbl_se.sessionId  AS session_id
            , tbl_se.location   AS location
            , tbl_se.userAgent  AS user_agent
        FROM staging_events AS tbl_se
        JOIN staging_songs AS tbl_ss ON tbl_se.artist = tbl_ss.artist_name
            AND tbl_se.song = tbl_ss.title
            AND tbl_se.length = tbl_ss.duration
        WHERE lower(tbl_se.page) = 'nextsong'
""")


user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT tbl_se.userId   AS user_id
        , tbl_se.firstName          AS first_name
        , tbl_se.lastName           AS last_name
        , tbl_se.gender             AS gender
        , tbl_se.level              AS level
    FROM staging_events AS tbl_se
    WHERE lower(tbl_se.page) = 'nextsong'
        AND tbl_se.userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT tbl_ss.song_id  AS song_id
        , tbl_ss.title              AS title
        , tbl_ss.artist_id          AS artist_id
        , tbl_ss.year               AS year
        , tbl_ss.duration           AS duration
    FROM staging_songs AS tbl_ss
    WHERE tbl_ss.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT tbl_ss.artist_id    AS artist_id
        , tbl_ss.artist_name            AS name
        , tbl_ss.artist_location        AS location
        , tbl_ss.artist_latitude        AS latitude
        , tbl_ss.artist_longitude       AS longitude
    FROM staging_songs AS tbl_ss 
    WHERE tbl_ss.artist_id IS NOT NULL
""")

# ref. Extract TimeStamp elements: https://docs.aws.amazon.com/de_de/redshift/latest/dg/r_EXTRACT_function.html
# using "songplays" table for higher integrity
time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time
        , EXTRACT(hour FROM start_time)  AS hour
        , EXTRACT(day FROM start_time)  AS day
        , EXTRACT(week FROM start_time)  AS week
        , EXTRACT(month FROM start_time)  AS month
        , EXTRACT(year FROM start_time)  AS year
        , EXTRACT(dow FROM start_time)  AS weekday -- instead I might need to use the command below
        -- , DATE_PART(dow, start_time) AS weekday
    FROM songplays
    WHERE start_time IS NOT NULL
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
