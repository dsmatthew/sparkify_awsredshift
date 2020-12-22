# sparkify_awsredshift (Udacity Data Engineering Nano Degree - Project 3)
> copy on [github](https://github.com/dsmatthew/sparkify_awsredshift)

Within this project the startup "Sparkify" would like to build a PostgreSQL database to analyze their logged user data. They collect data about songs played (streamed) by their users. This analysis should prepare to understand what their customers are listening to.

The raw data is provided within multiple json files differentiated by song data (songs, artists, albums etc.) and user action log data (users listening to music, navigating within the streaming application etc.).



### Table of Contents

1. [Installation](#installation)
2. [Project Motivation](#motivation)
3. [File Descriptions](#files)
4. [Results](#results)
5. [Licensing, Authors, and Acknowledgements](#licensing)

## Installation <a name="installation"></a>

You need to have a Python 3.x environment and a PostgreSQL database. I used the built-in workspace solution provided by Udacity. You might need to install the "psycopg2" library first.

Used libraries:
* psycopg2
* sql_queries (custom file, containing all the SQL queries)

HINT FOR USAGE:
0. Prepare your AWS account (IAM role and redshift roles are required)
0.1. Create an IAM Role on your AWS account
0.2. Get your S3 Bucket access (read-only) and extract the API information (key & secret key)
0.3. Create a policy group on your AWS cluster to open the cluster endpoint for all backends (security issue: use this only for your tests)
0.4. Create & run a redshift cluster with attached IAM role
1. Add configuration parameters to file _dwh.cfg_ (a sample file is attached)
2. Run _create_tables.py_ to create the staging tables and DWH tables.
3. Run _etl.py_ to import json files into the 2 stagings tables, afterwards insert data into DWH tables.
4. WARNING: Do not forget to delete your Redshift cluster (due to costs).

## Project Motivation<a name="motivation"></a>
Within this project I will create a Redshift database including the ETL pipeline in Python as preparation for further analytics. As described above, there are two types of files: song data and user-action-log data.
All data sets are provided within the online course by Udacity (www.udacity.com).

## File Descriptions <a name="files"></a>
* dwh.cfg - contains the AWS configuration parameters (e.g. your IAM role name, API credentials...)
* create_tables.py - provides functions to interact with the database (create, drop tables...).
* sql_queries.py - contains the sql query statements.
* etl.ipynb / etl.py - contains the ETL process to load json files into the previously created tables.



## Results<a name="results"></a>

** Database schema**
As a result the following database schema is created:

* **Fact Tables**
 * **songplays** - records in log data associated with song plays i.e. records with page _NextSong_.
   <p>songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent</p>
   

* **Dimension Tables**
 * **users** - users in the app
   <p>user_id, first_name, last_name, gender, level</p>
 * **songs** - songs in music database
   <p>song_id, title, artist_id, year, duration</p>
 * **artists** - artists in music database
   <p>artist_id, name, location, latitude, longitude</p>
 * **time** - timestamps of records in **songplays**
   <p>start_time, hour, day, week, month, year, weekday</p>


**ETL process**
The _etl.py_ script is used to extract the content from the json files - in detail the process works as followed:
1. Load data into staging:
 * Access the configured S3 bucket (see _dwh.cfg_) and copy the json files into staging tables (songs and user interaction logs).
 * All json files will be placed into the staging tables (_staging_songs_ & _staging_events_) on the Redshift cluster by running the _COPY command_ as iteration.
2. Loading & Transformation for DHW tables:
 * all DWH tables will be loaded with the previously imported data from both staging tables
 * Column constraints are added (primary keys) along with the creation of a new _time dimension table_ (incl. extraction of various time measures).


## Licensing, Authors, Acknowledgements<a name="licensing"></a>
I used data provided within the Nano Degree of Udacity. Including some descriptions belong to their documentations. All the licensing for the data and other information can be found on [Udacity](https://udacity.com)
