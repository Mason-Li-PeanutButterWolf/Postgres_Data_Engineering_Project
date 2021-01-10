# Sparkify Database ETL with Postgres

The purpose of this project is to provide a database solution for Sparkify to analyse their song and user activity data. The following was created:

  * An ETL pipeline was developed to move data into a Postgres database
  * A star schema is used to optimise data write times by reducing duplicate data entries
  * All CRUD queries are given as well as starter notebooks to verify operation on a subset of the data using given functions

## Datasets

There are two datasets for this project, namely: song and log datasets.

#### Song Dataset
Song files are partitioned by the first three letters of each song's track ID and are a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/).  The following filepath and its content are given as an example:

> song_data/A/A/B/TRAABJL12903CDCF1A.json

> {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

#### Log Dataset

Log files are partitioned by year and month as follows and is generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations.:

> log_data/2018/11/2018-11-13-events.json

## Song Play Analysis Schema

A Star schema is used to model the available data. This allows for flexible adhoc querying by the analytics team in the future. 

#### Fact Table

| songplays |
| --- |
| songplay_id |
| start_time |
| user_id |
| level |
| song_id |
| artist_id |
| session_id |
| location |
| user_agent |
> Records in log data associated with song plays i.e. records with page

#### Dimension Tables

| users  |
| --- |
| user_id |
| first_name |
| last_name |
| gender |
| level |
> App users 

| songs   |
| --- |
| song_id |
| title |
| artist_id |
| year |
| duration |
> Songs in music database

| artists    |
| --- |
| artist_id |
| name |
| location |
| lattitude |
| longitude |
> Artists in music database

| time     |
| --- |
| start_time |
| hour |
| day |
| week |
| month |
| year |
| weekday |
> Timestamps of records in songplays broken down into specific units

## File descriptions

`data` folder contains partitioned song and log data.

`sql_queries.py` is arepository of the SQL queries that are execued by other files.

`create_tables.py` drops and reinitialises the database environment using queries from `sql_queries.py`.

`test.ipynb` is provided to verify the creation of the database tables by `create_tables.py` and that data is loaded by `etl.ipynb` and `etl.py`.

`etl.ipynb` loads and transforms a single file from `songdata` and `log_data` and is used to verify the insert scripts in `sql_queries.py`.

`etl.py` performs the massive data extraction from JSON files in `data` folder, transforms it into the defined types and tables groups then uploads it to the Sparkify Database.

## Running the ETL pipeline

0  Ensure the `data` folder and all project files are downloaded and that all dependencies are met. Replace the given connection strings in `create_tables.py`, `etl.ipynb` and `etl.py` with your own, pointing to a postgres database server you have set up.

1  Run `create_tables.py` to initialise the database.

2  Start and run `test.ipynb` to ensure all tables were created. Restart the kernel.

3  Start and run `etl.ipynb` to ensure the transformation and loading scripts work for a single song and log file. Restart the kernel.

4  Run `create_tables.py` to reinitialise the database.

5  Run `etl.py` to start the ETL data pipeline from JSON logs in the `data` folder to the Sparkify database
