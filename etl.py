import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import uuid

def flat_list(list_of_list):
    """
    Description: This function flatten a list of list to a one dimentional list.
    Arguments:
        list_of_list: a list of list
    Returns:
        The flattened one dimentional list
    """
    return [item for sublist in list_of_list for item in sublist]


def process_song_file(cur, filepath):

    """
    Description: This function first read in a song json file into a panda dataframe,
    and then extract relavent columns for the songs/artists table into a list of values 
    before loading them into the respective database.

    Arguments:
        cur: the cursor object.
        filepath: song data file path.

    Returns:
        None
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = flat_list(df[['song_id','title','artist_id','year','duration']].values.tolist())
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = flat_list(df[['artist_id','artist_name','artist_longitude','artist_latitude','artist_longitude']].values.tolist())
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):

    """
    Description: This function first read in a log json file into a panda dataframe,
    and then extract and transform relavent columns for the time/user/playsongs table
    into a list of values before loading them into the respective database.

    Arguments:
        cur: the cursor object.
        filepath: song data file path.

    Returns:
        None
    """



    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong'].reset_index()

    # convert timestamp column to datetime
    t = pd.to_datetime(df.loc[:, ('ts')],unit = 'ms')
    df['ts'] = t
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ("start_time","hour","day","week_f_year","month","year","weekday")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)),columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record 
        songplay_data = (str(uuid.uuid4()),row.ts,row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

   """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = conn = psycopg2.connect(host="localhost",
                                database="sparkifydb",
                                user="postgres",
                                password="Ban3duwe")
    # conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='D:\data_engineering_project\Data_Modeling_Postgres\data\song_data', func=process_song_file)
    process_data(cur, conn, filepath='D:\data_engineering_project\Data_Modeling_Postgres\data\log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()