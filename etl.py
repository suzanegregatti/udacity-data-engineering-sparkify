import os
import glob
from io import StringIO
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """"
    - Read the JSON song file found in the filepath informed and convert to a pandas DataFrame.
    - Select the columns to be imported to the songs and artists tables.
    - Insert data to the respective tables in the database.
    Args:
        cur (cursor): cursor to sparkifydb.
        filepath (string): file path of the JSON file to be imported to the database.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Read the JSON log file found in the filepath informed and convert to a pandas DataFrame.
    - Filter the records with action = NextSong
    - Select the columns to be imported to the users and songplays tables.
    - Treat the timestamp column and extract relevant information to additional columns.
    - Update the tables time, temp_log, users and songplays.
    Args:
        cur (cursor): cursor to sparkifydb.
        filepath (string): file path of the JSON file to be imported to the database.    
    """    
    # open log file
    df = pd.read_json(filepath, lines=True)   

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    
    # select the columns to be used
    df = df[['artist', 'firstName', 'gender', 'lastName', 'length', 'level', 'location', 'sessionId', 'song', 'ts', 'userAgent', 'userId']]    
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = t
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))   
       
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    # save log data as csv in memory
    f = StringIO()
    df.to_csv(f, index=False, header=False, sep='|')
    f.seek(0)    
    try:
        # load log table
        cur.execute(temp_log_delete)
        cur.copy_from(f, 'temp_log', sep='|')
        # load user table
        cur.execute(user_table_insert)
        # load songplay table
        cur.execute(songplay_table_insert)   
        # delete data from log table
        cur.execute(temp_log_delete)
    except (Exception, psycopg2.DatabaseError) as err:
        print(err)

def process_data(cur, conn, filepath, func):
    """
    - Get the files paths from a specific directory.
    - For each file, apply the function that process this type of file.    
    - Save the transactions to the database.
    Args:
        cur (cursor): cursor to sparkifydb.
        conn (connection): connection to sparkifydb.
        filepath (string): file path of the JSON file to be imported to the database.
        fun (function): function to be applied.
    """
    # get all files matching extension from directory
    all_files = []
    files = glob.glob(filepath + '/**/*.json', recursive=True)
    all_files = [os.path.abspath(f) for f in files]

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connects to the sparkifydb database and get its cursor.
    - Process the JSON files for song and log data.
    - Close the connection.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()