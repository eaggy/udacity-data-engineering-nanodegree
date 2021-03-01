import os
import glob
import psycopg2
import pandas as pd
from datetime import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Read one song file given by 'filepath' and
    insert song record and artist record in database
    """
    # Create empty dataframe
    df = pd.DataFrame()
    
    # Open song file and read it do df
    try:
        df = pd.read_json(filepath, lines=True)
    except Exception as e:
        print('Fail to read file: {}'.format(filepath))
        print(e)

    if not df.empty:
        # Insert song record to db
        song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
        cur.execute(song_table_insert, song_data)
    
        # Insert artist record to db
        artist_data = list(df[['artist_id', 'artist_name', 'artist_location',
                       'artist_latitude', 'artist_longitude']].values[0])
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Read one log file given by 'filepath' and
    insert time data record, user record, and songplay record in database
    """
    # Create empty dataframe
    df = pd.DataFrame()
    
    # Open log file
    try:
        df = pd.read_json(filepath, lines=True)
    except Exception as e:
        print('Fail to read file: {}'.format(filepath))
        print(e) 

    if not df.empty:    
        # Filter by NextSong action
        df = df.loc[df['page'] == 'NextSong',:] 

        # Convert timestamp column to datetime
        t = df.copy()
        t['ts'] = t['ts'].apply(lambda x: datetime.fromtimestamp(x/1000.0))
    
        # Insert time data records
        time_data = (t['ts'].values, t['ts'].dt.hour.values, t['ts'].dt.day.values,
            t['ts'].dt.week.values, t['ts'].dt.month.values,
            t['ts'].dt.year.values, t['ts'].dt.dayofweek.values)
        column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') 
        time_df = pd.DataFrame(dict(zip(column_labels, time_data)), columns=column_labels) 

        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))

        # Load user table
        user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 

        # Insert user records
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
        
        # Insert songplay records
        for index, row in df.iterrows():
        
            # Get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # Insert songplay record
            songplay_data = (datetime.fromtimestamp(row.ts/1000.0), row.userId, row.level, songid,
                             artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
        

def process_data(cur, conn, filepath, func):
    """
    - Get list of files located in `filepath`.
    
    - Process all these files and populate database.
    """
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Process song data.  
    
    - Process log data. 
    
    - Finally, closes the connection. 
    """
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print('Fail to connect to sparkifydb database')
        print(e)
     
    if cur:
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)

        cur.close()
        conn.close()


if __name__ == "__main__":
    main()