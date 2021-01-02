import os
import io
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def clean_csv_value(value):
    """
    This procedure converts a given value to a format consistent with Postgres copy_from method
    ref: https://hakibenita.com/fast-load-data-python-postgresql#copy
    INPUTS: 
    * value the value of an attribute
    RETURNS:
    * value the value of attribute consistent with Postgres copy_from method
    """

    # replace None with \N
    if value is None:
        return r'\N'
    # convert value to string and add extra '\' to '\n'
    return str(value).replace('\n', '\\n')


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(
        df[['song_id', 'artist_id', 'title', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location',
                           'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It filters the log file on NextSong action and then extracts the time information,
    user information, and finally song plays data saving all three in their
    respective tables.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day,
                 t.dt.month, t.dt.year, t.dt.day_name())
    column_labels = ('start_time', 'hour', 'day', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    # bulk insert time_df into Postgres
    # create a csv like object
    output = io.StringIO()
    time_df.to_csv(output, sep='\t', index=False, header=False)
    # move the pointer to start of the file
    output.seek(0)
    # creating a temp table to handle conflict due to duplicate insert
    # ref: https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin-with-csv-do-on-conflic-do-update
    cur.execute(time_tmp_table)
    # copy data from csv to temp table
    cur.copy_from(output, 'tmp_table')
    # merge temp table with main table
    cur.execute(time_table_bulk_insert)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    # create a csv like object
    output = io.StringIO()
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        # insert songplay record
        songplay_data = (t[index], row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent)
        # write to csv like object
        output.write('\t'.join(map(clean_csv_value, songplay_data)) + '\n')
    # move the pointer to start of the csv like object
    output.seek(0)
    # columns to insert (songplay_id is a serial insert)
    columns = ['start_time', 'user_id', 'level', 'song_id',
               'artist_id', 'session_id', 'location', 'user_agent']
    # copy data to songplays table
    cur.copy_from(output, 'songplays', columns=columns)


def process_data(cur, conn, filepath, func):
    """
    This procedure extracts all the json files that is either in the filepath directory or a sub directory.
    Then it applies the func procedure to the filepath and the cursor variable.

    INPUTS: 
    * cur the cursor variable
    * conn the connection variable
    * filepath the file path to the song file
    * func the function to apply to filepath and cursor variable
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """
    This procedure connects to the database and then processes both song data and
    log data. Finally, it closes the connection to the database.
    """

    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
