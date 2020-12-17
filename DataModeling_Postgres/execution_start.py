"""
This is the script to be executes as ETL
It will create all the necessary tables and insert the available data
"""
from os import listdir
import pandas as pd
import psycopg2
import datetime

def list_files(*args, **kwargs):
    """
    This function is to be called to return a list of files in data path;
    It will be returned the files in subfolder log or data, wich one will depend on the informed 'prefix',
    by default is returned 'log_data' folder, to return data subfolder on th call of the function use,
    prefix='data';
    """
    prefix = kwargs.get('prefix', 'log')
    list_files = listdir(f'data/{prefix}_data/')
    return list_files

def return_df_file(file, *args, **kwargs):
    """
    This function read a specific file and return a pandas DF;
    To call this function pass the file name variable;
    By default the function read the file in log_data subfolder, to change that inform prefix='data' in function call;
    """
    prefix = kwargs.get('prefix', 'log')
    df = pd.read_json(f'data/{prefix}_data/{file}', lines=True)
    return df

def structured_db():
    """
    This function is to be called to drop/create the necessary tables for this ETL;
    """
    conn = psycopg2.connect(host="postgres_udacity", dbname="udacity", user="udacity", password="udacity")
    cur = conn.cursor()
    conn.autocommit = True
    cur.execute("""DROP TABLE IF EXISTS sparkifydb.songplays;""")
    cur.execute("""DROP TABLE IF EXISTS sparkifydb.users;""")
    cur.execute("""DROP TABLE IF EXISTS sparkifydb.songs;""")
    cur.execute("""DROP TABLE IF EXISTS sparkifydb.artists;""")
    cur.execute("""DROP TABLE IF EXISTS sparkifydb.time;""")
    print('Tables Droped')
    cur.execute("""DROP SCHEMA IF EXISTS sparkifydb;""")
    print('Schema Droped')
    cur.execute("""CREATE SCHEMA IF NOT EXISTS sparkifydb;""")
    print('Schema Created')
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.users(user_id varchar, first_name varchar, last_name varchar, gender varchar, level varchar, PRIMARY KEY(user_id));""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.songs(song_id varchar, title varchar, artist_id varchar, year int, duration varchar, PRIMARY KEY(song_id));""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.artists(artist_id varchar, artist_name varchar, artist_location varchar, artist_latitude varchar, artist_longitude varchar, PRIMARY KEY(artist_id));""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.time(start_time varchar, hour int, day int, week int, month int, year int, weakday varchar, PRIMARY KEY(start_time));""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.songplays(
                    songplay_id serial PRIMARY KEY,
                    start_time varchar,
                    user_id varchar, 
                    level varchar, 
                    song_id varchar, 
                    artist_id varchar, 
                    session_id int, 
                    location varchar, 
                    user_agent varchar,
                    CONSTRAINT fk_users
                        FOREIGN KEY(user_id)
                        REFERENCES sparkifydb.users (user_id)
                        ON DELETE CASCADE,
                    CONSTRAINT fk_songs
                        FOREIGN KEY(song_id)
                        REFERENCES sparkifydb.songs (song_id)
                        ON DELETE CASCADE,
                    CONSTRAINT fk_artists
                        FOREIGN KEY(artist_id)
                        REFERENCES sparkifydb.artists (artist_id)
                        ON DELETE CASCADE,
                    CONSTRAINT fk_time
                        FOREIGN KEY(start_time)
                        REFERENCES sparkifydb.time (start_time)
                        ON DELETE CASCADE
                );""")
    print('Tables Created')

def insert_songs():
    """
    This function is to be called to insert data in the previously created table 'songs';
    To do this insert a few data treatment is necessary, like remove ' from data (condition in line 96 through 99);
    Also is necessary to create an Undefined song (id=0);
    """
    files = list_files(prefix='song')
    conn = psycopg2.connect(host="postgres_udacity", dbname="udacity", user="udacity", password="udacity")
    conn.autocommit = True
    for file in files:
        df = return_df_file(file, prefix='song')
        print(df.head())
        df_insert = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
        list_song = []
        for i, line in df_insert.iterrows():
            if ("'" in line.title):
                a = str(line.title)
                a = a.replace("'","´")
                list_song.append(a)
            else:
                list_song.append(line.title)
        
        df_insert['title'] = list_song

        df = pd.DataFrame([['0', 'Undentified', '0', '0', '0']], columns=['song_id', 'title', 'artist_id', 'year', 'duration'])
        frames = [df_insert, df]
        df_insert = pd.concat(frames)

        print(df_insert.head())

        sql_insert = '''INSERT INTO sparkifydb.songs
                        (song_id, title, artist_id, year, duration) 
                        values (%s,%s,%s,%s,%s)
                        ON CONFLICT (song_id) 
                        DO 
                        UPDATE SET
                        title = excluded.title,
                        artist_id = excluded.artist_id,
                        year = excluded.year,
                        duration = excluded.duration;'''

        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql_insert,df_insert.values.tolist())
                print('Commit')
        except Exception as e:
            print(e)

def insert_artists():
    """
    This function is to be called to insert data in the previously created table 'artists';
    To do this insert a few data treatment is necessary, like remove ' from data (condition in line 144 through 147);
    Also is necessary to create an Undefined artist (id=0);
    """
    files = list_files(prefix='song')
    conn = psycopg2.connect(host="postgres_udacity", dbname="udacity", user="udacity", password="udacity")
    conn.autocommit = True
    for file in files:
        df = return_df_file(file, prefix='song')
        print(df.head())
        df_insert = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
        list_name = []
        for i, line in df_insert.iterrows():
            if ("'" in line.artist_name):
                a = str(line.artist_name)
                a = a.replace("'","´")
                list_name.append(a)
            else:
                list_name.append(line.artist_name)
        
        df_insert['artist_name'] = list_name

        df = pd.DataFrame([['0', 'Undentified', '0', '0', '0']], columns=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
        frames = [df_insert, df]
        df_insert = pd.concat(frames)
        
        print(df_insert.head())

        sql_insert = '''INSERT INTO sparkifydb.artists
                        (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) 
                        values (%s,%s,%s,%s,%s)
                        ON CONFLICT (artist_id) 
                        DO 
                        UPDATE SET
                        artist_name = excluded.artist_name,
                        artist_location = excluded.artist_location,
                        artist_latitude = excluded.artist_latitude,
                        artist_longitude = excluded.artist_longitude;'''

        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql_insert,df_insert.values.tolist())
                print('Commit')
        except Exception as e:
            print(e)

def insert_users():
    """
    This function is to be called to insert data in the previously created table 'users';
    To do this insert is necessary to create an Undefined user (id=0);
    """
    files = list_files()
    conn = psycopg2.connect(host="postgres_udacity", dbname="udacity", user="udacity", password="udacity")
    conn.autocommit = True
    for file in files:
        df = return_df_file(file)
        print(df.head())
        df_insert = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
        df_insert.columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
        print(df_insert.head())

        df = pd.DataFrame([['0', 'Undentified', 'Undentified', '0', '0']], columns=['user_id', 'first_name', 'last_name', 'gender', 'level'])
        frames = [df_insert, df]
        df_insert = pd.concat(frames)

        sql_insert = '''INSERT INTO sparkifydb.users
                        (user_id, first_name, last_name, gender, level) 
                        values (%s,%s,%s,%s,%s)
                        ON CONFLICT (user_id) 
                        DO 
                        UPDATE SET
                        first_name = excluded.first_name,
                        last_name = excluded.last_name,
                        gender = excluded.gender,
                        level = excluded.level;'''

        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql_insert,df_insert.values.tolist())
                print('Commit')
        except Exception as e:
            print(e)

def insert_time():
    """
    This function is to be called to insert data in the previously created table 'time';
    To do this insert is necessary to break the column 'ts' in separate variables;
    """
    files = list_files()
    conn = psycopg2.connect(host="postgres_udacity", dbname="udacity", user="udacity", password="udacity")
    conn.autocommit = True
    for file in files:
        df = return_df_file(file)
        print(df.head())
        df_used = df[['ts', 'page']]
        df_used = df_used[df_used['page'] == 'NextSong']
        df_used = df_used[['ts']]

        df_final = pd.DataFrame(columns=['start_time', 'hour', 'day', 'week', 'month', 'year', 'weakday'])
        list_final = []
        list_year = []
        list_month = []
        list_day = []
        list_hour = []
        list_week = []
        list_weekday = []

        for i, line in df_used.iterrows():
            a = pd.to_datetime(line.ts, unit='ms')
            hour = a.strftime('%H')
            day = a.strftime('%d')
            month = a.strftime('%m')
            year = a.strftime('%Y')
            week = datetime.date(int(year), int(month), int(day)).isocalendar()[1]
            weekday = a.strftime('%A')
            list_final.append(a)
            list_year.append(int(year))
            list_month.append(int(month))
            list_day.append(int(day))
            list_hour.append(int(hour))
            list_week.append(int(week))
            list_weekday.append(weekday)
        
        df_final['start_time'] = list_final
        df_final['hour'] = list_hour
        df_final['day'] = list_day
        df_final['week'] = list_week
        df_final['month'] = list_month
        df_final['year'] = list_year
        df_final['weakday'] = list_weekday

        sql_insert = '''INSERT INTO sparkifydb.time
                        (start_time, hour, day, week, month, year, weakday) 
                        values (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (start_time) 
                        DO 
                        UPDATE SET
                        hour = excluded.hour,
                        day = excluded.day,
                        week = excluded.week,
                        month = excluded.month,
                        year = excluded.year,
                        weakday = excluded.weakday;'''

        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql_insert,df_final.values.tolist())
                print('Commit')
        except Exception as e:
            print(e)
    
def return_song_id(song, conn):
    """
    This function is to be called to return a pandas DF containing a song_id and artist_id from table 'song';
    To do this is necessary to pass the song_name and connection to DB where the song table is located;
    This function is important for the insetion of data in the table 'songplays';
    """
    sql = f'''select song_id, artist_id from sparkifydb.songs where title = '{song}';'''
    print(sql)
    df = pd.read_sql(sql, conn)
    return df

def insert_songplays():
    """
    This function is to be called to insert data in the previously created table 'songplays';
    To do this insert a few data treatment is necessary, like remove ' from data (condition in line 313 through 315);
    The previously created id=0 in tables artists, users and song is important to satisfy FOREIGN KEY condition (it's necessary to exist in referenced table);
    """
    files = list_files()
    conn = psycopg2.connect(host="postgres_udacity", dbname="udacity", user="udacity", password="udacity")
    conn.autocommit = True
    for file in files:
        df = return_df_file(file)
        print(df.head())
        df_used = df[df['page'] == 'NextSong']
        df_used = df_used[['ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']]
        columns = ['start_time', 'user_id', 'level', 'session_id', 'location', 'user_agent', 'song_id', 'artist_id']
        list_song_id = []
        list_artist_id = []
        list_ts = []
        for i, line in df_used.iterrows():
            print(line.song)
            if ("'" in line.song):
                a = str(line.song)
                a = a.replace("'","´")
            else:
                a = line.song
            df = return_song_id(a, conn)
            print(df)
            if df.empty:
                list_song_id.append('0')
                list_artist_id.append('0')
            else:
                list_song_id.append(df['song_id'].values[0])
                list_artist_id.append(df['artist_id'].values[0])

            time = pd.to_datetime(line.ts, unit='ms')
            list_ts.append(time)
        
        df_used = df_used[['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent']]
        df_used['song_id'] = list_song_id
        df_used['artist_id'] = list_artist_id
        df_used['ts'] = list_ts
    
        df_used.columns = columns
        print(df_used.head())
                    
        sql_insert = '''INSERT INTO sparkifydb.songplays
                    (start_time, user_id, level, session_id, location, user_agent, song_id, artist_id) 
                    values (%s,%s,%s,%s,%s,%s,%s,%s);'''

        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql_insert,df_used.values.tolist())
                print('Commit')
        except Exception as e:
            print(e)

def main():
    """
    This is the function called to execute all other functions;
    """
    structured_db()
    insert_songs()
    insert_artists()
    insert_users()
    insert_time()
    insert_songplays()

main()