from os import listdir
import pandas as pd
import psycopg2
import datetime

def list_files(*args, **kwargs):
    prefix = kwargs.get('prefix', 'log')
    list_files = listdir(f'data/{prefix}_data/')
    return list_files

def return_df_file(file, *args, **kwargs):
    prefix = kwargs.get('prefix', 'log')
    df = pd.read_json(f'data/{prefix}_data/{file}', lines=True)
    return df

def structured_db():
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
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.songplays(songplay_id int, start_time varchar, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.users(user_id varchar, first_name varchar, last_name varchar, gender varchar, level varchar, PRIMARY KEY(user_id));""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.songs(song_id varchar, title varchar, artist_id varchar, year int, duration varchar, PRIMARY KEY(song_id));""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.artists(artist_id varchar, artist_name varchar, artist_location varchar, artist_latitude varchar, artist_longitude varchar, PRIMARY KEY(artist_id));""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sparkifydb.time(start_time varchar, hour int, day int, week int, month int, year int, weakday varchar);""")
    print('Tables Created')

def insert_songs():
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
    files = list_files()
    conn = psycopg2.connect(host="postgres_udacity", dbname="udacity", user="udacity", password="udacity")
    conn.autocommit = True
    for file in files:
        df = return_df_file(file)
        print(df.head())
        df_insert = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
        df_insert.columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
        print(df_insert.head())

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
                        values (%s,%s,%s,%s,%s,%s,%s);'''

        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql_insert,df_final.values.tolist())
                print('Commit')
        except Exception as e:
            print(e)
    
def return_song_id(song, conn):
    sql = f'''select song_id, artist_id from sparkifydb.songs where title = '{song}';'''
    print(sql)
    df = pd.read_sql(sql, conn)
    return df

def insert_songplays():
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
        
        df_used = df_used[['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent']]
        df_used['song_id'] = list_song_id
        df_used['artist_id'] = list_artist_id
    
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
    structured_db()
    insert_songs()
    insert_artists()
    insert_users()
    insert_time()
    insert_songplays()

main()