"""
This is the script to be executes as ETL
It will create all the necessary tables and insert the available data
"""
from os import listdir
import pandas as pd
import datetime
from cassandra.cluster import Cluster

def list_files():

    """
    This function is to be called to return a list of files in data path;
    It will be returned the files in subfolder log or data, wich one will depend on the informed 'prefix',
    by default is returned 'log_data' folder, to return data subfolder on th call of the function use,
    prefix='data';
    """

    list_files = listdir('data/')
    return list_files

def return_df_file(file):

    """
    This function read a specific file and return a pandas DF;
    To call this function pass the file name variable;
    By default the function read the file in log_data subfolder, to change that inform prefix='data' in function call;
    """
    
    df = pd.read_csv(f'data/{file}')
    return df

def return_connection():

    """
    This function connects to a Cassandra Cluster to be used to insert the data;
    It also creates a keyspace to be used;
    And finally return the session available;
    """

    cluster = Cluster(['cassandra_udacity'], port=9042)
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity_cassandra
        WITH REPLICATION = 
            {'class':'SimpleStrategy', 'replication_factor':1}"""
    )
    session.set_keyspace('udacity_cassandra')

    return session

def extructure_db(session):

    """
    This function is used to structured the databases utilized in this etl;
    First will be created a 'main' table to insert all data;
    And then each 'data' table is created to respond a specific query necessity;
    """

    session.execute("""DROP TABLE IF EXISTS full_data;""")
    print('Table full_data droped')
    session.execute("""CREATE TABLE IF NOT EXISTS full_data(
        artist text,
        firstName text,
        gender text,
        itemInSession text,
        lastName text,
        length text,
        level text,
        location text,
        sessionId text,
        song text,
        userId text,
        PRIMARY KEY (artist, sessionId, song, userId));"""
    )
    print('Table full_data created')

    session.execute("""DROP TABLE IF EXISTS data_1;""")
    print('Table data_1 droped')
    session.execute("""CREATE TABLE IF NOT EXISTS data_1(
        artist text,
        firstName text,
        gender text,
        itemInSession text,
        lastName text,
        length text,
        level text,
        location text,
        sessionId text,
        song text,
        userId text,
        PRIMARY KEY (itemInSession, sessionId));"""
    )
    print('Table data_1 created')

    session.execute("""DROP TABLE IF EXISTS data_2;""")
    print('Table data_2 droped')
    session.execute("""CREATE TABLE IF NOT EXISTS data_2(
        artist text,
        firstName text,
        gender text,
        itemInSession text,
        lastName text,
        length text,
        level text,
        location text,
        sessionId text,
        song text,
        userId text,
        PRIMARY KEY (userId, sessionId, itemInSession))
        WITH CLUSTERING ORDER BY (sessionId DESC, itemInSession DESC);"""
    )
    print('Table data_2 created')


    session.execute("""DROP TABLE IF EXISTS data_3;""")
    print('Table data_3 droped')
    session.execute("""CREATE TABLE IF NOT EXISTS data_3(
        artist text,
        firstName text,
        gender text,
        itemInSession text,
        lastName text,
        length text,
        level text,
        location text,
        sessionId text,
        song text,
        userId text,
        PRIMARY KEY (song));"""
    )
    print('Table data_3 created')

def insert_df(session, df, table):

    """
    This function is called to insert a pandas df in a specific table;
    """

    sql = f"INSERT INTO {table}(artist,firstName,gender,itemInSession,lastName,length,level,location,sessionId,song,userId) VALUES (?,?,?,?,?,?,?,?,?,?,?)"

    prepared = session.prepare(sql)
    for i, item in df.iterrows():
        try:
            session.execute(prepared, (item.artist,item.firstName,item.gender,item.itemInSession,
                item.lastName,item.length,item.level,
                item.location,item.sessionId,item.song,item.userId))
            print('Commit')
        except Exception as e:
            print(e)

def main():

    """
    This is the function called to execute all other functions;
    """

    session = return_connection()
    extructure_db(session)

    files = list_files()

    for file in files:

        df = return_df_file(file)
        df_insert = df[['artist','firstName','gender','itemInSession','lastName','length','level','location','sessionId','song','userId']]
        df_insert = df_insert.fillna('None').astype(str)
        print(df_insert)

        insert_df(session, df_insert, 'full_data')
        insert_df(session, df_insert, 'data_1')
        insert_df(session, df_insert, 'data_2')
        insert_df(session, df_insert, 'data_3')

main()