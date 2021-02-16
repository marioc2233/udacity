
from os import listdir
import pandas as pd
import psycopg2
import datetime
import tweepy
import emoji

def list_files():
    list_files = listdir('data/')
    final = []
    for file in list_files:
        if len(file.split('.')) == 2:
            final.append(file)
    return final

def return_df_file(file):
    df = pd.read_csv(f'data/{file}', sep=';')
    return df

def structured_db():
    conn = psycopg2.connect(host="postgres_boticario", dbname="boticario", user="boticaro", password="boticario")
    cur = conn.cursor()
    conn.autocommit = True
    cur.execute("""DROP TABLE IF EXISTS boticario.base_total;""")
    cur.execute("""DROP TABLE IF EXISTS boticario.tabela_1;""")
    cur.execute("""DROP TABLE IF EXISTS boticario.tabela_2;""")
    cur.execute("""DROP TABLE IF EXISTS boticario.tabela_3;""")
    cur.execute("""DROP TABLE IF EXISTS boticario.tabela_4;""")
    cur.execute("""DROP TABLE IF EXISTS boticario.twitter_bot;""")
    cur.execute("""DROP TABLE IF EXISTS boticario.twitter_max;""")
    cur.execute("""DROP TABLE IF EXISTS boticario.twitter_bot_max;""")
    print('Tables Droped')
    cur.execute("""DROP SCHEMA IF EXISTS boticario;""")
    print('Schema Droped')
    cur.execute("""CREATE SCHEMA IF NOT EXISTS boticario;""")
    print('Schema Created')
    cur.execute("""CREATE TABLE IF NOT EXISTS boticario.base_total(
        id_marca integer, 
        marca varchar, 
        id_linha integer, 
        linha varchar, 
        data_venda date,
        qtd_venda integer
        );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS boticario.tabela_1(
            total integer, 
            mes integer, 
            ano integer, 
            PRIMARY KEY(mes, ano)
            );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS boticario.tabela_2(
            total integer, 
            marca varchar, 
            linha varchar, 
            PRIMARY KEY(marca, linha)
            );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS boticario.tabela_3(
            total integer, 
            mes integer, 
            ano integer, 
            marca varchar, 
            PRIMARY KEY(mes, ano, marca)
            );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS boticario.tabela_4(
            total integer, 
            mes integer, 
            ano integer, 
            linha varchar, 
            PRIMARY KEY(mes, ano, linha)
            );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS boticario.twitter_bot(
                id varchar, 
                name varchar, 
                screen_name varchar, 
                twitter text, 
                PRIMARY KEY(id, twitter)
                );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS boticario.twitter_max(
                id varchar, 
                name varchar, 
                screen_name varchar, 
                twitter text, 
                PRIMARY KEY(id, twitter)
                );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS boticario.twitter_bot_max(
                    id varchar, 
                    name varchar, 
                    screen_name varchar, 
                    twitter text, 
                    PRIMARY KEY(id, twitter)
                    );""")

    print('Tables Created')

def insert_base():
    files = list_files()
    conn = psycopg2.connect(host="postgres_boticario", dbname="boticario", user="boticaro", password="boticario")
    conn.autocommit = True
    for file in files:
        df = return_df_file(file)
        df = df.rename(columns=str.lower)
        df["data_venda"] = pd.to_datetime(df["data_venda"], dayfirst=True).dt.date
        print(df.head())

        sql_insert = '''INSERT INTO boticario.base_total
                        (id_marca, marca, id_linha, linha, data_venda, qtd_venda) 
                        values (%s,%s,%s,%s,%s,%s);'''

        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql_insert,df.values.tolist())
                print('Commit')
        except Exception as e:
            print(e)

def populate_1():
    conn = psycopg2.connect(host="postgres_boticario", dbname="boticario", user="boticaro", password="boticario")
    conn.autocommit = True
    sql_1 = '''select sum(qtd_venda) total, extract(MONTH from data_venda) mes, extract(YEAR from data_venda) ano
                from boticario.base_total
                GROUP BY mes,ano;'''

    df_1 = pd.read_sql(sql_1, conn)

    sql_insert = '''INSERT INTO boticario.tabela_1
                            (total, mes, ano) 
                            values (%s,%s,%s);'''

    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql_insert, df_1.values.tolist())
            print('Commit')
    except Exception as e:
        print(e)

def populate_2():
    conn = psycopg2.connect(host="postgres_boticario", dbname="boticario", user="boticaro", password="boticario")
    conn.autocommit = True
    sql_2 = '''select sum(qtd_venda) total, marca, linha
                from boticario.base_total
                GROUP BY marca,linha;'''

    df_2 = pd.read_sql(sql_2, conn)

    sql_insert = '''INSERT INTO boticario.tabela_2
                            (total, marca, linha) 
                            values (%s,%s,%s);'''

    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql_insert, df_2.values.tolist())
            print('Commit')
    except Exception as e:
        print(e)

def populate_3():
    conn = psycopg2.connect(host="postgres_boticario", dbname="boticario", user="boticaro", password="boticario")
    conn.autocommit = True
    sql_3 = '''select sum(qtd_venda) total, extract(MONTH from data_venda) mes, extract(YEAR from data_venda) ano, marca
                from boticario.base_total
                GROUP BY mes,ano,marca;'''

    df_3 = pd.read_sql(sql_3, conn)

    sql_insert = '''INSERT INTO boticario.tabela_3
                            (total, mes, ano, marca) 
                            values (%s,%s,%s,%s);'''

    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql_insert, df_3.values.tolist())
            print('Commit')
    except Exception as e:
        print(e)

def populate_4():
    conn = psycopg2.connect(host="postgres_boticario", dbname="boticario", user="boticaro", password="boticario")
    conn.autocommit = True
    sql_4 = '''select sum(qtd_venda) total, extract(MONTH from data_venda) mes, extract(YEAR from data_venda) ano, linha
                from boticario.base_total
                GROUP BY mes,ano,linha;'''

    df_4 = pd.read_sql(sql_4, conn)

    sql_insert = '''INSERT INTO boticario.tabela_4
                            (total, mes, ano, linha) 
                            values (%s,%s,%s,%s);'''

    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql_insert, df_4.values.tolist())
            print('Commit')
    except Exception as e:
        print(e)

def populate_twitter_bot():
    conn = psycopg2.connect(host="postgres_boticario", dbname="boticario", user="boticaro", password="boticario")
    conn.autocommit = True

    access_token = '1361447029889712130-QPFVJFx6iFyckwO6cpXvXsaJsJXmAe'
    access_token_secret = 'ffU0dmqdgLDrZQ0Zw1cudp3qAK0nWJf8rO9lGqIkBYIDz'
    api_key = 'Wv9mHbHUEGyVbTYJcaasfSqbZ'
    api_secret_key = 'zoyiCGfbus3xok6SFT3A7dMoLjGS4LNgL3v9OBWSh8CxSzs6ij'

    autenticacao = tweepy.OAuthHandler(api_key, api_secret_key)
    autenticacao.set_access_token(access_token, access_token_secret)

    twitter = tweepy.API(autenticacao)

    resultados = twitter.search(q='Boticário', lang='pt', count='50', result_type='recent')

    colunas = ['id', 'name', 'screen_name', 'twitter']

    df_insert = pd.DataFrame(columns=colunas)

    for twets in resultados:

        df = pd.DataFrame([[twets.user.id, emoji.get_emoji_regexp().sub(u'', twets.user.name), emoji.get_emoji_regexp().sub(u'', twets.user.screen_name), emoji.get_emoji_regexp().sub(u'', twets.text)]], columns=colunas)
        frames = [df_insert, df]

        df_insert = pd.concat(frames)

    print(df_insert.head())
    sql_insert = '''INSERT INTO boticario.twitter_bot
                            (id, name, screen_name, twitter) 
                            values (%s,%s,%s,%s);'''

    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql_insert, df_insert.values.tolist())
            print('Commit')
    except Exception as e:
        print(e)

def populate_twitter_max():
    conn = psycopg2.connect(host="postgres_boticario", dbname="boticario", user="boticaro", password="boticario")
    conn.autocommit = True

    access_token = '1361447029889712130-QPFVJFx6iFyckwO6cpXvXsaJsJXmAe'
    access_token_secret = 'ffU0dmqdgLDrZQ0Zw1cudp3qAK0nWJf8rO9lGqIkBYIDz'
    api_key = 'Wv9mHbHUEGyVbTYJcaasfSqbZ'
    api_secret_key = 'zoyiCGfbus3xok6SFT3A7dMoLjGS4LNgL3v9OBWSh8CxSzs6ij'

    autenticacao = tweepy.OAuthHandler(api_key, api_secret_key)
    autenticacao.set_access_token(access_token, access_token_secret)

    sql_max = 'select max(total), linha from boticario.tabela_4 where mes = 12 GROUP BY linha;'
    df_max = pd.read_sql(sql_max, conn)
    param = df_max['linha'][0]

    twitter = tweepy.API(autenticacao)

    resultados = twitter.search(q=f'{param}', lang='pt', count='50', result_type='recent')

    colunas = ['id', 'name', 'screen_name', 'twitter']

    df_insert = pd.DataFrame(columns=colunas)


    for twets in resultados:

        df = pd.DataFrame([[twets.user.id, emoji.get_emoji_regexp().sub(u'', twets.user.name), emoji.get_emoji_regexp().sub(u'', twets.user.screen_name), emoji.get_emoji_regexp().sub(u'', twets.text)]], columns=colunas)
        frames = [df_insert, df]

        df_insert = pd.concat(frames)

    print(df_insert.head())
    sql_insert = '''INSERT INTO boticario.twitter_max
                            (id, name, screen_name, twitter) 
                            values (%s,%s,%s,%s);'''

    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql_insert, df_insert.values.tolist())
            print('Commit')
    except Exception as e:
        print(e)

def populate_twitter_bot_max():
    conn = psycopg2.connect(host="postgres_boticario", dbname="boticario", user="boticaro", password="boticario")
    conn.autocommit = True

    access_token = '1361447029889712130-QPFVJFx6iFyckwO6cpXvXsaJsJXmAe'
    access_token_secret = 'ffU0dmqdgLDrZQ0Zw1cudp3qAK0nWJf8rO9lGqIkBYIDz'
    api_key = 'Wv9mHbHUEGyVbTYJcaasfSqbZ'
    api_secret_key = 'zoyiCGfbus3xok6SFT3A7dMoLjGS4LNgL3v9OBWSh8CxSzs6ij'

    autenticacao = tweepy.OAuthHandler(api_key, api_secret_key)
    autenticacao.set_access_token(access_token, access_token_secret)

    sql_max = 'select max(total), linha from boticario.tabela_4 where mes = 12 GROUP BY linha;'
    df_max = pd.read_sql(sql_max, conn)
    param = df_max['linha'][0]

    twitter = tweepy.API(autenticacao)

    resultados = twitter.search(q=f'Boticário {param}', lang='pt', count='50', result_type='recent')

    colunas = ['id', 'name', 'screen_name', 'twitter']

    df_insert = pd.DataFrame(columns=colunas)


    for twets in resultados:

        df = pd.DataFrame([[twets.user.id, emoji.get_emoji_regexp().sub(u'', twets.user.name), emoji.get_emoji_regexp().sub(u'', twets.user.screen_name), emoji.get_emoji_regexp().sub(u'', twets.text)]], columns=colunas)
        frames = [df_insert, df]

        df_insert = pd.concat(frames)

    print(df_insert.head())
    sql_insert = '''INSERT INTO boticario.twitter_bot_max
                            (id, name, screen_name, twitter) 
                            values (%s,%s,%s,%s);'''

    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql_insert, df_insert.values.tolist())
            print('Commit')
    except Exception as e:
        print(e)

def main():
    """
    This is the function called to execute all other functions;
    """
    structured_db()
    insert_base()
    populate_1()
    populate_2()
    populate_3()
    populate_4()
    populate_twitter_bot()
    populate_twitter_max()
    populate_twitter_bot_max()

main()