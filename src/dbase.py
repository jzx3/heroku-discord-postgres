import os
import psycopg2


def url():
    return os.environ['DATABASE_URL']


def connect():
    try:
        DATABASE_URL = os.environ['DATABASE_URL']
    except:
        print('DATABASE_URL is not defined')
        print('Did you attach the Heroku Postgres add-on in your app\'s dashboard?')
        return None

    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        return conn
    except:
        print(f'Failed to connect to Postgres at URL: {DATABASE_URL}')
        return None


def create():
    conn = connect()
    c = conn.cursor()
    try:
        c.execute('CREATE TABLE players (discord_id TEXT PRIMARY KEY, discord_name TEXT, data TEXT, remarks TEXT)')
        conn.commit()
        txt = 'Database table created'
    except:
        txt = 'Failed to create table'
        print(txt)
    conn.close()
    return txt


def read():
    conn = connect()
    c = conn.cursor()
    try:
        sql_query = "SELECT * FROM players"
        c.execute(sql_query)
        rows = c.fetchall()
        txt = ''
        for row in rows:
            txt += '{}\n'.format(row)
    except:
        txt = 'Failed to read table'
        print(txt)
    conn.close()
    return txt


def getrow(discord_id):
    conn = connect()
    c = conn.cursor()
    try:
        sql_query = "SELECT * FROM players WHERE discord_id=?"
        c.execute(sql_query, (discord_id,))
        rows = c.fetchall()
        txt = ''
        for row in rows:
            txt += '{}\n'.format(row)
    except:
        txt = 'Failed to read row'
        print(txt)
    conn.close()
    return txt


def insert(author_id, author_name, txt):
    conn = connect()
    c = conn.cursor()
    try:
        c.execute('INSERT INTO players VALUES ("{}", "{}", "{}", "{}")'.format(author_id, author_name, txt, 'Remarks'))
        conn.commit()
        txt = 'Row inserted: "{}", "{}", "{}"'.format(author_id, author_name, txt)
    except:
        txt = 'Failed to insert row'
        print(txt)
    conn.close()
    return txt


def sql(cmd):
    conn = connect()
    c = conn.cursor()
    try:
        c.execute(cmd)
        conn.commit()
        txt = 'Database initialized'
    except:
        txt = 'Failed to execute SQL query "{}"'.format(cmd)
        print(txt)
    conn.close()
    return txt

