import os
import psycopg2


def create():
    DATABASE_URL = os.environ['DATABASE_URL']
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    c = conn.cursor()
    try:
        c.execute('CREATE TABLE players (discord_id TEXT PRIMARY KEY, discord_name TEXT, data TEXT, remarks TEXT)')
        conn.commit()
        txt = 'Database initialized'
    except:
        txt = 'Failed to create table'
    conn.close()

    return txt


def read(discord_id: str):
    DATABASE_URL = os.environ['DATABASE_URL']
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    c = conn.cursor()
    try:
        sql_query = "SELECT * FROM players WHERE discord_id=?"
        c.execute(sql_query, (discord_id,))
        rows = c.fetchall()
        txt = ''
        for row in rows:
            txt += '{}\n'.format(row)
    except:
        txt = 'Failed to read table'
    conn.close()
    return txt


def insert(author_id, author_name, txt):
    DATABASE_URL = os.environ['DATABASE_URL']
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    c = conn.cursor()
    try:
        c.execute('INSERT INTO players VALUES ("{}", "{}", "{}", "{}")'.format(author_id, author_name, txt, ''))
        conn.commit()
        txt = 'Row inserted: "{}", "{}", "{}"'.format(author_id, author_name, txt)
    except:
        txt = 'Failed to insert data'
    conn.close()

    return txt


def sql(cmd):
    DATABASE_URL = os.environ['DATABASE_URL']
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    c = conn.cursor()
    try:
        c.execute(cmd)
        conn.commit()
        txt = 'Database initialized'
    except:
        txt = 'Failed to create table'
    conn.close()

    return txt


