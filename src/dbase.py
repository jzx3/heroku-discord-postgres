import os
import psycopg2


def url():
    return os.environ['DATABASE_URL']


def rows_to_text(rows):
    for i, row in enumerate(rows):
        if i == 0:
            txt = f'{i}: {row}'
        else:
            txt += f'\n{i}: {row}'
    return txt


class HerokuDB():
    """Class for a Heroku Database"""
    
    def __init__(self):
        try:
            self._url = os.environ['DATABASE_URL']
        except:
            print('DATABASE_URL is not defined')
            print('Did you attach the Heroku Postgres add-on in your app\'s dashboard?')
            self._conn = None
            return

        try:
            # self._conn = await asyncpg.connect(self._url, sslmode='require')
            self._conn = psycopg2.connect(self._url, sslmode='require')
        except:
            print(f'Failed to connect to Postgres at URL: {self._url}')
            self._conn = None


    @property
    def url(self):
        return self._url


    def check_connection(self):
        if self._conn is None:
            txt = 'Database is not connected'
            print(txt)
            return txt
        else:
            return f'Database is connected at {self._url}. Connection is {self._conn}'


    def create(self):
        try:
            c = self._conn.cursor()
        except:
            txt = 'Could not get cursor'
            print(txt)
            return txt
        try:
            c.execute('CREATE TABLE players (discord_id TEXT PRIMARY KEY, discord_name TEXT, data TEXT, remarks TEXT);')
            self._conn.commit()
            txt = 'Database table created'
        except:
            txt = 'Failed to create table'
            print(txt)
        return txt


    def close(self):
        try:
            self._conn.close()
            txt = 'Database connection closed'
            self._conn = None
        except:
            txt = 'Failed to close database connection'
            print(txt)
        return txt


    def read(self):
        try:
            sql_query = "SELECT * FROM players;"
            # rows = await conn.fetch(sql_query)
            with self._conn.cursor() as c:
                c.execute(sql_query)
                rows = c.fetchall()
        except:
            txt = 'Failed to read table'
            print(txt)
            return txt

        print(f'{len(rows)} rows retrieved')
        return rows_to_text(rows)


    def getrow(self, discord_id):
        try:
            sql_query = "SELECT * FROM players WHERE discord_id=?;"
            with self._conn.cursor() as c:
                c.execute(sql_query, (discord_id,))
                rows = c.fetchall()
        except:
            txt = 'Failed to read row'
            print(txt)
            return txt
        return rows_to_text(rows)


    def insert(self, author_id, author_name, txt):
        try:
            with self._conn.cursor() as c:
                c.execute('INSERT INTO players VALUES ("{}", "{}", "{}", "{}");'.format(author_id, author_name, txt, 'Remarks'))
                self._conn.commit()
            txt = f'Row inserted: "{author_id}", "{author_name}", "{txt}"'
        except:
            txt = 'Failed to insert row'
            print(txt)
        return txt


    def sql(self, cmd):
        try:
            with self._conn.cursor() as c:
                c.execute(cmd)
                self._conn.commit()
            txt = f'Executed SQL command: "{cmd}"'
        except:
            txt = f'Failed to execute SQL command "{cmd}"'
        print(txt)
        return txt
