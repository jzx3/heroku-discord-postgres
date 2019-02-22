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


def error_to_text(header, e):
    txt = header
    txt += f'Code: {e.pgcode}\n'
    txt += f'Error: {e.pgerror}\n'
    txt += f'Severity: {e.diag.severity}\n'
    txt += f'Message: {e.diag.message_primary}'
    print(txt)
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
            print(dir(self._conn))
            return f'Database is connected at {self._url}. Connection is {self._conn}'


    def create(self):
        c = self._conn.cursor()
        try:
            c.execute('CREATE TABLE players (discord_id TEXT PRIMARY KEY, server_id TEXT, discord_name TEXT, ign TEXT, local_data TEXT, global_data TEXT);')
            self._conn.commit()
            txt = 'Database table created'
        except psycopg2.Error as e:
            txt = error_to_text('Failed to create table\n', e)
        return txt


    def drop(self):
        c = self._conn.cursor()
        try:
            c.execute('DROP TABLE IF EXISTS players;')
            self._conn.commit()
            txt = 'Database table deleted'
        except psycopg2.Error as e:
            txt = error_to_text('Failed to delete table\n', e)
        return txt
    

    def close(self):
        try:
            self._conn.close()
            txt = 'Database connection closed'
            self._conn = None
        except psycopg2.Error as e:
            txt = error_to_text('Failed to close database connection\n', e)
        return txt


    def read(self):
        sql_query = "SELECT * FROM players;"
        try:
            with self._conn.cursor() as c:
                c.execute(sql_query)
                rows = c.fetchall()
        except psycopg2.Error as e:
            txt = error_to_text('Failed to read table\n', e)
            return txt

        print(f'{len(rows)} rows retrieved')
        return rows_to_text(rows)


    def getrow(self, discord_id):
        try:
            sql_query = "SELECT * FROM players WHERE discord_id=?;"
            with self._conn.cursor() as c:
                c.execute(sql_query, (discord_id,))
                rows = c.fetchall()
        except psycopg2.Error as e:
            txt = error_to_text('Failed to read row\n', e)
            return txt
        return rows_to_text(rows)


    def insert_ign(self, author_id, author_name, ign):
        try:
            with self._conn.cursor() as c:
                c.execute('INSERT INTO players (discord_id, server_id, discord_name, ign) VALUES (%s, %s, %s, %s)',
                    (author_id, server_id, author_name, ign))
                self._conn.commit()
            txt = f'Row inserted: "{author_id}", "{server_id}", "{author_name}": ign="{ign}"'
        except psycopg2.Error as e:
            txt = error_to_text('Failed to insert row\n', e)
        return txt


    def insert_local(self, author_id, author_name, txt):
        try:
            with self._conn.cursor() as c:
                c.execute('INSERT INTO players (discord_id, server_id, discord_name, local_data) VALUES (%s, %s, %s, %s)',
                    (author_id, server_id, author_name, txt))
                self._conn.commit()
            txt = f'Row inserted: "{author_id}", "{server_id}", "{author_name}": local_data="{txt}"'
        except psycopg2.Error as e:
            txt = error_to_text('Failed to insert row\n', e)
        return txt


    def insert(self, author_id, author_name, txt):
        try:
            with self._conn.cursor() as c:
                c.execute('INSERT INTO players (discord_id, server_id, discord_name, global_data) VALUES (%s, %s, %s, %s)',
                    (author_id, server_id, author_name, txt))
                self._conn.commit()
            txt = f'Row inserted: "{author_id}", "{server_id}", "{author_name}": global_data="{txt}"'
        except psycopg2.Error as e:
            txt = error_to_text('Failed to insert row\n', e)
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
