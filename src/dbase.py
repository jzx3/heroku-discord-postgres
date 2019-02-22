import os
import psycopg2


def url():
    return os.environ['DATABASE_URL']


def rows_to_text(rows):
    txt = f'{len(rows)} records retrieved'
    for i, row in enumerate(rows):
        txt += f'\n{i+1}: {row}'
    return txt

        
def error_to_text(header, e):
    txt = header
    txt += f'Error: {e.pgerror.strip()} (Code: {e.pgcode})\n'
    txt += f'Message: {e.diag.message_primary} (Severity: {e.diag.severity})'
    print(txt)
    return txt


class HerokuDB():
    """Class for a Heroku Database"""

    def __init__(self, params=None):
        if params is None:
            try:
                self._url = os.environ['DATABASE_URL']
            except:
                print('DATABASE_URL is not defined')
                print('Did you attach the Heroku Postgres add-on in your app\'s dashboard?')
                self._conn = None
                return
            try:
                self._conn = psycopg2.connect(self._url, sslmode='require')
            except:
                print(f'Failed to connect to Postgres at URL: {self._url}')
                self._conn = None
        else:
            try:
                self._conn = psycopg2.connect(
                    user = params['USER'],
                    password = params['PASSWORD'],
                    host = params['HOST'],
                    port = params['PORT'],
                    database = DATABASE)
            except (Exception, psycopg2.Error) as error:
                print ("Error while connecting to PostgreSQL", error)


    def dsn(self):
        self._dsn = self._conn.get_dsn_parameters()  # [dictionary]
        return self._dsn


    def fetchone(self, sql_query, my_tuple=None):
        try:
            with self._conn.cursor() as c:
                if my_tuple is None:
                    c.execute(sql_query)
                else:
                    c.execute(sql_query, my_tuple)
                return c.fetchone(), None, None  # [tuple of length 1]
        except psycopg2.Error as e:
            txt = error_to_text(f'Failed to execute "{sql_query}" followed by fetchone\n', e)
            return None, txt, e


    def fetchall(self, sql_query):
        try:
            with self._conn.cursor() as c:
                if my_tuple is None:
                    c.execute(sql_query)
                else:
                    c.execute(sql_query, my_tuple)
                return c.fetchall(), None, None
        except psycopg2.Error as e:
            txt = error_to_text(f'Failed to execute "{sql_query}" followed by fetchall\n', e)
            return None, txt, e


    def commit(self, sql_query, my_tuple=None):
        try:
            with self._conn.cursor() as c:
                txt = f'Successful execute+commit of "{sql_query}", {my_tuple}'
                if my_tuple is None:
                    c.execute(sql_query)
                else:
                    c.execute(sql_query, my_tuple)
                self._conn.commit()
                return txt, None
        except psycopg2.Error as e:
            txt = error_to_text(f'Failed to execute+commit "{sql_query}", {my_tuple}\n', e)
            return txt, e


    def version(self):
        return self.fetchone('SELECT version();')


    def rollback(self):
        txt, e = self.commit('ROLLBACK;')
        return txt


    def close(self):
        try:
            if (self._conn):
                self._conn.close()
                txt = "PostgreSQL connection closed"
                self._conn = False
            else:
                print('PostgreSQL connection is already closed')
        except psycopg2.Error as e:
            txt = error_to_text('Failed to close database connection\n', e)
            print('Failed to close connection')
        finally:
            return txt


    @property
    def url(self):
        return self._url


    def check_connection(self):
        if self._conn is None:
            txt = 'Database is not connected'
            print(txt)
            return txt
        else:
            print(self._conn)
            print(f'\nConnection - dsn: {self._conn.dsn}')
            print(f'\nDSN parameters: {self._conn.get_dsn_parameters()}')

            txt = f'Database is connected at {self._url}'
            txt += f'\nDSN parameters: {self._conn.get_dsn_parameters()}'
            txt += f'\nClosed: {self._conn.closed} (0 = open, non-zero = closed/broken)'
            return txt


class HerokuDiscordTable(HerokuDB):

    def __init__(self, params=None):
        super().__init__(params)


    def dsn(self):
        super().dsn()


    def fetchone(self, sql_query):
        super().fetchone(sql_query)


    def fetchall(self, sql_query):
        super().fetchall(sql_query)


    def commit(self, sql_query):
        super().commit(sql_query)


    def commit_tuple(self, sql_query, my_tuple):
        super().commit_tuple(sql_query, my_tuple)


    def version(self):
        super().version()


    def rollback(self):
        super().rollback()


    def close(self):
        super().close()


    def create(self):
        sql_query = """CREATE TABLE players (discord_id TEXT PRIMARY KEY, server_id TEXT, discord_name TEXT, ign TEXT, local_data TEXT, global_data TEXT);"""
        txt, e = super().commit(sql_query)
        if e is None:
            return txt
        else:
            return error_to_text('Failed to create table\n', e)


    def drop(self):
        sql_query = """DROP TABLE IF EXISTS players;"""
        txt, e = super().commit(sql_query)
        if e is None:
            return txt
        else:
            return error_to_text('Failed to delete table\n', e)
    

    def add_column(self, column_name, data_type):
        if data_type == 'TEXT':
            sql_query = """ALTER TABLE players ADD %s TEXT;"""
        else:
            return f'Unknown/unimplemented data type: {data_type}'
        txt, e = super().commit(sql_query, (column_name,))
        if e is None:
            return txt
        else:
            return error_to_text(f'Failed to add column {column_name}\n', e)


    def drop_column(self, column_name):
       sql_query = """ALTER TABLE players DROP COLUMN %s;"""
       txt, e = super().commit(sql_query, (column_name,))
        if e is None:
            return txt
        else:
            return error_to_text(f'Failed to drop column {column_name}\n', e)


    def read(self):
        sql_query = """SELECT * from players;"""
        rows, txt, e = super().fetchall(sql_query)
        if rows is None:
            return txt
        else:
            return rows_to_text(rows)


    def getrow(self, discord_id):
        sql_query = """SELECT * from players WHERE discord_id = %s;"""
        rows, txt, e = super().fetchall(sql_query, (discord_id,) )
        if rows is None:
            return txt
        else:
            return rows_to_text(rows)


    def set_home_discord(self, author_id, server_id, author_name):
        sql_query = """INSERT INTO players (discord_id, server_id, discord_name)
            VALUES (%s, %s, %s) ON CONFLICT (discord_id) DO UPDATE SET server_id = EXCLUDED.server_id"""
        my_tuple = (author_id, server_id, author_name)
        txt, e = super().commit(sql_query, my_tuple)
        if e is None:
            return txt
        else:
            return error_to_text(f'Failed to set home discord {server_id}\n', e)


    def insert_ign(self, author_id, server_id, author_name, ign):
        sql_query = """INSERT INTO players (discord_id, server_id, discord_name, ign)
            VALUES (%s, %s, %s, %s) ON CONFLICT (discord_id) DO UPDATE SET ign = EXCLUDED.ign"""
        my_tuple = (author_id, server_id, author_name, ign)
        txt, e = super().commit(sql_query, my_tuple)
        if e is None:
            return txt
        else:
            return error_to_text(f'Failed to set ign = "{ign}"\n', e)


    def insert_local(self, author_id, server_id, author_name, txt):
        # To do: local data can only be done on home discord
        sql_query = """INSERT INTO players (discord_id, discord_name, local_data)
            VALUES (%s, %s, %s) ON CONFLICT (discord_id) DO UPDATE SET local_data = EXCLUDED.local_data"""
        my_tuple = (author_id, author_name, txt)
        txt, e = super().commit(sql_query, my_tuple)
        if e is None:
            return txt
        else:
            return error_to_text(f'Failed to set local_data = "{local_data}"\n', e)


    def insert_global(self, author_id, server_id, author_name, txt):
        sql_query = """INSERT INTO players (discord_id, discord_name, global_data)
            VALUES (%s, %s, %s) ON CONFLICT (discord_id) DO UPDATE SET global_data = EXCLUDED.global_data"""
        my_tuple = (author_id, author_name, txt)
        txt, e = super().commit(sql_query, my_tuple)
        if e is None:
            return txt
        else:
            return error_to_text(f'Failed to set global_data = "{global_data}"\n', e)


    def sql_commit(self, sql_query):
        txt, e = super().commit(sql_query)
        if e is None:
            return txt
        else:
            return error_to_text(f'Failed to execute+commit query: "{sql_query}"\n', e)


    def sql_fetch(self, sql_query):
        rows, txt, e = super().fetchall(sql_query)
        if e is None:
            return rows_to_text(txt)
        else:
            return error_to_text(f'Failed to execute+fetchall query: "{sql_query}"\n', e)
