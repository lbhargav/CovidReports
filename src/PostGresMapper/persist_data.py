"""
Script that connects PostGres DB.
"""

import logging
from sqlalchemy import create_engine

class DBConnection:
    def __init__(self, host, database, user, password, port):
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port

    def connect(self):
        # Connect to the PostgreSQL database server
        try:
            # connect to the PostgreSQL server
            conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(self._user, self._password, self._host, self._port, self._database)
            engine = create_engine(conn_string)
            logging.info("DB connection was successful")
            return engine
        except Exception as e:
            logging.error(e)
            print(e)

    def upsert(self, db_connection, data, sink_table_name):
        with db_connection.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS public."{}"'.format(sink_table_name))
            data.to_sql(sink_table_name, conn)
            print("upsert was successful" + sink_table_name)
            logging.info("upsert was successful" + sink_table_name)
        # close the communication with the PostgreSQL
        conn.close()

