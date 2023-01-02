"""Copies data to staging tables and inserts data to final tables."""

import configparser
from time import time
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load staging tables.
    Param:
    cur : cursor of psycopg2 database connection
    conn : connection of psycopg2
    """
    for i, query in enumerate(copy_table_queries):
        starttime = time()
        print('Running query {} of {}'.format(i+1, len(copy_table_queries)))
        cur.execute(query)
        conn.commit()
        loadtime = time() - starttime
        print("Elapsed time: {0:.2f} sec\n".format(loadtime))


def insert_tables(cur, conn):
    """Insert final tables.
    Param:
    cur : cursor of psycopg2 database connection
    conn : connection of psycopg2
    """
    for i, query in enumerate(insert_table_queries):
        starttime = time()
        print('Running query {} of {}'.format(i+1, len(insert_table_queries)))
        cur.execute(query)
        conn.commit()
        loadtime = time() - starttime
        print("Elapsed time: {0:.2f} sec\n".format(loadtime))

def main():
    """
    Configure Amazon Redshift credentials based on config file
    Create connection and cursor
    Load staging tables.
    Insert final tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected!') 

    print('Loading staging tables!')
    load_staging_tables(cur, conn)

    print('Inserting tables!')
    insert_tables(cur, conn)

    print('Data loaded!')

    conn.close()


if __name__ == "__main__":
    main()
