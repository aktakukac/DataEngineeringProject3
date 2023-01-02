"""Drops and creates tables in amazon redshift for Sparkify data."""

import configparser
from time import time
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Dropping tables (staging and final) if they exist.
    Param:
    cur : cursor of psycopg2 database connection
    conn : connection of psycopg2
    """
    for i, query in enumerate(drop_table_queries):
        starttime = time()
        print('Running query {} of {}'.format(i+1, len(drop_table_queries)))
        cur.execute(query)
        conn.commit()
        loadtime = time() - starttime
        print("Elapsed time: {0:.2f} sec\n".format(loadtime))


def create_tables(cur, conn):
    """Creating tables (staging and final).
    Param:
    cur : cursor of psycopg2 database connection
    conn : connection of psycopg2
    """
    for i, query in enumerate(create_table_queries):
        starttime = time()
        print('Running query {} of {}'.format(i+1, len(create_table_queries)))
        cur.execute(query)
        conn.commit()
        loadtime = time() - starttime
        print("Elapsed time: {0:.2f} sec\n".format(loadtime))


def main():
    """ 
    Configure Amazon Redshift credentials based on config file
    Create connection and cursor
    Dropping tables (staging and final) if they exist.
    Creating tables (staging and final).
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected!') 

    print('Dropping tables!')
    drop_tables(cur, conn)

    print('Creating tables!')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
