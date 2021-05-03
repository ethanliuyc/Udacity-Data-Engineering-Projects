import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''Loads data from S3 into staging tables on Redshift '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''Processes data from staging tables into analytics tables on Redshift'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads AWS configuration from file dwh.cfg
    - Connects to the Redshift cluster
    - Executes ETL with existing functions
    - Closes the connection
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()