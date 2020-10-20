import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - This function uses the queries, which are the queries in the copy_table_queries to read data from S3 
    - And store the data in the database staging tables
    Parameters:
        cur: database cursor
        conn: database connection object
    Returns:
        None
    """
    for query in copy_table_queries:
        
        print('Running: ' + query)
        
        # Execute the query
        cur.execute(query)
        
        # Commit it
        conn.commit()


def insert_tables(cur, conn):
    """
    - This function uses the queries, which are in the insert_table_queries to read data from the staging tables.
    - And store the data in the database fact tables.
    Parameters:
        cur: database cursor
        conn: database connection object
    Returns:
        None
    """
    for query in insert_table_queries:       
        print('Running: ' + query)
        
        # Execute the query
        cur.execute(query)
        
        # Commit the translaction
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Establisch the connection  to the redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()