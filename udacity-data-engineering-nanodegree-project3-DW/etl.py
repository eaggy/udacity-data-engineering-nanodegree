import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data into tables using the queries in `copy_table_queries` list. 
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Fail to execute the query: {}'.format(query))
            print(e) 


def insert_tables(cur, conn):
    """
    Inserts data into tables using the queries in `insert_table_queries` list. 
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Fail to execute the query: {}'.format(query))
            print(e) 


def main():
    """
    1. Creates an object of `ConfigParser` and reads the configuration into it. 
    
    2. Establishes connection with the database and gets cursor to it.  
    
    3. Loads data to the staging tables.  
    
    4. Inserts data into the tables. 
    
    5. Closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn=psycopg2.connect("host={} dbname={} user={} password={} port={}"
                              .format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except psycopg2.Error as e:
        print('Fail to establish connection with the database')
        print(e)
    
    if cur:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        cur.close()
        conn.close()


if __name__ == "__main__":
    main()