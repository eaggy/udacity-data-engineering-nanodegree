import sys
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """  
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Fail to execute the query: {}'.format(query))
            print(e)    


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
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
    
    3. Drops all existing tables in the database.  
    
    4. Creates all required tables. 
    
    5. Closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn=psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except psycopg2.Error as e:
        print('Fail to establish connection with the database')
        print(e)
        sys.exit(1)

    if cur:
        drop_tables(cur, conn)
        create_tables(cur, conn)
        
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()