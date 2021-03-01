import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # Connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.Error as e:
        print('Fail to connect to default database')
        print(e)
        return None, None
    
    # Drop sparkify database
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print('Fail to drop default database')
        print(e)
        return None, None
        
    # Create sparkify database with UTF8 encoding    
    try:    
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print('Fail to create default database')
        print(e)
        return None, None

    # Close connection to default database
    cur.close()
    conn.close()    
    
    # Connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print('Fail to connect to sparkify database')
        print(e)
        return None, None
    
    return cur, conn


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
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    if cur:
        drop_tables(cur, conn)
        create_tables(cur, conn)

        cur.close()
        conn.close()


if __name__ == "__main__":
    main()