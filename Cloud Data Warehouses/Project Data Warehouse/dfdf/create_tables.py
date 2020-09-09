import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Arguments:
    curr: Argument for the cursor that will execute the queries
    conn: Argument for connecting to the database (This connection will make sure we have the privileges to connect to this database)
    
    Returns:
    This function doesnt return anything
    '''
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    '''
    Arguments:
    curr: Argument for the cursor that will execute the queries
    conn: Argument for connecting to the database (This connection will make sure we have the privileges to connect to this database)
    
    Returns:
    This function doesnt return anything
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    '''
    The main function that calls all the other functions from within
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()