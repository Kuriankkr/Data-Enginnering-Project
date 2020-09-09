import configparser
import psycopg2
from sql_queries import analytics_queries


def analytics(cur,conn):
    """
    Run queries written in the sql_queries script
    
    Arguments:
    curr: Argument for the cursor that will execute the queries
    conn: Argument for connecting to the database (This connection will make sure we have the privileges to connect to this database)
    
    Returns:
    This function doesnt return anything
    """
    for query in analytics_queries:
        print(query)
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)


def main():
    
    '''
    The main function that calls all the other functions from within
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    analytics(cur,conn)
    conn.close()


if __name__ == "__main__":
    main()
