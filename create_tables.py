import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries#, copy_table_queries, insert_table_queries

def get_queryName(query, searchTermStart='from', searchTermEnd=' ', toLower=True):
    '''
    Retrieve table name being processed

    Parameters
    ----------
    query : str
        SQL like query text for database operation.
    searchTermStart : str
        Search term which is before the 'to be extracted' string (e.g. "FROM" mytable ... )
    searchTermEnd : str
        Search term which is afterwards the 'to be extracted' string (e.g. FROM mytable "..." )
    toLower : bool
        Convert all strings to lower cases.

    Return
    ------
    query_part : str
        The extracted string value (e.g. table name).
    '''
    if toLower:
        query = query.lower()
        searchTermStart = searchTermStart.lower()
        searchTermEnd = searchTermEnd.lower()
    if searchTermStart in query:
        query_part = query[query.find(searchTermStart)+len(searchTermStart)+1:]
        if searchTermEnd in query_part:
            if query_part.find(searchTermEnd) > 0:
                query_part = query_part[0:query_part.find(searchTermEnd)]
    else:
        query_part = 'unkown'
    return query_part
        
def drop_tables(cur, conn):
    '''
    Drops each table using the queries in `drop_table_queries` list.
    '''
    for query in drop_table_queries:
        try:
            query_objectName = get_queryName(query, searchTermStart='DROP TABLE IF EXISTS', searchTermEnd=' ')
            print('executing TABLE DROP command for query: {}'.format(query_objectName))
            cur.execute(query)
            conn.commit()
        except Exception as error:
            print('Executing DROP TABLES command failed for query: {}\n\Exception: {}'.format(query, error))
    
    print('\tsuccessfully dropped table!')

def create_tables(cur, conn):
    '''
    Creates each table using the queries in `create_table_queries` list. 
    '''
    for query in create_table_queries:
        try:
            query_objectName = get_queryName(query, searchTermStart='CREATE TABLE IF NOT EXISTS', searchTermEnd='(')
            print('executing TABLE CREATE command for query: {}'.format(query_objectName))
            cur.execute(query)
            conn.commit()
        except Exception as error:
            print('Executing CREATE TABLES command failed for query: {}\n\Exception: {}'.format(query, error))

    print('\tsuccessfully created table!')

    
    
def main():
    '''
        - Drops (if exists) and Creates the tables on AWS.
        - Establishes connection with the sparkify database and gets cursor to it.  
        - Drops all the tables.  
        - Creates all tables.
        - Finally, closes the connection. 
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print('[SUCCESS] imported AWS config.')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print('[SUCCESS] connected to AWS cluster (host={}:{} DBname={}).'.format(config.get('CLUSTER', 'HOST')
                                                                                            , config.get('CLUSTER', 'DB_PORT')
                                                                                            , config.get('CLUSTER', 'DB_NAME')))
    except Exception as error:
        print('[FAIL] connecting to AWS cluster.')
        print('Exception message: {}'.format(error))
    
    print('starting to drop tables ...')
    drop_tables(cur, conn)
    
    print('starting to create tables ...')
    create_tables(cur, conn)

    
    conn.close()
    print('successfully closed connection.')


if __name__ == "__main__":
    main()