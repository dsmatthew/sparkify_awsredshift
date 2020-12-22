import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

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


def load_staging_tables(cur, conn):
    '''
    Executes all copy commands from `copy_table_queries` list.
    '''
    for query in copy_table_queries:
        try:
            query_objectName = get_queryName(query, searchTermStart='COPY', searchTermEnd=' ')
            print('executing TABLE COPY command for query: {}'.format(query_objectName))
            cur.execute(query)
            conn.commit()
            print('\t[SUCCESS]')
        except Exception as error:
            print('[FAILED] executing copy command for staging tables. Query: {}\n\Exception: {}'.format(query, error))
    print('[FINISHED] processing all copy commands on staging tables')

def insert_tables(cur, conn):
    '''
    Executes all insert commands from `insert_table_queries` list.
    '''
    for query in insert_table_queries:
        try:
            query_objectName = get_queryName(query, searchTermStart='INSERT INTO', searchTermEnd='(')
            print('executing TABLE INSERT command for query: {}'.format(query_objectName))
            cur.execute(query)
            conn.commit()
            print('\t[SUCCESS]')
        except Exception as error:
            print('[FAILED] executing insert command for DWH tables. Query: {}\n\Exception: {}'.format(query, error))
    print('[FINISHED] processing all insert commands on DWH tables')

def main():
    '''
    Execute the etl.py content:
        - import AWS cluster connection config information
        - open AWS RedShift cluster connection
        - create staging tables: execute COPY commands for staging tables
        - create dwh tables: execute INSERT commands
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

    print('starting to copy files into staging tables ...')
    load_staging_tables(cur, conn)
    print('starting to insert from staging into DWH tables ...')
    insert_tables(cur, conn)

    conn.close()
    print('[SUCCESS] closing the connection.')

if __name__ == "__main__":
    main()