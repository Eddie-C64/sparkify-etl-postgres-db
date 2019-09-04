import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    :return: Establishes connection to DB,
             then checks if a sparkifydb exist.
             If it does not it is created then,
             returns connection and cursor objects
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=lalo user=lalo password=Eddie!1992")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=lalo user=lalo password=Eddie!1992")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    :param cur: cursor object from psycopg2
    :param conn: connection object from psycopg2
    :return: Drops all tables and database that is in the
             connection to start fresh.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    :param cur: cursor object from psycopg2
    :param conn: connection object from psycopg2
    :return: returns the created tables for the sparkifydb
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    :return: runs in order of connection establishment, drop tables,
             and create tables for sparkifydb.
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
