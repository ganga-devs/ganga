'''Module that manages the database connection

This module is temporary, is planned to access the database 
via web service and not by direct access'''

import re

import psycopg2
import psycopg2.extras

from GangaCore.Utility.Config import *
import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()

# From psycogp2 documentation: In Python 2, if you want to uniformly receive 
# all your database input in Unicode, you can register the related typecasters 
# globally as soon as Psycopg is imported, and then forget about this story.
import psycopg2.extensions

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


def gridmon(query, params=None):
    '''Sends query to gridmon DB'''
    
    pattern = re.compile(r'\s+')
    query = re.sub(pattern, ' ', query.strip())
    
    # Connect to an existing database
    conn = psycopg2.connect(host = 'localhost',
                            database = 'gridmon',
                            user = getConfig('SuperB')['gridmon_user'],
                            password = getConfig('SuperB')['gridmon_pass'])
    
    # Open a cursor to perform database operations
    #cur = conn.cursor()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # log query
    logger.debug(cur.mogrify(query, params))
    
    # Query the database and obtain data as Python objects
    cur.execute("SET search_path = gridmonitor, pg_catalog;", )
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    # Make the changes to the database persistent
    conn.commit()
    
    # Close communication with the database
    cur.close()
    conn.close()
    
    return results

def read(query, params=None):
    '''Sends SELECT query to sbk DB, returns all the results'''
    
    pattern = re.compile(r'\s+')
    query = re.sub(pattern, ' ', query.strip())
    
    # Connect to an existing database
    conn = psycopg2.connect(host = 'localhost',
                            database = 'sbk5',
                            user = getConfig('SuperB')['sbk_user'],
                            password = getConfig('SuperB')['sbk_pass'])
    
    # Open a cursor to perform database operations
    #cur = conn.cursor()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Register adapter and typecaster for dict-hstore conversions.
    psycopg2.extras.register_hstore(conn, unicode=True)
    
    # Query the database and obtain data as Python objects
    cur.execute("SET search_path = sbk, pg_catalog;", )
    
    # log query
    logger.debug(cur.mogrify(query, params))
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    # Make the changes to the database persistent
    conn.commit()
    
    # Close communication with the database
    cur.close()
    conn.close()
    
    return results

def write(query, params=None):
    '''Sends WRITE, UPDATE, DELETE query to sbk DB, returns nothing.'''
    
    pattern = re.compile(r'\s+')
    query = re.sub(pattern, ' ', query.strip())
    
    # Connect to an existing database
    conn = psycopg2.connect(host = 'localhost',
                            database = 'sbk5',
                            user = getConfig('SuperB')['sbk_user'],
                            password = getConfig('SuperB')['sbk_pass'])
    
    # Open a cursor to perform database operations
    #cur = conn.cursor()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Register adapter and typecaster for dict-hstore conversions.
    psycopg2.extras.register_hstore(conn, unicode=True)
    
    # Query the database and obtain data as Python objects
    cur.execute("SET search_path = sbk, pg_catalog;", )
    
    # log query
    logger.debug(cur.mogrify(query, params))
    
    cur.execute(query, params)
    
    # Make the changes to the database persistent
    conn.commit()
    
    # Close communication with the database
    cur.close()
    conn.close()
