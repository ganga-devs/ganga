from GangaCore.Utility.Config import getConfig

from sqlalchemy import create_engine
import urllib.parse

def db_engine():
    c = getConfig('DatabaseConfigurations')

    if c['database'] == 'sqlite':
        path = getConfig('Configuration')['gangadir']
        conn = 'sqlite:///'+path+'/ganga.db'
    else:
        dialect = c['database']
        driver = c['driver']
        username = urllib.parse.quote_plus(c['username'])
        password = urllib.parse.quote_plus(c['password'])
        host = c['host']
        port = c['port']
        database = c['dbname']

        # dialect+driver://username:password@host:port/database
        conn = "{}+{}://{}:{}@{}:{}/{}".format(dialect,driver,username,password,host,port,database)

    engine = create_engine(conn)
    return engine