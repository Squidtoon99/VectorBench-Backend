import psycopg2
from configparser import ConfigParser
from flask import g

def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return db

def get_db():
    if 'db' not in g:
        try:
            params = config()
            g.db = psycopg2.connect(**params)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            g.db = None
    return g.db

def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()
    if e is not None:
        print(e)

