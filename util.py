"""This file contains helper functions used by both simbiose.py and simulate.py"""
import random
import string
from datetime import datetime

def create_ks_str(ks):
    """Create a string to create a keyspace into cs

    Args:
        ks (str): keyspace name

    Returns:
        str: a string to create the keyspace
    """
    return "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = \
            { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };" % (ks)

def create_cf_str(ks, cf, cols):
    """Create a string to create a table inside a keyspace into cs

    Args:
        ks (str): keyspace name
        cf (str): columnfammily name

    Returns:
        str: a string to create a columnfamily
    """
    begin_str = "CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, timestamp timestamp, " % (ks, cf)
    columns_def = ', '.join('%s text' % (c) for c in cols)
    end_str = ");"

    return ''.join([begin_str, columns_def, end_str])

def insert_cs(session, ks, cf, _id, body, timestamp=None):
    """Insert a new data into cs database"""
    cols = list(body.keys())
    if timestamp is None:
        timestamp = datetime.now()

    print('ics %s %s %s %s' % (ks, cf, _id, timestamp))

    prepared_stmt = session.prepare( \
            "INSERT INTO %s.%s (id, timestamp, %s) VALUES (%s)" % \
            (ks, cf, ', '.join(cols), ','.join(['?'] * (len(cols)+2))))
    bound_stmt = prepared_stmt.bind([_id, timestamp] + [body[c] for c in cols])
    session.execute(bound_stmt)

def clear_cs(session, meta):
    """Clear all cs data"""
    drop_str = lambda ks: "drop keyspace %s;" % (ks)
    (session.execute(drop_str(ks)) for ks in meta.keys())

def insert_es(es, index, doc, _id, body, timestamp=None):
    """Insert a new data into es database"""
    if timestamp is None:
        timestamp = datetime.now()

    print('ies: %s %s %s %s' % (index, doc, _id, timestamp))

    body['timestamp'] = timestamp
    es.index(index=index, doc_type=doc, id=_id, body=body)

def clear_es(es):
    """Clear all es data"""
    es.indices.delete('_all')

# Functions to generate new random data
def rand_word(size=10):
    """Generate a random word with a given length"""
    return ''.join(random.choice(string.ascii_letters) for _ in range(size))

def rand_body(columns):
    """Generate a random body for a document"""
    return {col: rand_word() for col in columns}
