"""This file contains helper functions used by both simbiose.py and simulate.py"""
import random
import string
import copy
from datetime import datetime

class CassandraHelper:
    def __init__(self, cluster, session):
        self.cluster = cluster
        self.session = session

    def create_ks_str(self, ks):
        """Create a string to create a keyspace into cs

        Args:
            ks (str): keyspace name

        Returns:
            str: a string to create the keyspace
        """
        return "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = \
                { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };" % (ks)

    def create_ks(self, ks):
        if ks not in self.cluster.metadata.keyspaces:
            self.execute(self.create_ks_str(ks))

    def create_cf_str(self, ks, cf, cols):
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

    def create_cf(self, ks, cf, cols):
        if cf not in self.cluster.metadata.keyspaces[ks].tables.keys():
            self.execute(self.create_cf_str(ks, cf, cols))

    def get_meta(self):
        ks_meta = copy.copy(self.cluster.metadata.keyspaces)
        ks_exclude = ['system', 'system_traces'] # this is system keyspaces

        for e in ks_exclude:
            ks_meta.pop(e, None)

        """
        Exemplo meta
        {'db1': ['t1', 't2'], 'db2': ['t3', 't4']}
        """
        meta = {ks: list(cfs.tables.keys()) for ks, cfs in ks_meta.items()}
        return meta

    def execute(self, cmd):
        return self.session.execute(cmd)

    def insert(self, ks, cf, _id, body, timestamp=None, log=False):
        """Insert a new data into cs database"""
        cols = list(body.keys())
        if timestamp is None:
            timestamp = datetime.now()

        if log:
            print('ics %s %s %s %s' % (ks, cf, _id, timestamp))

        prepared_stmt = self.session.prepare( \
                "INSERT INTO %s.%s (id, timestamp, %s) VALUES (%s)" % \
                (ks, cf, ', '.join(cols), ','.join(['?'] * (len(cols)+2))))
        bound_stmt = prepared_stmt.bind([_id, timestamp] + [body[c] for c in cols])
        self.execute(bound_stmt)

    def update(self, ks, cf, _id, body, timestamp=None):
        """Update a es data"""
        cols = list(body.keys())
        if timestamp is None:
            timestamp = datetime.now()

        prepared_stmt = self.session.prepare( \
                "UPDATE %s.%s SET %s, timestamp = ? WHERE id= ?" % \
                (ks, cf, ', '.join('%s = ?' % (k) for k in body.keys())))
        bound_stmt = prepared_stmt.bind(list(body.values()) + [timestamp, _id])
        self.execute(bound_stmt)

    def clear(self):
        """Clear all cs data"""
        ks_meta = copy.copy(self.cluster.metadata.keyspaces)
        ks_exclude = ['system', 'system_traces'] # this is system keyspaces

        for e in ks_exclude:
            ks_meta.pop(e, None)

        drop_str = lambda ks: "drop keyspace %s;" % (ks)
        [self.execute(drop_str(ks)) for ks in ks_meta.keys()]

class ElasticsearchHelper:
    def __init__(self, es):
        self.es = es

    def insert(self, index, doc, _id, body, timestamp=None, log=False):
        """Insert a new data into es database"""
        if timestamp is None:
            timestamp = datetime.now()

        if log:
            print('ies: %s %s %s %s' % (index, doc, _id, timestamp))

        body['timestamp'] = timestamp
        self.es.index(index=index, doc_type=doc, id=_id, body=body)

    def clear(self):
        """Clear all es data"""
        self.es.indices.delete('_all')

# Functions to generate new random data
def rand_word(size=10):
    """Generate a random word with a given length"""
    return ''.join(random.choice(string.ascii_letters) for _ in range(size))

def rand_body(columns):
    """Generate a random body for a document"""
    return {col: rand_word() for col in columns}

def str_to_ts(ts_str):
    ts_fmt = '%Y-%m-%dT%H:%M:%S.%f'
    return datetime.strptime(ts_str, ts_fmt)
