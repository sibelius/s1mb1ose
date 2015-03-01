#!/usr/bin/env python
"""This file simulate insert and update operations on ElasticSearch (es) and Cassandra (cs) databases"""

from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
from uuid import uuid4
from datetime import datetime
import random
import string
import time
from util import CassandraHelper, ElasticsearchHelper, rand_body

# meta data to generate the data for both databases
meta = {
    'db1': {
        't1': ['c1', 'c2'],
        't2': ['c3', 'c4']
    },
    'db2': {
        't3': ['c5', 'c6'],
        't4': ['c7', 'c8']
    }
}

def init_cs(cs_helper, meta):
    """Cassandra requires to first create the keyspaces and the 'tables' definition"""
    for ks in meta.keys():
        cs_helper.execute(cs_helper.create_ks_str(ks))
        for cf in meta[ks].keys():
            cs_helper.execute(cs_helper.create_cf_str(ks, cf, meta[ks][cf]))

# Functions to generate new random data
def rand_data(meta):
    db = random.choice(list(meta.keys()))
    table = random.choice(list(meta[db].keys()))
    body = rand_body(meta[db][table])
    _id = uuid4()

    return db, table, _id, body

def insert_rand_data(es_helper, cs_helper, meta):
    """Insert a random data inside es or cs"""
    db, table, _id, body = rand_data(meta)

    # 0 - only es, 1 - only cs, 2 - both
    op = random.randint(0,2)

    ies = lambda: es_helper.insert(db, table, _id, body, log=True)
    ics = lambda: cs_helper.insert(db, table, _id, body, log=True)

    if op == 0:
        ies()
    elif op == 1:
        ics()
    else:
        order = random.randint(0,1) # 0 - es first, 1 - cs first

        if order == 0:
            ies()
            time.sleep(1) # wait a little bit
            ics()
        else:
            ics()
            time.sleep(1)
            ies()

def simulate():
    """simulate insertions into es and cs"""
    # Cassandra variables
    cluster = Cluster()
    session = cluster.connect()

    cs_helper = CassandraHelper(cluster, session)

    # Elasticsearch variable
    es = Elasticsearch()

    es_helper = ElasticsearchHelper(es)

    init_cs(cs_helper, meta)
    while True:
        cmd = input('new data (y/n), clear data (c): ')

        if cmd == 'y':
            insert_rand_data(es_helper, cs_helper, meta)
        elif cmd == 'c':
            print('cleaning...')
            es_helper.clear()
            cs_helper.clear()
            break
        elif cmd == 'n':
            break

if __name__ == "__main__":
    simulate()
