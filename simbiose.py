#!/usr/bin/env python

from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
import cassandra
import uuid
import json
import time
from util import CassandraHelper, ElasticsearchHelper, str_to_ts

class SyncDB:
    """Class to sync the ES and CS databases"""
    def __init__(self, es, cluster, session):
        self.es_helper = ElasticsearchHelper(es)
        self.cs_helper = CassandraHelper(cluster, session)

    def get_sync_data(self, _type):
        sync_fn = {
            'es' : lambda: self.es_sync_data(),
            'cs' : lambda: self.cs_sync_data()
        }
        return sync_fn[_type]()

    def es_sync_data(self):
        """Get enough data from cs to synchronize the databases"""
        query = self.es_helper.es.search(fields=['_id', 'timestamp'])

        fill = lambda doc: {'db': doc['_index'], 'table': doc['_type'], 'timestamp': str_to_ts(doc['fields']['timestamp'][0])}

        try:
            data = {doc['_id']: fill(doc) for doc in query['hits']['hits']}
        except:
            data = {}

        return data

    def _cs_data_cf(self, ks, cf):
        """Return id and timestamp for one column family inside a keyspace"""
        result = self.cs_helper.execute('SELECT id, timestamp FROM %s.%s' % (ks, cf))
        if result is None:
            return {}
        else:
            fill = lambda res: {'db' : ks, 'table': cf, 'timestamp': res['timestamp']}

            data = {str(res['id']): fill(res) for res in result}

            return data

    def cs_sync_data(self):
        """Get enough data from cs to synchronize the databases"""
        meta_cs = self.cs_helper.get_meta()

        datas = []
        for ks in meta_cs.keys():
            for cf in meta_cs[ks]:
                datas.append(self._cs_data_cf(ks,cf))

        # merge dicts
        data = {k:v for d in datas for (k,v) in d.items()}

        return data

    def es_to_cs_insert(self, data, _id):
        """Transfer a new data from es to cs"""
        print('es_to_cs_insert: %s' % (_id))
        ks = data[_id]['db']
        cf = data[_id]['table']

        data = self.es_helper.es.get(index=ks, doc_type=cf, id=_id)
        cols = list(data['_source'].keys())
        cols.remove('timestamp') # the timestamp is already determined

        # we need to create the keyspace and column families definitions
        self.cs_helper.create_ks(ks)
        self.cs_helper.create_cf(ks, cf, cols)

        timestamp = str_to_ts(data['_source']['timestamp'])

        body = {k:v for k,v in data['_source'].items() if k != 'timestamp'}

        self.cs_helper.insert(ks, cf, uuid.UUID(_id), body, timestamp)

    def es_to_cs_update(self, data, _id):
        """Sync recent data from es to cs"""
        print('es_to_cs_update %s' % (_id))
        ks = data[_id]['db']
        cf = data[_id]['table']

        data = self.es_helper.es.get(index=ks, doc_type=cf, id=_id)
        cols = list(data['_source'].keys())
        cols.remove('timestamp') # the timestamp is already determined

        timestamp = str_to_ts(data['_source']['timestamp'])
        body = {k:v for k,v in data['_source'].items() if k != 'timestamp'}

        self.cs_helper.update(ks, cf, uuid.UUID(_id), body, timestamp)

    def cs_to_es_insert(self, data, _id):
        """Transfer a new data from cs to es"""
        print('cs_to_es_insert %s' % (_id))
        index = data[_id]['db']
        doc = data[_id]['table']

        select = lambda ks, cf, _id: 'select * from %s.%s where id=%s' % (ks,cf,uuid.UUID(_id))

        data = self.cs_helper.execute(select(index, doc, _id))[0]
        timestamp = data['timestamp']
        body = {k:v for k,v in data.items() if k != 'timestamp' and k != 'id'}

        self.es_helper.insert(index, doc, uuid.UUID(_id), body, timestamp)

    def sync(self):
        types = ['es', 'cs']
        sync_data = {_type: self.get_sync_data(_type) for _type in types}

        # indexes per es and cs
        idxs = {k:set(v.keys()) for k,v in sync_data.items()}

        both = list(idxs['es'].intersection(idxs['cs']))
        only_es = list(idxs['es'].difference(idxs['cs'])) # es -> cs
        only_cs = list(idxs['cs'].difference(idxs['es'])) # cs -> es

        if len(only_es) > 0:
            print('es ==> cs syncing...')
            [self.es_to_cs_insert(sync_data['es'], _id) for _id in only_es]

        if len(only_cs) > 0:
            print('cs ==> es syncing...')
            [self.cs_to_es_insert(sync_data['cs'], _id) for _id in only_cs]

        # return if the _id is more recent into cs or es
        eq = lambda _id: abs(sync_data['cs'][_id]['timestamp'] - \
                sync_data['es'][_id]['timestamp']).seconds == 0
        gt = lambda _id: sync_data['cs'][_id]['timestamp'] > \
                sync_data['es'][_id]['timestamp']

        # remove idx where the timestamp is the same, i.e., already synchronized
        both_neq = [idx for idx in both if not eq(idx)]

        cs_recent = [gt(_id) for _id in both_neq]
        both_cs_to_es = [both_neq[i] for i in range(len(both_neq)) if cs_recent[i]]
        both_es_to_cs = [both_neq[i] for i in range(len(both_neq)) if not cs_recent[i]]

        if len(both_cs_to_es) > 0:
            print('cs ==> es update syncing...')
            [self.cs_to_es_insert(sync_data['cs'], _id) for _id in both_cs_to_es]

        if len(both_es_to_cs) > 0:
            print('es ==> cs update syncing...')
            [self.es_to_cs_update(sync_data['es'], _id) for _id in both_es_to_cs]

        print('finished!')

        return sync_data, both_neq, only_es, only_cs

def get_period():
    """Read the period of sleep for this daemon, or 5 seconds as default"""
    try:
        return json.load(open('config.json', 'r'))['period']
    except:
        return 5

def run():
    """Synchronize es and cs databases"""
    es = Elasticsearch()

    cluster = Cluster()
    session = cluster.connect()
    # dict factory makes easy to handle unknown columns
    session.row_factory = cassandra.query.dict_factory
    sync_db = SyncDB(es, cluster, session)

    while True:
        sync_db.sync()
        time.sleep(get_period())

if __name__ == '__main__':
    run()

# return the writetime for a given column
# select writetime (column name)

#column.column.clock.timestamp
