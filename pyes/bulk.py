import threading

from elasticsearch.helpers import parallel_bulk
from pyes.response import get_hits
from pyfunk.pyfunk import get, zipmap, count


class BulkBuilder(object):
    def __init__(self):
        self.bulks = []
        self.lock = threading.Lock()

    def index(self, id, index, doc, parent=None):
        action = {
            '_op_type': 'index',
            '_index': index,
            '_id': id,
            '_source': doc
        }
        if parent is not None:
            action['_parent'] = parent
        self.bulks.append(action)

    def create(self, id, index, doc, parent=None):
        action = {
            '_op_type': 'create',
            '_index': index,
            '_id': id,
            '_source': doc
        }
        if parent is not None:
            action['_parent'] = parent
        self.bulks.append(action)

    def script_update(self, id, index, script, initial=None, parent=None):
        doc = {
            "script": script
        }
        if initial:
            doc["upsert"] = initial
        action = {
            '_op_type': 'update',
            '_index': index,
            '_id': id,
            '_source': doc
        }
        if parent is not None:
            action['_parent'] = parent
        self.bulks.append(action)

    def update(self, id, index, doc, parent=None):
        action = {
            '_op_type': 'update',
            '_index': index,
            '_id': id,
            'doc': doc
        }
        if parent is not None:
            action['_parent'] = parent
        self.bulks.append(action)

    def upsert(self, id, index, doc, parent=None):
        action = {
            '_op_type': 'update',
            '_index': index,
            '_id': id,
            'doc': doc,
            'doc_as_upsert': True
        }
        if parent is not None:
            action['_parent'] = parent
        self.bulks.append(action)

    def delete(self, id, index):
        action = {
            '_op_type': 'delete',
            '_index': index,
            '_id': id,
        }
        self.bulks.append(action)

    def commit(self, es, thread_count=4, chunk_size=500):
        with self.lock:
            to_commit = self.bulks
            self.reset()
        if to_commit:
            g = parallel_bulk(es, to_commit, thread_count=thread_count, chunk_size=chunk_size)
            return [x for x in g]
        return []

    def reset(self):
        self.bulks = []

    def count(self):
        return count(self.bulks)


class QueryBuilder(object):
    def __init__(self):
        self.queries = {}
        self.transforms = {}

    def query(self, query_key, index, query, transform=get_hits):
        command = {
            'index': index,
        }

        self.transforms[query_key] = transform

        self.queries[query_key] = [command, query]

    def search(self, es):
        ks = self.queries.keys()
        values = [self.queries[k] for k in ks]

        search_array = []
        for value in values:
            search_array.extend(value)

        returned_responses = {}

        if search_array:
            responses = get(es.msearch(body=search_array), 'responses')
            responses_by_key = zipmap(ks, responses)

            if responses_by_key:
                for k, response in responses_by_key.items():
                    transform = get(self.transforms, k)
                    if transform:
                        returned_responses[k] = transform(response)
                    else:
                        returned_responses[k] = response
        self.reset()
        return returned_responses

    def reset(self):
        self.queries = {}

    def count(self):
        return count(self.queries)


class MultiGet(object):
    def __init__(self):
        self.gets = {}

    def get(self, key, index, id, parent=None, **params):
        get = {
            "_index": index,
            "_id": id,
            **params
        }

        if parent:
            get["_parent"] = parent

        self.gets[key] = get

    def reset(self):
        self.gets = {}

    def multiget(self, es):
        ks = self.gets.keys()
        values = [self.gets[k] for k in ks]
        if values:
            response = es.mget(body={'docs': values})
            self.reset()
            return zipmap(ks, get(response, 'docs'))
        self.reset()
        return {}

    def count(self):
        return count(self.gets)