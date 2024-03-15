from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.client.indices import IndicesClient
from elasticsearch.helpers import scan

from pyes.query_builder import Body, Query, Slice
from pyes.response import get_source, sources_from_response, get_sources, include_ids
from pyes.bulk import BulkBuilder, MultiGet, QueryBuilder
from pyfunk.pyfunk import get, now, comp, get_in, first, identity, swarm, assoc
from pyes.schema import checkargs, string


class TransformBuilder:
    def __init__(self):
        self.transform = None

    def add_transform(self, transform):
        if self.transform:
            self.transform = comp(transform, self.transform)
        else:
            self.transform = transform


def build_transform(transform=None, hits=True, just_one=False, include_id=False):
    tb = TransformBuilder()
    if include_id:
        tb.add_transform(include_ids)
    if hits:
        tb.add_transform(sources_from_response)
    if just_one:
        tb.add_transform(first)
    if transform:
        tb.add_transform(transform)
    return tb.transform or identity


class Store:
    """
    The standard store contract, this should outline all the functions
    available for a Store implementation
    """
    def create(self, id, index, doc):
        raise NotImplementedError()

    def upsert(self, id, index, doc):
        raise NotImplementedError()

    def update(self, id, index, doc):
        raise NotImplementedError()

    def index(self, id, index, doc):
        raise NotImplementedError()

    def get(self, id, index):
        raise NotImplementedError()

    def delete(self, id, index):
        raise NotImplementedError()

    def query(self, index, query, key=None):
        raise NotImplementedError()

    def suggest(self, index, field, prefix, key=None):
        raise NotImplementedError()

    def clear_cache(self, indices):
        raise NotImplementedError()


class ElasticsearchStore(Store):
    """
    The native ES implementation of the Store protocol
    """

    def __init__(self, es):
        self.es = es
        self.indices = IndicesClient(es)

    def create(self, id, index, doc):
        doc['created_time'] = now()
        self.es.create(id=id, index=index, body=doc)

    def upsert(self, id, index, doc):
        doc['upsert_time'] = now()
        body = {
            'doc': doc,
            'doc_as_upsert': True
        }
        self.es.update(id=id, index=index, body=body)

    def update(self, id, index, doc):
        doc['update_time'] = now()
        body = {
            'doc': doc
        }
        return self.es.update(id=id, index=index, body=body)

    def index(self, id, index, doc):
        return self.es.index(id=id, index=index, body=doc)

    def script_update(self, id, index, script, params=None, initial=None):
        script = {
            'source': script
        }
        if params:
            script['params'] = params
        body = {
            'script': script
        }
        if initial:
            body['upsert'] = initial
        return self.es.update(id=id, index=index, body=body)

    def get(self, id, index, **params):
        try:
            result = self.es.get(id=id, index=index, **params)
            if get(result, 'found'):
                return get_source(result)
        except NotFoundError:
            return None

    def delete(self, id, index):
        return self.es.delete(id=id, index=index)

    def delete_by_query(self, index, query):
        self.es.delete_by_query(index, Body().query(query).build())

    def query(self, index, query, key=None, transform=None, hits=True, just_one=False, include_id=False):
        result = self.es.search(index=index, body=query)

        store_transform = build_transform(transform, hits=hits, just_one=just_one, include_id=include_id)

        return store_transform(result)

    def count(self, index, query, key=None):
        result = self.es.count(index=index, body=query)
        return get(result, "count")

    def profile(self, index, query, no_source=True):
        query = assoc(query, "profile", True)
        if no_source:
            query = assoc(query, "_source", "")
        result = self.es.search(index=index, body=query)
        return get(result, "profile")

    def suggest(self, index, field, prefix, key=None, contexts=None):
        suggest_key = "suggest-key"
        results = self.es.search(
            index=index,
            body=Body().suggest(suggest_key, field, prefix, contexts=contexts).build()
        )
        results = get_in(results, ['suggest', suggest_key])
        options = get(first(results), 'options')
        return get_sources(options)

    def refresh_index(self, index):
        self.indices.refresh(index=index)

    def reindex(self, reindex_body):
        self.es.reindex(reindex_body.build())

    def scan(self, index, query=None, size=1000, scroll='5m'):
        if query is None:
            query = Body().query(Query().match_all()).build()
        for hit in scan(self.es, query=query, index=index, size=size, scroll=scroll):
            yield hit

    def sliced_scan(self, index, handler, query=None, fields=None,
                    slices=2, size=1000, scroll='5m', workers=None):
        if query is None:
            query = Query().match_all()

        def w_handler(slice_id):
            sliced_query = Body()\
                .query(query)\
                .slice(Slice(slice_id, slices))\
                .source(fields)\
                .build()
            for hit in self.scan(index,
                                 query=sliced_query,
                                 size=size,
                                 scroll=scroll):
                handler(hit)

        swarm(w_handler, range(0, slices), workers=workers or slices)

    @checkargs
    def get_mappings(self, index: string):
        return self.indices.get_mapping(index=index)

    @checkargs
    def put_mappings(self, index: string, mappings: {}):
        body = {"properties": mappings}
        self.indices.put_mapping(index=index, body=body)

    @checkargs
    def put_settings(self, index: string, settings: {}):
        self.indices.put_settings(index=index, body=settings)

    @checkargs
    def open(self, index: string):
        self.indices.open(index=index)

    @checkargs
    def close(self, index: string):
        self.indices.close(index=index)

    def explain(self, index, id, body):
        return self.es.explain(index, id, body=body)

    def clear_cache(self, index):
        self.indices.clear_cache(index=index)


class MultiWriteStore(Store):
    """
    A store that handles batch writes, each function call registers an
    intent to do a write, with the subsequent `write` function doing
    the bulk persist
    """
    def __init__(self):
        self.bulk_builder = BulkBuilder()

    def create(self, id, index, doc):
        doc['created_time'] = now()
        self.bulk_builder.create(id, index, doc)

    def upsert(self, id, index, doc):
        doc['upsert_time'] = now()
        self.bulk_builder.upsert(id, index, doc)

    def update(self, id, index, doc):
        doc['update_time'] = now()
        self.bulk_builder.update(id, index, doc)

    def index(self, id, index, doc):
        self.bulk_builder.index(id, index, doc)

    def script_update(self, id, index, script, params=None, initial=None):
        script = {
            'source': script
        }
        if params:
            script['params'] = params
        self.bulk_builder.script_update(id, index, script, initial=initial)

    def delete(self, id, index):
        self.bulk_builder.delete(id, index)

    def write(self, es, chunk_size=500):
        return self.bulk_builder.commit(es, chunk_size=chunk_size)

    def pending(self):
        return self.bulk_builder.count()


class MultiGetStore(Store):
    """
    A store that handles batch gets, each get call registers an
    intent to do a get, with the subsequent `get_all` function doing
    the bulk get
    """
    def __init__(self):
        self.multiget = MultiGet()
        self.keys = []

    def get(self, id, index, **params):
        self.multiget.get(id, index, id, **params)

    def get_all(self, es):
        response = self.multiget.multiget(es)
        return_value = {}
        for id, result in response.items():
            if get(result, 'found') is True and get(result, 'error') is None:
                return_value[id] = get_source(result)
            else:
                return_value[id] = None
        return return_value

    def pending(self):
        return self.multiget.count()


class MultiQueryStore(Store):
    """
    A store that handles batch queries, each function call registers an
    intent to do a query/suggest, with the subsequent `query_all` function doing
    the bulk query
    """
    def __init__(self):
        self.query_builder = QueryBuilder()

    def query(self, index, query, key=None, transform=None, hits=True, just_one=False, include_id=False):
        if key is None:
            raise ValueError("A query key must be supplied")

        store_transform = build_transform(transform, hits=hits, just_one=just_one, include_id=include_id)

        self.query_builder.query(key, index, query, transform=store_transform)

    def suggest(self, index, field, prefix, key=None, contexts=None):
        if key is None:
            raise ValueError("A query key must be supplied")

        def transform(response):
            options = get_in(response, ['suggest', key, 'options'])
            return get_sources(options)

        self.query_builder.query(key, index, Body().suggest(key, field, prefix), transform=transform)

    def query_all(self, es):
        return self.query_builder.search(es)

    def pending(self):
        return self.query_builder.count()


class BatchStore(Store):
    """
    A store that wraps the MultiWriteStore, MultiGetStore and MultiQueryStore
    """
    def __init__(self, es):
        self.multi_write_store = MultiWriteStore()
        self.multi_get_store = MultiGetStore()
        self.multi_query_store = MultiQueryStore()
        self.es = es

    def create(self, id, index, doc):
        self.multi_write_store.create(id, index, doc)

    def upsert(self, id, index, doc):
        self.multi_write_store.upsert(id, index, doc)

    def update(self, id, index, doc):
        self.multi_write_store.update(id, index, doc)

    def index(self, id, index, doc):
        self.multi_write_store.index(id, index, doc)

    def script_update(self, id, index, script, params=None, initial=None):
        self.multi_write_store.script_update(id, index, script, params=params, initial=initial)

    def get(self, id, index, **params):
        self.multi_get_store.get(id, index, **params)

    def delete(self, id, index):
        self.multi_write_store.delete(id, index)

    def query(self, index, query, key=None, transform=None, hits=True, just_one=False, include_id=False):
        self.multi_query_store.query(index, query, key=key, transform=transform, hits=hits, just_one=just_one,
                                     include_id=include_id)

    def count(self, index, query, key=None):
        query['size'] = 0
        query['track_total_hits'] = True
        self.query(index, query, key=key, hits=False, transform=lambda result: get_in(result, ['hits', 'total', 'value']))

    def suggest(self, index, field, prefix, key=None, contexts=None):
        self.multi_query_store.suggest(index, field, prefix, key=key, contexts=contexts)

    def write(self, chunk_size=500):
        self.multi_write_store.write(self.es, chunk_size=chunk_size)

    def do_get(self):
        return self.multi_get_store.get_all(self.es)

    def do_query(self):
        return self.multi_query_store.query_all(self.es)

    def pending_writes(self):
        return self.multi_write_store.pending()

    def pending_gets(self):
        return self.multi_get_store.pending()

    def pending_queries(self):
        return self.multi_query_store.pending()


class MegaStore(Store):
    """
    A store that wraps the ElasticsearchStore and the BatchStore,
    each function has a batch argument to decide which store we delegate
    to. If batch is set to true, the calls will delegate to the BatchStore,
    with operations being realized with the `batch_write`, `batch_get` and
    `batch_query` functions. If batch is false (which it is by default),
    the ElasticsearchStore is used, and the result it evaluated immediately
    """
    def __init__(self, es):
        self.es = es
        self.elasticsearch_store = ElasticsearchStore(es)
        self.batch_store = BatchStore(es)

    def get_store(self, batch):
        if batch:
            return self.batch_store
        else:
            return self.elasticsearch_store

    def create(self, id, index, doc, batch=False):
        self.get_store(batch).create(id, index, doc)

    def upsert(self, id, index, doc, batch=False):
        self.get_store(batch).upsert(id, index, doc)

    def update(self, id, index, doc, batch=False):
        self.get_store(batch).update(id, index, doc)

    def index(self, id, index, doc, batch=False):
        self.get_store(batch).index(id, index, doc)

    def script_update(self, id, index, script, params=None, initial=None, batch=False):
        self.get_store(batch).script_update(id, index, script, params=params, initial=initial)

    def get(self, id, index, batch=False, **params):
        return self.get_store(batch).get(id, index, **params)

    def delete(self, id, index, batch=False):
        self.get_store(batch).delete(id, index)

    def delete_by_query(self, index, query):
        self.get_store(False).delete_by_query(index, query)

    def query(self, index, query, key=None, batch=False, transform=None, hits=True,
              just_one=False, include_id=False):
        if isinstance(query, Body):
            query = query.build()
        return self.get_store(batch).query(index, query, key=key, transform=transform, hits=hits,
                                           just_one=just_one, include_id=include_id)

    def count(self, index, query, key=None, batch=False):
        if isinstance(query, Query):
            query = query.build()

        query = {'query': query}

        return self.get_store(batch).count(index, query, key=key)

    def profile(self, index, query, no_source=True):
        return self.get_store(False).profile(index, query, no_source=no_source)

    def suggest(self, index, field, prefix, key=None, batch=False, contexts=None):
        return self.get_store(batch).suggest(index, field, prefix, key=key, contexts=contexts)

    def batch_write(self, size=500):
        self.batch_store.write(chunk_size=size)

    def batch_get(self):
        return self.batch_store.do_get()

    def batch_query(self):
        return self.batch_store.do_query()

    def refresh_index(self, index):
        self.get_store(False).refresh_index(index)

    def reindex(self, reindex_body):
        self.get_store(False).reindex(reindex_body)

    def scan(self, index, query=None, size=1000, scroll='5m'):
        return self.elasticsearch_store.scan(index, query=query, size=size, scroll=scroll)

    def sliced_scan(self, index, handler, query=None, fields=None,
                    slices=2, size=1000, scroll='5m', workers=None):
        self.elasticsearch_store.sliced_scan(index, handler,
                                             query=query,
                                             fields=fields,
                                             slices=slices,
                                             size=size,
                                             scroll=scroll,
                                             workers=workers)

    def get_mappings(self, index):
        return self.elasticsearch_store.get_mappings(index)

    def put_mappings(self, index, mappings):
        self.elasticsearch_store.put_mappings(index, mappings)

    def put_settings(self, index, settings):
        self.elasticsearch_store.put_settings(index, settings)

    def open(self, index):
        self.elasticsearch_store.open(index)

    def close(self, index):
        self.elasticsearch_store.close(index)

    def explain(self, index, id, body):
        return self.elasticsearch_store.explain(index, id, body)

    def pending_writes(self):
        return self.batch_store.pending_writes()

    def pending_gets(self):
        return self.batch_store.pending_gets()

    def pending_queries(self):
        return self.batch_store.pending_queries()

    def clear_cache(self, index):
        return self.elasticsearch_store.clear_cache(index)


def new_mega_store(hostname="localhost"):
    es = Elasticsearch(hostname)
    return MegaStore(es)


class ConflictException(Exception):
    pass
