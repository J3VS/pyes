from elasticsearch.helpers import BulkIndexError

from pyes.query_builder import Body, Must, Query, SortDirection, Filter, Should, MustNot, Reindex
from pyes.store import ConflictException
from pyes.validators import NotExistsException
from pyes.response import get_source
from pyfunk.pyfunk import count, get, partition, swarm, partial, now
from pyes.timing import log_time
from pyes.utils import uuid

MAX_GET_ALL = 1000

MATCH_ALL = Query().match_all()


class ESCrudService:
    def __init__(self, es, index):
        self.es = es
        self.index = index

    @checkargs
    def _get_all_helper(self, fields: [string] = [], entity_ids: [string] = []):
        query = Query()

        filter = Filter()
        filter.terms('_id', entity_ids)

        body = Body().query(query.bool(filter)).size(len(entity_ids)).source(fields)
        results = self.query(body, raw_query=True, hits=False)
        result_dict = {}

        for result in get(get(results, 'hits'), 'hits'):
            result_dict[get(result, '_id')] = get_source(result)

        return result_dict

    @checkargs
    def create(self,
               entity: {},
               entity_id: string_or_nil = None,
               batch: boolean = False):
        entity_id = entity_id or uuid()
        entity['uid'] = entity_id
        self.es.create(entity_id, self.index, entity, batch=batch)
        return entity_id

    @checkargs
    def index_doc(self,
                  entity: {},
                  entity_id: string_or_nil = None,
                  batch: boolean = False):
        self.es.index(entity_id, self.index, entity, batch=batch)

    @checkargs
    def get_entity(self,
                   entity_id: string,
                   batch: boolean = False,
                   source: nillable([string])=None):
        params = {"_source": source} if source is not None else {}
        return self.es.get(entity_id, self.index, batch=batch, **params)

    @checkargs
    def get_all(self, entity_ids: [string]):
        for entity_id in entity_ids:
            self.get_entity(entity_id, batch=True)
        return self.batch_get()

    @checkargs
    def get_entities(self,
                     entity_ids: [string],
                     limit: number = 1000,
                     batch: boolean = False):
        query = Query().bool(Must().terms("_id", entity_ids))
        query = Body().query(query).size(limit)

        return self.es.query(self.index, query, batch=batch)

    @checkargs
    def exists(self,
               entity_id: string,
               throw: boolean = True):
        record_exists = self.get_entity(entity_id) is not None
        if not record_exists and throw:
            raise NotExistsException("{0} does not exist for id {1}".format(self.index, entity_id))
        else:
            return record_exists

    @checkargs
    def unique_after_update(self,
                            entity_id: string,
                            fields: {},
                            throw: boolean = True):
        existing = self.query(fields, just_one=True)
        if existing:
            existing_id = get(existing, 'uid')
            if existing_id == entity_id:
                return True
            else:
                if throw:
                    raise ConflictException("Update of {0} causes a conflict".format(fields))
                else:
                    return False
        return True

    @checkargs
    def update(self,
               entity_id: string,
               update: {},
               batch: boolean = False,
               check_existence: boolean = True):
        if not check_existence or self.exists(entity_id, throw=not batch):
            self.es.update(entity_id, self.index, update, batch=batch)

    @checkargs
    def upsert(self,
               entity_id: string,
               entity: {},
               batch: boolean = False):
        self.es.upsert(entity_id, self.index, entity, batch=batch)

    @checkargs
    def script_update(self,
                      entity_id: string,
                      inline: string):
        self.es.script_update(entity_id, self.index, inline)

    @checkargs
    def delete(self,
               entity_id: string,
               batch: boolean = False,
               check_existence: boolean = True):
        if not check_existence or self.exists(entity_id, throw=not batch):
            self.es.delete(entity_id, self.index, batch=batch)

    @checkargs
    def delete_by_query(self, query: type_of(Query)):
        self.es.delete_by_query(self.index, query)

    @log_time(threshold=10000)
    @checkargs
    def query(self,
              query: s_or({}, type_of(Body)),
              limit: number = 1000,
              sort: string_or_nil = None,
              sort_direction: string_or_nil = SortDirection.ASC,
              just_one: boolean = False,
              raw_query: boolean = False,
              key: string_or_nil = None,
              batch: boolean = False,
              hits: boolean = True,
              include_id: boolean = False,
              transform: nillable(function) = None,
              fields: nillable([string]) = None):
        if not raw_query:
            if isinstance(query, dict):
                query = Query().bool(Must(query))
                query = Body().query(query)

            if limit is not None and query.limit is None:
                query.size(limit)

            if sort:
                query.sort(sort, sort_direction)

            if fields:
                query.source(fields)

        return self.es.query(self.index, query, just_one=just_one, key=key,
                             batch=batch, hits=hits, transform=transform, include_id=include_id)

    @checkargs
    def count(self,
              query: s_or({}, type_of(Query)) = MATCH_ALL,
              batch: boolean = False,
              key: string_or_nil = None):
        return self.es.count(self.index, query, batch=batch, key=key)

    @checkargs
    def unique_by_query(self,
                        fields: {},
                        throw: boolean = True,
                        error_msg_fields: nillable({}) = {}):
        c = count(self.query(fields)) > 0
        if c and throw:
            error_msg = "Query for {0}, already exists. {1}".format(fields, error_msg_fields) if error_msg_fields else \
                "Query for {0}, already exists.".format(fields)
            raise ConflictException(error_msg)
        else:
            return not c

    @checkargs
    def overwrite(self,
                  entity_id: string,
                  entity: {},
                  batch: boolean = False):
        self.es.index(entity_id, self.index, entity, batch=batch)

    @checkargs
    def suggest(self,
                prefix: string,
                key: string_or_nil = None,
                batch: boolean = False,
                contexts: nillable({}) = None):
        return self.es.suggest(self.index, "text_suggest", prefix, key=key, batch=batch, contexts=contexts)


    @checkargs
    def get_all(self, entity_ids: [string] = [], fields: [string] = []):
        # TODO IBP-4075 After ES is upgraded use Multi GET.
        # for entity_id in entity_ids:
        #     self.es.get(entity_id, self.index, self.type, batch=True)
        # return self.es.batch_get()
        all_results = {}
        entity_id_groups = partition(MAX_GET_ALL, entity_ids)

        def callback(_, results):
            all_results.update(results)

        swarm(partial(self._get_all_helper, fields), entity_id_groups, callback=callback,
              workers=min(count(entity_id_groups), 40))

        return all_results

    @checkargs
    def match_all(self, size: number = 1000):
        return self.query(Body().query(Query().match_all()).size(size), raw_query=True)

    def refresh(self):
        self.es.refresh_index(self.index)

    def scan(self, query=None, size=1000, scroll='5m'):
        return self.es.scan(self.index, query=query, size=size, scroll=scroll)

    def batch_scan(self, query=None, size=1000, scroll='5m'):
        batch = []
        for hit in self.scan(query=query, size=size, scroll=scroll):
            batch.append(hit)
            if count(batch) == size:
                batch_to_yield = batch
                batch = []
                yield batch_to_yield
        yield batch

    def sliced_scan(self, handler, query=None, fields=None, slices=2, size=1000, scroll='5m', workers=None):
        self.es.sliced_scan(self.index, handler, query=query, fields=fields, slices=slices,
                            size=size, scroll=scroll, workers=workers)

    def profile(self, query):
        return self.es.profile(self.index, query)

    @checkargs
    def find_first(self,
                   queries: [s_or({}, type_of(Body))]):
        for query in queries:
            result = self.query(query, just_one=True)
            if result:
                return result
        return None

    @checkargs
    def find_all(self,
                 keyed_queries: {string: s_or({}, type_of(Body))},
                 fields: nillable([string]) = None):
        for key, query in keyed_queries.items():
            self.query(query, key=key, fields=fields, just_one=True, batch=True)
        return self.es.batch_query()

    def get_mappings(self):
        return self.es.get_mappings(self.index)

    @checkargs
    def put_mappings(self, mappings: {}):
        self.es.put_mappings(self.index, mappings)

    @checkargs
    def put_settings(self, settings: {}):
        self.es.put_settings(self.index, settings)

    def open(self):
        self.es.open(self.index)

    def close(self):
        self.es.close(self.index)

    @checkargs
    def reindex(self, reindex_body: type_of(Reindex)):
        self.es.reindex(reindex_body)

    @checkargs
    def explain(self, id: string, body: dictionary):
        return self.es.explain(self.index, id, body)

    def batch_get(self):
        return self.es.batch_get()

    def batch_write(self):
        self.es.batch_write()

    def batch_query(self):
        return self.es.batch_query()

    def pending_writes(self):
        return self.es.pending_writes()
    
    def flush_if_necessary(self, batch_size, on_write=None, on_error=None):
        if self.pending_writes() >= batch_size:
            try:
                self.batch_write()
                if on_write:
                    on_write()
            except BulkIndexError as e:
                if on_error:
                    on_error(e)
                else:
                    raise e

    def clear_cache(self):
        self.es.clear_cache(self.index)


class ESSoftCrudService(ESCrudService):
    @staticmethod
    def is_soft_deleted(record, deleted_time=None):
        if deleted_time is None:
            deleted_time = now()
        record_deleted_time = get(record, 'deleted_time')
        return record_deleted_time and record_deleted_time < deleted_time

    @staticmethod
    def soft_delete_update(deleted_time=None):
        if deleted_time is None:
            deleted_time = now()
        return {'deleted_time': deleted_time}

    @staticmethod
    def not_deleted_query(deleted_time=None):
        if deleted_time is None:
            deleted_time = now()
        return Should().bool(MustNot().exists('deleted_time')).range('deleted_time', gt=deleted_time)

    @checkargs
    def get_including_deleted(self,
                              entity_id: string,
                              batch: boolean = False):
        return super().get_entity(entity_id, batch=batch)

    @checkargs
    def get_entity(self,
                   entity_id: string,
                   batch: boolean = False):
        record = super().get_entity(entity_id, batch=batch)
        if self.is_soft_deleted(record):
            return None
        return record

    @checkargs
    def delete(self,
               entity_id: string,
               batch: boolean = False):
        if self.exists(entity_id, throw=not batch):
            super().update(entity_id, self.soft_delete_update(), batch=batch)

    @checkargs
    def exists_including_deleted(self,
                                 entity_id: string,
                                 throw: boolean = True):
        record_exists = self.get_including_deleted(entity_id) is not None
        if not record_exists and throw:
            raise NotExistsException("{0} does not exist for id {1}".format(self.index, entity_id))
        else:
            return record_exists

    @checkargs
    def hard_delete(self,
                    entity_id: string,
                    batch: boolean = False):
        if self.exists_including_deleted(entity_id, throw=not batch):
            self.es.delete(entity_id, self.index, batch=batch)

    @checkargs
    def get_entities(self,
                     entity_ids: [string],
                     limit: number = 1000,
                     batch: boolean = False):
        query = Query().bool(Must().terms("_id", entity_ids), self.not_deleted_query())
        query = Body().query(query).size(limit)

        return self.es.query(self.index, query, batch=batch)

    @checkargs
    def query(self,
              query: s_or({}, type_of(Body)),
              limit: number = 1000,
              sort: string_or_nil = None,
              sort_direction: string_or_nil = SortDirection.ASC,
              just_one: boolean = False,
              raw_query: boolean = False,
              key: string_or_nil = None,
              batch: boolean = False,
              hits: boolean = True,
              include_id: boolean = False,
              transform: nillable(function) = None,
              fields: nillable([string]) = None):
        if not raw_query:
            if isinstance(query, dict):
                query = Query().bool(Must(query))
                query = Body().query(query)

        query_term = query.query_term
        new_must = Must().derive(query_term)
        new_must.bool(self.not_deleted_query())

        query = Body().query(Query().bool(new_must))

        return super().query(query,
                             limit=limit,
                             sort=sort,
                             sort_direction=sort_direction,
                             just_one=just_one,
                             raw_query=raw_query,
                             key=key,
                             batch=batch,
                             hits=hits,
                             include_id=include_id,
                             transform=transform,
                             fields=fields)
