from elasticsearch.client.indices import IndicesClient

from pyes.crud import ESCrudService
from pyes.query_builder import Query, Body, Reindex
from pyes.response import get_hits, get_sources, get_index, get_type, get_id
from pyfunk.pyfunk import get, first, keys, get_in, first_key_match
from pyes.schema import checkargs, string, boolean, number, type_of


class MultiIndexESCrudService(ESCrudService):
    def __init__(self, es, alias):
        self.alias = alias
        self.indices = IndicesClient(es.es)
        super().__init__(es, alias)

    def get_indexes(self):
        index_info = self.indices.get(index=self.alias)
        indexes = []
        for index, info in index_info.items():
            indexes.append({
                'index': index,
                'is_write_index': bool(get_in(info, ['aliases', self.alias, 'is_write_index'])),
                'type': first(keys(get(info, 'mappings')))
            })
        return indexes

    @staticmethod
    def get_write_index(indexes):
        return first_key_match('is_write_index', True, indexes)

    @checkargs
    def get_entity_hits(self,
                        entity_ids: [string],
                        limit: number = 1000):
        query = Query().terms("_id", entity_ids)
        query = Body().query(query).size(limit)

        hits = get_hits(self.es.query(self.index, query, hits=False))
        hits_by_id = {get_id(hit): hit for hit in hits}
        return [get(hits_by_id, entity_id) for entity_id in entity_ids]

    @checkargs
    def get_entities(self,
                     entity_ids: [string],
                     limit: number = 1000,
                     batch: boolean = False):
        return get_sources(self.get_entity_hits(entity_ids, limit=limit))

    @checkargs
    def get_entity(self,
                   entity_id: string,
                   limit: number = 1000,
                   batch: boolean = False):
        return first(self.get_entities([entity_id], limit=limit))

    def iterate_entities(self, entity_ids):
        hits = self.get_entity_hits(entity_ids)
        for hit in hits:
            yield get_id(hit), get_index(hit)

    @checkargs
    def update(self,
               entity_id: string,
               update: {}):
        self.update_all({entity_id: update})

    @checkargs
    def update_all(self,
                   update_by_id: {string: {}},
                   batch: boolean = False):
        for entity_id, index in self.iterate_entities(keys(update_by_id)):
            update = get(update_by_id, entity_id)
            if update:
                self.es.update(entity_id, index, update, batch=True)
        if not batch:
            self.es.batch_write()

    @checkargs
    def upsert(self,
               entity_id: string,
               entity: {}):
        self.es.upsert_all({entity_id: entity})

    @checkargs
    def upsert_all(self,
                   entity_by_id: {string: {}},
                   batch: boolean = False):
        for entity_id, index in self.iterate_entities(keys(entity_by_id)):
            update = get(entity_by_id, entity_id)
            if update:
                self.es.upsert(entity_id, index, update, batch=True)
        if not batch:
            self.es.batch_write()

    @checkargs
    def delete(self, entity_id: string):
        self.delete_all([entity_id])

    @checkargs
    def delete_all(self,
                   entity_ids: [string],
                   batch: boolean = False):
        for entity_id, index in self.iterate_entities(entity_ids):
            self.es.delete(entity_id, index, batch=True)
        if not batch:
            self.es.batch_write()

    @checkargs
    def overwrite(self,
                  entity_id: string,
                  entity: {}):
        self.overwrite({entity_id: entity})

    @checkargs
    def overwrite_all(self,
                      entity_by_id: {string: {}},
                      batch: boolean = False):
        for entity_id, index in self.iterate_entities(keys(entity_by_id)):
            entity = get(entity_by_id, entity_id)
            if entity:
                self.es.index(entity_id, index, entity, batch=True)
        if not batch:
            self.es.batch_write()

    @checkargs
    def refresh(self):
        for index in self.get_indexes():
            self.es.refresh_index(get(index, 'index'))


# Assumes only 2 indexes, with one marked as `is_write_index`
class ArchivingESCrudService(MultiIndexESCrudService):
    def __init__(self, es, alias):
        super().__init__(es, alias)

    @staticmethod
    def get_archive_index(indexes):
        return first_key_match('is_write_index', False, indexes)

    @checkargs
    def archive(self, query=type_of(Query)):
        indexes = self.get_indexes()
        write_index = get(self.get_write_index(indexes), 'index')
        archive_index = get(self.get_archive_index(indexes), 'index')

        self.reindex(
            Reindex()
            .source(write_index, query=query)
            .dest(archive_index)
        )
        self.es.delete_by_query(write_index, query)
