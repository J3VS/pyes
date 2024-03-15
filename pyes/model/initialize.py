from typing import Optional

from elasticsearch.client.indices import IndicesClient
import logging

from pyes.model.config import MAPPINGS, SETTINGS
from pyfunk.pyfunk import get, first, last, merge, get_in

logger = logging.getLogger(__name__)


def get_iteration(index_name):
    return int(last(index_name.split('_')))


class IndexInitialization:
    def __init__(self, es):
        self.es = es
        self.indices = IndicesClient(es)

    def create_index(self, index_name, mappings_key, shards=1, replicas=1):
        logger.info("Creating index {index_name}, with mappings for key {mappings_key}".format(index_name=index_name,
                                                                                               mappings_key=mappings_key))

        mappings = get(MAPPINGS, mappings_key)
        settings = get(SETTINGS, mappings_key)

        body = {
            "mappings": mappings,
            "settings": merge(settings, {
                "index": merge(get(settings, "index", {}), {
                    "number_of_shards": shards,
                    "number_of_replicas": replicas,
                })
            })
        }

        self.indices.create(index=index_name, body=body)

    def get_next_name(self, alias):
        iteration = 0
        if self.indices.exists(alias):
            existing_name = self.get_index_name(alias)
            iteration = get_iteration(existing_name)
        return "{index}_{iteration}".format(index=alias, iteration=iteration + 1)

    def delete_index(self, index_name):
        logger.info("Deleting index {index}".format(index=index_name))
        self.indices.delete(index=index_name)

    def exists(self, name):
        return self.indices.exists(name)

    def add_alias(self, index_name: str, alias: str, is_write_index: Optional[bool] = True):
        logger.info("Adding alias {alias} to index {index}. Write: {write}".format(
            alias=alias, index=index_name, write=is_write_index
        ))

        body = {}

        if is_write_index is not None:
            body["is_write_index"] = is_write_index

        self.indices.put_alias(index=index_name, name=alias, body=body)

    def remove_alias(self, index_name, alias):
        logger.info("Removing alias {alias} from index {index}".format(alias=alias, index=index_name))
        self.indices.delete_alias(index=index_name, name=alias)

    def configure_aliases(self, indexes, alias, write_index=None):
        actions = []
        for index in indexes:
            add = {"index": index, "alias": alias}
            if write_index == index:
                add["is_write_index"] = True
            actions.append({"add": add})
        self.indices.update_aliases({"actions": actions})

    def get_index_names(self, alias):
        return list(self.indices.get(index=alias).keys())

    def get_index_name(self, alias):
        try:
            return first(self.get_index_names(alias))
        except:
            return None

    def switch_alias(self, index_name, alias):
        if self.indices.exists(alias):
            existing_name = self.get_index_name(alias)
            self.remove_alias(existing_name, alias)
        self.add_alias(index_name, alias)

    def create_basic(self, alias, force, delete_old, manage_aliasing=True, shards=1, replicas=1):
        if force or not self.exists(alias):
            old_index_name = self.get_index_name(alias)
            new_index_name = self.get_next_name(alias)

            self.create_index(new_index_name, alias, shards=shards, replicas=replicas)

            if manage_aliasing:
                self.switch_alias(new_index_name, alias)

            if delete_old:
                self.delete_index(old_index_name)
        else:
            logger.info("{alias} already exists".format(alias=alias))

    def update_index(self, index_name, mappings_key, property_path):
        logger.info("updating mapping on index {index_name}, with mappings for key {mappings_key}".format(
            index_name=index_name,
            mappings_key=mappings_key
        ))

        mappings = get(MAPPINGS, mappings_key)
        property_mapping = get_in(mappings, ['properties', property_path])
        mapping_update = {
            "properties": {property_path: property_mapping}
        }
        self.indices.put_mapping(mapping_update, index=index_name)
