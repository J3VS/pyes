from functools import wraps

from elasticsearch import exceptions, Elasticsearch

from pyes.crud import ESCrudService
from pyes.model.config import get_config
from pyes.model.initialize import *
from pyes.store import MegaStore
from pyfunk.pyfunk import split, massoc

logger = logging.getLogger(__name__)

es = Elasticsearch()


def testify(index):
    return "test__{0}".format(index)


def detestify(alias):
    return last(split(alias, "__"))


def __create_new_index(indices, alias):
    __delete_test_index(alias, indices)

    index = detestify(alias)

    config = get_config(index)
    indices.create(alias, body=config)

def __delete_test_index(alias, indices=IndicesClient(es)):
    try:
        indices.delete(index=alias)
    except exceptions.NotFoundError:
        logger.info("No index to remove by alias: {0}".format(alias))


def create_test_index(indices=[]):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            indices_client = IndicesClient(es)

            try:
                for alias in indices:
                    __create_new_index(indices_client, alias)

                r = f(*args, **kwargs)
            finally:
                # Ensure we delete even if the tests errors out
                for alias in indices:
                    __delete_test_index(alias, indices_client)

            return r

        return wrapped

    return decorator


def with_test_index(name="test__index"):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            indices_client = IndicesClient(es)
            try:
                indices_client.create(index=name)
                r = f(*args, **kwargs)
            finally:
                # Ensure we delete even if the tests errors out
                indices_client.delete(index=name)
            return r

        return wrapped

    return decorator


def with_temp_indexes(indexes):
    def wrapper(f):
        def wrapped(*args, **kwargs):
            es = Elasticsearch()
            indices_client = IndicesClient(es)
            store = MegaStore(es)
            try:
                temp_indexes = {}
                for index_name, config in indexes.items():
                    indices_client.create(index_name, body=config)
                    temp_indexes[index_name] = ESCrudService(store, index_name)
                kwargs = massoc(kwargs, 'temp_indexes', temp_indexes)
                return f(*args, **kwargs)
            finally:
                for index_name, _ in indexes.items():
                    try:
                        indices_client.delete(index_name)
                    except:
                        pass
        return wrapped
    return wrapper


def ensure_deletion(*indexes):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            indices_client = IndicesClient(es)
            try:
                r = f(*args, **kwargs)
            finally:
                # Ensure we delete even if the tests errors out
                for name in indexes:
                    try:
                        indices_client.delete(index=name)
                    except:
                        pass
            return r

        return wrapped

    return decorator
