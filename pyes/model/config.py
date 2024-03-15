from pyfunk.pyfunk import filter_none_values, get, merge

from pyes.schema import checkargs, string


MAPPINGS = {}
SETTINGS = {}

@checkargs
def get_config(key: string, shards=1, replicas=1):
    return filter_none_values({
        "mappings": get(MAPPINGS, key),
        "settings": merge(
            get(SETTINGS, key),
            {
                "index": {
                    "number_of_shards": shards,
                    "number_of_replicas": replicas
                }
            }
        )
    })
