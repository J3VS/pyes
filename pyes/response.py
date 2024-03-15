from functools import partial

from pyfunk.pyfunk import get, get_in, first, comp, update_in, assoc, mapl, map_map, map_key


def get_id(hit):
    return get(hit, '_id')


def get_type(hit):
    return get(hit, '_type')


def get_index(hit):
    return get(hit, '_index')


def get_source(hit):
    return get(hit, '_source')


def get_parent(hit):
    return get(hit, '_parent')


def get_hits(response):
    return get_in(response, ['hits', 'hits'], [])


def get_source_by_id(hits):
    return map_map(get_id, get_source, hits)


def get_aggs(response):
    return get(response, "aggregations", {})


def get_total(response):
    return get_in(response, ['hits', 'total', 'value'], 0)


def include_id(hit):
    source = assoc(get_source(hit), 'uid', get_id(hit))
    return assoc(hit, '_source', source)


def include_ids(response):
    return update_in(response, ['hits', 'hits'], partial(mapl, include_id))


def get_ids(hits):
    return mapl(get_id, hits)


ids_from_response = comp(get_ids, get_hits)


def get_sources(hits):
    return mapl(get_source, hits)


sources_from_response = comp(get_sources, get_hits)


def get_sources_by_id(hits):
    return map_map(get_id, get_source, hits)


sources_by_id_from_response = comp(get_sources_by_id, get_hits)

get_single_source = comp(get_source, first)


def get_suggest_options_fn(key):
    def get_suggest_options(response):
        return get_sources(get(first(get_in(response, ['suggest', key])), 'options'))

    return get_suggest_options


def get_keys_from_bucket(response, aggregation_name):
    return map_key('key', get_in(response, ['aggregations', aggregation_name, 'buckets'], default=[]))


def get_keys_from_nested_bucket(response, nested_aggregation_names):
    path = ['aggregations'] + nested_aggregation_names + ['buckets']
    return map_key('key', get_in(response, path, default=[]))


DEFAULT_PAGE_SIZE = 10


def paginate_response(result, data_key='data', item_transformer=None):
    return {
        'total': get_total(result),
        data_key: [
            item_transformer(get_source(r)) if item_transformer else get_source(r)
            for r in get_hits(result)],
    }
