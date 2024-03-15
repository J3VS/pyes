from elasticsearch import Elasticsearch

from pyes.multi_index_crud import MultiIndexESCrudService, ArchivingESCrudService
from pyes.model.initialize import IndexInitialization
from pyes.query_builder import Query, Body
from pyes.response import get_index, get_type, get_source, get_id
from pyes.test.indices import ensure_deletion, get, IndicesClient
from pyfunk.pyfunk import count, mapl, dissoc

from pyes.test.fixtures import test_services


ALIAS = 'thing'

ARCHIVED_INDEX = 'archived_thing_1'
LIVE_INDEX = 'live_thing_1'


class ThingType:
    INITIAL = 'initial'
    UPDATED = 'updated'


def get_index_from_i(i):
    if 0 < i <= 2500:
        return 'testing_1'
    elif 2500 < i <= 5000:
        return 'testing_2'
    elif 5000 < i <= 7500:
        return 'testing_3'
    elif 7500 < i <= 10000:
        return 'testing_4'


@ensure_deletion("testing_1", "testing_2", "testing_3", "testing_4")
def test_multi_index_write_store(test_services):
    es = Elasticsearch()
    indices = IndicesClient(es)

    indices.create(index="testing_1")
    indices.create(index="testing_2")
    indices.create(index="testing_3")
    indices.create(index="testing_4")
    indices.update_aliases({"actions": [
        {"add": {"index": "testing_1", "alias": "testing", "is_write_index": True}},
        {"add": {"index": "testing_2", "alias": "testing"}},
        {"add": {"index": "testing_3", "alias": "testing"}},
        {"add": {"index": "testing_4", "alias": "testing"}}
    ]})

    miescs = MultiIndexESCrudService(test_services.store, 'testing')

    for id in range(1, 10001, 1):
        index = get_index_from_i(id)
        test_services.store.create(str(id), index, {'hello': "world_{0}".format(id)}, batch=True)

    test_services.store.batch_write()

    miescs.refresh()

    assert mapl(lambda hit: {
        'id': get_id(hit),
        'index': get_index(hit),
        'type': get_type(hit),
        'doc': dissoc(get_source(hit), 'created_time')
    }, miescs.get_entity_hits(["2000", "4000", "6000", "8000"])) == [
        {'id': "2000", 'index': 'testing_1', 'type': '_doc', 'doc': {'hello': "world_2000"}},
        {'id': "4000", 'index': 'testing_2', 'type': '_doc', 'doc': {'hello': "world_4000"}},
        {'id': "6000", 'index': 'testing_3', 'type': '_doc', 'doc': {'hello': "world_6000"}},
        {'id': "8000", 'index': 'testing_4', 'type': '_doc', 'doc': {'hello': "world_8000"}}
    ]

    match_all = Query().match_all()
    assert miescs.count(match_all) == 10000
    assert test_services.store.count('testing_1', match_all) == 2500
    assert test_services.store.count('testing_2', match_all) == 2500
    assert test_services.store.count('testing_3', match_all) == 2500
    assert test_services.store.count('testing_4', match_all) == 2500

    miescs.update_all({
        "2000": {"goodbye": "universe_2000"},
        "4000": {"goodbye": "universe_4000"},
        "6000": {"goodbye": "universe_6000"},
        "8000": {"goodbye": "universe_8000"}
    })

    miescs.refresh()

    assert mapl(lambda hit: {
        'id': get_id(hit),
        'index': get_index(hit),
        'type': get_type(hit),
        'doc': dissoc(get_source(hit), 'created_time', 'update_time')
    }, miescs.get_entity_hits(["2000", "4000", "6000", "8000"])) == [
       {'id': "2000", 'index': 'testing_1', 'type': '_doc', 'doc': {'hello': "world_2000", 'goodbye': "universe_2000"}},
       {'id': "4000", 'index': 'testing_2', 'type': '_doc', 'doc': {'hello': "world_4000", 'goodbye': "universe_4000"}},
       {'id': "6000", 'index': 'testing_3', 'type': '_doc', 'doc': {'hello': "world_6000", 'goodbye': "universe_6000"}},
       {'id': "8000", 'index': 'testing_4', 'type': '_doc', 'doc': {'hello': "world_8000", 'goodbye': "universe_8000"}}
    ]

    miescs.delete_all(["2000", "4000", "6000", "8000"])
    miescs.refresh()

    assert miescs.get_entity_hits(["2000", "4000", "6000", "8000"]) == [None, None, None, None]

    miescs.create({'hello': "world_2000"}, entity_id="2000", batch=True)
    miescs.create({'hello': "world_4000"}, entity_id="4000", batch=True)
    miescs.create({'hello': "world_6000"}, entity_id="6000", batch=True)
    miescs.create({'hello': "world_8000"}, entity_id="8000", batch=True)
    miescs.batch_write()
    miescs.refresh()

    assert mapl(lambda hit: {
        'id': get_id(hit),
        'index': get_index(hit),
        'type': get_type(hit),
        'doc': dissoc(get_source(hit), 'created_time')
    }, miescs.get_entity_hits(["2000", "4000", "6000", "8000"])) == [
       {'id': "2000", 'index': 'testing_1', 'type': '_doc', 'doc': {'hello': "world_2000", 'uid': "2000"}},
       {'id': "4000", 'index': 'testing_1', 'type': '_doc', 'doc': {'hello': "world_4000", 'uid': "4000"}},
       {'id': "6000", 'index': 'testing_1', 'type': '_doc', 'doc': {'hello': "world_6000", 'uid': "6000"}},
       {'id': "8000", 'index': 'testing_1', 'type': '_doc', 'doc': {'hello': "world_8000", 'uid': "8000"}}
    ]


@ensure_deletion(ARCHIVED_INDEX, LIVE_INDEX)
def test_archiving_crud(test_services):
    ii = IndexInitialization(test_services.es)

    ii.create_index(ARCHIVED_INDEX, 'thing')
    ii.create_index(LIVE_INDEX, 'thing')

    ii.configure_aliases([ARCHIVED_INDEX, LIVE_INDEX], ALIAS, write_index=LIVE_INDEX)

    aescs = ArchivingESCrudService(test_services.store, 'thing')

    start_time = 1577836800000
    end_time = 1609459200000
    mid_time = int(start_time + (end_time - start_time)/2)
    for thing_id in range(1, 10001, 1):
        aescs.create({
            'thing_id': thing_id,
            'thing_type': ThingType.INITIAL,
            'thing_time': int(start_time + (thing_id * ((end_time - start_time) / 10000)))
        }, entity_id=str(thing_id), batch=True)

    aescs.batch_write()

    aescs.refresh()

    match_all = Body().query(Query().match_all()).size(10000)
    assert count(aescs.query(match_all)) == 10000

    query = Query().range('thing_time', lte=mid_time)

    aescs.archive(query)

    aescs.refresh()

    assert count(aescs.query(match_all)) == 10000
    assert count(test_services.store.query(ARCHIVED_INDEX, match_all)) == 5000
    assert count(test_services.store.query(LIVE_INDEX, match_all)) == 5000

    aescs.update("1", {'thing_type': ThingType.UPDATED})

    aescs.refresh()

    first_thing = aescs.get_entity("1")

    assert get(first_thing, 'thing_type') == ThingType.UPDATED
