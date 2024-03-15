import pytest
from elasticsearch import Elasticsearch

from pyes.store import MegaStore
from pyes.test.services import TestServices


@pytest.yield_fixture(scope='session', autouse=True)
def test_store():
    es = Elasticsearch("localhost")
    yield MegaStore(es)


@pytest.yield_fixture(scope='session', autouse=True)
def test_services():
    es = Elasticsearch("localhost")
    yield TestServices(es)
