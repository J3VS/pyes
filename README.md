## PyES

Elasticsearch tools in python

### Example

```py
from pyes.crud import ESCrudService
from pyes.store import MegaStore
from elasticsearch import Elasticsearch

class MyCrudService(ESCrudService):
    def __init__(self, store):
        super().__init__(store, 'myindex')


elasticsearch = Elasticsearch('localhost:9200')
store = MegaStore(elasticsearch)
my_service = MyCrudService(store)

doc_id = my_service.create({'id': 1, 'hello': 'world'})

doc = my_service.get_entity(doc_id)

my_service.update(doc_id, {'hello': 'universe'})

my_service.delete(doc_id)
```