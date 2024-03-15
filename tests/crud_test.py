import pytest

from pyes.crud import ESCrudService
from pyes.validators import NotExistsException
from pyfunk.pyfunk import select_keys
from pyes.schema import SchemaError, boolean, string_or_nil, Keys, OptionalKeys, string, RequiredKeys

from pyes.test.indices import create_test_index
from pyes.test.fixtures import test_services

create_spec = Keys(required=RequiredKeys(thing_type=string))
update_spec = Keys(optional=OptionalKeys(thing_type=string))


class ThingService(ESCrudService):
    def __init__(self, es):
        super().__init__(es, "thing")

    def create(self,
               entity: create_spec,
               entity_id: string_or_nil = None,
               batch: boolean = False):
        super().create(entity, entity_id=entity_id, batch=batch)

    def update(self,
               entity_id: string,
               update: update_spec,
               batch: boolean = False,
               check_existence: boolean = True):
        super().update(entity_id, update, batch=batch, check_existence=check_existence)


class ThingType:
    COMMON = "common"
    UNIQUE = "unique"


@create_test_index(indices=["thing"])
def test_lifecycle(test_services):
    # Try and create an empty thing
    with pytest.raises(SchemaError):
        test_services.thing.create({})

    thing_service = ThingService(test_services.es)

    # Create a thing
    thing_id = thing_service.create({'thing_type': ThingType.COMMON})

    # Get that thing back
    created_thing = thing_service.get_entity(thing_id)

    # Check that thing
    assert select_keys(created_thing, ['uid', 'thing_type']) == {'uid': thing_id,
                                                                 'thing_type': ThingType.COMMON}

    # Update some fields badly on the thing
    with pytest.raises(SchemaError):
        thing_service.update(thing_id, {'wrong_field': 'oops'})

    # Update a field on the thing
    thing_service.update(thing_id, {'thing_type': ThingType.UNIQUE})

    # Get that thing back
    updated_thing = thing_service.get_entity(thing_id)

    # Check that thing
    assert select_keys(updated_thing, ['uid', 'thing_type']) == {'uid': thing_id,
                                                                 'thing_type': ThingType.UNIQUE}

    # Delete that thing
    thing_service.delete(thing_id)

    # Check the thing no longer exists
    with pytest.raises(NotExistsException):
        thing_service.exists(thing_id)
