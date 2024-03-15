from pyes.query_builder import Body, Query


class NotExistsException(Exception):
    pass


def exists(store, id, index, type, batch=False):
    if not store.get(id, index, type):
        if not batch:
            raise NotExistsException("Entity does not exist")
        else:
            return False
    return True


class NotUniqueException(Exception):
    pass


def unique(store, index, type, partial, batch=False):
    body = Body().query(Query(partial))

    if store.query(index, type, body):
        if not batch:
            raise NotUniqueException("Entity is not unique")
        else:
            return False

    return True
