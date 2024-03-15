from uuid import uuid4
import collections
import logging
from elasticsearch.helpers import scan

from pyes.query_builder import Query, Body
from pyfunk.pyfunk import merge

logger = logging.getLogger(__name__)

EMAIL_REGEX = '^[\\w\\.!#$%&\'*+-/=?^_`{|}~]+\\@[\\w\\-]+[\\.]+[\\w\\-]+[\\w\\-\\.]+'


def enumerate_class(class_name):
    return {k: v for k, v in vars(class_name).items()
            if not callable(getattr(class_name, k)) and not k.startswith("__")}


def uuid():
    return str(uuid4())


class ChildByParentScroller:
    def __init__(self, es, index, parent_ids, batch, opts=None):
        self.es = es
        self.index = index

        if isinstance(parent_ids, collections.Iterable):
            self.parent_ids = iter(parent_ids)
        else:
            self.parent_ids = parent_ids

        self.batch = batch
        self.opts = opts
        self.parent_id = None
        self.es_scroller = None

        self.next_parent()

    def __iter__(self):
        return self

    def next_parent(self):
        self.parent_id = self.next_parent_id()
        self.es_scroller = self.children_scroller()

    def next_parent_id(self):
        return self.parent_ids.__next__()

    def children_scroller(self):
        query = Query().has_children(self.parent_id)
        body = merge(Body().query(query), self.opts)
        return scan(self.es,
                    query=body,
                    index=self.index,
                    size=self.batch)

    def __next__(self):
        result = None
        while result is None:
            try:
                result = self.es_scroller.__next__()
            except StopIteration:
                result = None

            if result is None:
                self.next_parent()

        return result
