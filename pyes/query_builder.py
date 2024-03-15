from typing import List, Dict, Optional

from pyfunk.pyfunk import assoc_in, first, merge, apply, count, mapl, find, filter_falsey_values, last


class DistanceUnits:
    MILES = "mi"
    YARDS = "yd"
    FEET = "ft"
    INCHES = "in"
    KILOMETERS = "km"
    METERS = "m"
    CENTIMETERS = "cm"
    MILLIMETERS = "mm"
    NAUTICAL_MILE = "nmi"


class ScriptSortType:
    number = "number"
    string = "string"


class BoolType:
    AND = "and"
    OR = "or"


class SortDirection:
    ASC = 'asc'
    DESC = 'desc'


class ScoreMode:
    MULTIPLY = 'multiply'
    SUM = 'sum'
    AVG = 'avg'
    FIRST = 'first'
    MAX = 'max'
    MIN = 'min'


############################################################
# Builders
############################################################


class Bools:
    def __init__(self, *args):
        self.bools = args

    def build(self):
        built = mapl(lambda bool: bool.build(), self.bools)
        cleaned = find(filter_falsey_values, built)
        bool = apply(merge, cleaned)
        if bool:
            return {
                'bool': apply(merge, cleaned)
            }


class QueryNode:
    def __init__(self, fields=None):
        self.children = []
        if fields:
            for field, value in fields.items():
                self.term(field, value)

    def bool(self, *args):
        self.children.append(Bools(*args))
        return self

    def match_all(self):
        self.children.append({
            "match_all": {}
        })
        return self

    def match(self, field, query, operator=None, fuzzy_transpositions=None):
        query = {
            'query': query
        }
        if operator:
            query.update({
                'operator': operator,
            })
        if fuzzy_transpositions is not None:
            query.update({
                'fuzzy_transpositions': fuzzy_transpositions,
            })
        self.children.append({
            'match': {
                field: query
            }
        })
        return self

    def term(self, field, value):
        self.children.append({
            'term': {
                field: value
            }
        })
        return self

    def terms(self, field, value, boost=None):
        terms = {
            'terms': {
                field: value,
            }
        }

        if boost is not None:
            terms['terms']['boost'] = boost

        self.children.append(terms)
        return self

    def constant_score(self, query, boost=None):
        constant_score = {
            'constant_score': {
                'filter': query.build()
            }
        }

        if boost is not None:
            constant_score['constant_score']['boost'] = boost

        self.children.append(constant_score)
        return self

    def exists(self, field):
        self.children.append({
            'exists': {
                'field': field
            }
        })
        return self

    def range(self, field, gte=None, lte=None, gt=None, lt=None):
        range_clause = {
            "range": {
                field: {}
            }
        }

        if gte is not None:
            range_clause = assoc_in(range_clause, ['range', field, 'gte'], gte)

        if lte is not None:
            range_clause = assoc_in(range_clause, ['range', field, 'lte'], lte)

        if gt is not None:
            range_clause = assoc_in(range_clause, ['range', field, 'gt'], gt)

        if lt is not None:
            range_clause = assoc_in(range_clause, ['range', field, 'lt'], lt)

        self.children.append(range_clause)
        return self

    def wildcard(self, field, value):
        self.children.append({
            "wildcard": {
                field: {
                    "value": "*{0}*".format(value)
                }
            }
        })
        return self

    def within(self, field, distance, lat, lon, units=DistanceUnits.MILES):
        self.children.append({
            "geo_distance": {
                field: {
                    "lat": lat,
                    "lon": lon
                },
                "distance": "{0}{1}".format(distance, units)
            }
        })
        return self

    def has_child(self, child, clause, count):
        self.children.append({
            "has_child": {
                "query": clause,
                "inner_hits": {
                    "size": count
                },
                "type": child
            }
        })
        return self

    def function_score(self, query, field, modifier="none"):
        self.children.append({
            'function_score': {
                'field_value_factor': {
                    'field': field,
                    'modifier': modifier,
                    'missing': 0
                },
                'query': query.build()
            }
        })
        return self

    def function_score_with_custom_function(self, query, score_filters, boost=None, score_mode=None):
        child = {
            'function_score': {
                'functions': score_filters,
                'query': query.build()
            }
        }
        if boost is not None:
            child = assoc_in(child, ['function_score', 'boost'], boost)

        if score_mode is not None:
            child = assoc_in(child, ['function_score', 'score_mode'], score_mode)

        self.children.append(child)
        return self

    def nested_query(self, field, query, boost=None, score_mode=None):
        child = {
            "nested": {
                "path": field,
                "query": query.build()
            }
        }
        if boost is not None:
            child = assoc_in(child, ['nested', 'boost'], boost)

        if score_mode is not None:
            child = assoc_in(child, ['nested', 'score_mode'], score_mode)

        self.children.append(child)
        return self

    def has_children(self, parent_id):
        self.children.append({
            "parent_id": {
                "id": parent_id
            }
        })
        return self

    def match_phrase_prefix(self, field, value):
        self.children.append({
            "match_phrase_prefix": {
                field: value
            }
        })
        return self

    def match_phrase(self, field: str, value: str, name: str = None) -> "QueryNode":
        node = {
            field: {
                "query": value,
            }
        }

        if name:
            node = assoc_in(node, [field, "_name"], name)

        self.children.append({
            "match_phrase": node
        })
        return self

    def prefix(self, field, value):
        self.children.append({
            "prefix": {
                field: value
            }
        })
        return self

    def more_like_this(self, fields, like, min_term_freq=1, max_query_terms=48, stop_words=None):
        node = {
            "more_like_this": {
                "fields": fields,
                "like": like,
                "min_term_freq": min_term_freq,
                "max_query_terms": max_query_terms
            }
        }

        if stop_words:
            node["more_like_this"].update({
                "stop_words": stop_words
            })

        self.children.append(node)
        return self

    def query_string(self, query: str):
        self.children.append({
            'query_string': {
                'query': query
            }
        })
        return self

    def build_children(self):
        built = []
        for child in self.children:
            if isinstance(child, Bools):
                built_child = child.build()
                if built_child is not None:
                    built.append(built_child)
            else:
                built.append(child)
        return built

    def is_empty(self):
        return count(self.children) == 0

    def derive(self, query_node):
        self.children = query_node.children
        return self

    def query(self, query):
        self.children.append(query.build())
        return self


class Should(QueryNode):
    def build(self):
        return {
            'should': self.build_children()
        }


class Must(QueryNode):
    def build(self):
        return {
            'must': self.build_children()
        }


class MustNot(QueryNode):
    def build(self):
        return {
            'must_not': self.build_children()
        }


class Filter(QueryNode):
    def build(self):
        return {
            'filter': self.build_children()
        }


class Query(QueryNode):
    def __init__(self, fields=None):
        super().__init__()
        if fields:
            if count(fields) == 1:
                for field, value in fields.items():
                    self.term(field, value)
            else:
                must = Must()
                for field, value in fields.items():
                    must.term(field, value)
                self.bool(must)

    def build(self):
        return first(self.build_children())


class Aggs:
    def __init__(self, key):
        self.key = key
        self.clause = None
        self.sub_aggs = None

    def terms(self, field, size=None, sort=None, sort_direction=SortDirection.ASC, include=None):
        terms = {
            'field': field
        }

        if size:
            terms.update({
                'size': size
            })

        if sort and sort_direction:
            terms.update({
                'order': {
                    sort: sort_direction
                }
            })

        if include is not None:
            terms.update({
                'include': include
            })

        self.clause = {
            'terms': terms
        }

        return self

    def scripted_terms(self,
                       script: str,
                       size: int = None,
                       sort: str = '_term',
                       sort_direction: str = SortDirection.ASC):
        terms = {
            'script': script
        }

        if size:
            terms.update({
                'size': size
            })

        if sort and sort_direction:
            terms.update({
                'order': {
                    sort: sort_direction
                }
            })

        self.clause = {
            'terms': terms
        }

        return self

    def range(self, field: str, ranges: List[Dict]):
        terms = {
            'field': field,
            'ranges': ranges,
        }

        self.clause = {
            'range': terms
        }

        return self

    def average(self, field):
        self.clause = {
            'avg': {
                'field': field
            }
        }
        return self

    def minimum(self, field):
        self.clause = {
            'min': {
                'field': field
            }
        }
        return self

    def maximum(self, field):
        self.clause = {
            'max': {
                'field': field
            }
        }
        return self

    def scripted_average(self, script):
        self.clause = {
            'avg': {
                'script': {
                    'inline': script,
                    'lang': 'painless'
                }
            }
        }
        return self

    def scripted_sum(self, script: str, params: Optional[Dict] = None):
        script = {
            'lang': 'painless',
            'inline': script,
        }

        if params:
            script['params'] = params

        self.clause = {
            'sum': {'script': script},
        }
        return self

    def sum(self, field):
        self.clause = {
            'sum': {
                'field': field
            }
        }
        return self

    def date_histogram(self, field, calendar_interval, fmt='yyyy-MM-dd 00:00:00', extended_bounds=None, missing=None):
        date_histogram = {
            'field': field,
            'interval': calendar_interval,
        }
        if fmt is not None:
            date_histogram['format'] = fmt
        if missing is not None:
            date_histogram['missing'] = missing

        if extended_bounds:
            date_histogram['extended_bounds'] = {
                'min': first(extended_bounds),
                'max': last(extended_bounds)
            }
        self.clause = {
            'date_histogram': date_histogram
        }
        return self

    def cardinality(self, field):
        self.clause = {
            'cardinality': {
                'field': field
            }
        }
        return self

    def value_count(self, field):
        self.clause = {
            'value_count': {
                'field': field
            }
        }
        return self

    def significant_terms(self, field, size=10, additional_config=None):
        self.clause = {
            'significant_terms': {
                'field': field,
                'size': size,
            }
        }
        if additional_config:
            self.clause['significant_terms'].update(additional_config)

        return self

    def percentile(self, field: str, percents: List[int] = None):
        if not percents:
            percents = [25, 50, 75, 90, 95, 99]

        self.clause = {
            'percentiles': {
                'field': field,
                'percents': percents,
            }
        }
        return self

    def sub(self, *aggs):
        self.sub_aggs = aggs
        return self

    def reverse_nested(self):
        self.clause = {
            'reverse_nested': {}
        }
        return self

    def build(self):
        clause = self.clause

        # Support multiple sub_aggs
        if self.sub_aggs and type(self.sub_aggs) in [list, tuple]:
            clause['aggs'] = {}
            for sub_agg in self.sub_aggs:
                clause['aggs'].update(sub_agg.build())
        elif self.sub_aggs:
            clause['aggs'] = self.sub_aggs.build()

        return {
            self.key: clause
        }


class FilteredAggs:
    def __init__(self, key):
        self.key = key
        self.filter_term = None
        self.aggs_term = None

    def filter(self, query):
        self.filter_term = query
        return self

    def aggs(self, *aggs_term):
        self.aggs_term = aggs_term
        return self

    def build(self):
        query = {}

        if self.filter_term:
            query.update({
                'filter': self.filter_term.build()
            })

        if self.aggs_term and type(self.aggs_term) in [list, tuple]:
            aggs = {}
            for agg in self.aggs_term:
                aggs = merge(aggs, agg.build())
            query.update({
                'aggs': aggs
            })
        elif self.aggs_term:
            query.update({
                'aggs': self.aggs_term.build()
            })

        return {
            self.key: query
        }


class NestedAggs:
    def __init__(self, key):
        self.key = key
        self.nested_path = None
        self.aggs_term = None

    def path(self, path):
        self.nested_path = path
        return self

    def aggs(self, *aggs_term):
        self.aggs_term = aggs_term
        return self

    def build(self):
        query = {
            'nested': {
                'path': self.nested_path
            },
        }

        if self.aggs_term and type(self.aggs_term) in [list, tuple]:
            aggs = {}
            for agg in self.aggs_term:
                aggs = merge(aggs, agg.build())
            query.update({
                'aggs': aggs,
            })
        elif self.aggs_term:
            query.update({
                'aggs': self.aggs_term.build(),
            })

        return {
            self.key: query,
        }


class Slice:
    def __init__(self, slice_id, slices):
        self.slice_id = slice_id
        self.slices = slices

    def build(self):
        return {
            "id": self.slice_id,
            "max": self.slices
        }


class Body:
    def __init__(self, fields=None):
        if fields:
            self.query_term = Query(fields)
        else:
            self.query_term = None
        self.suggest_term = None
        self.aggs_terms = []
        self.limit = None
        self.start_value = None
        self.sort_clauses = []
        self.source_fields = None
        self.slice_term = None
        self.track_total_hits_value = None

    def query(self, query_term):
        self.query_term = query_term
        return self

    def slice(self, slice_term):
        self.slice_term = slice_term
        return self

    def suggest(self, key, field, prefix, contexts=None):
        self.suggest_term = {
            key: {
                "prefix": prefix,
                "completion": {
                    "field": field
                }
            }
        }

        if contexts:
            self.suggest_term[key]["completion"].update({
                "contexts": contexts
            })

        return self

    def aggs(self, aggs_term):
        self.aggs_terms.append(aggs_term)
        return self

    def size(self, limit):
        self.limit = limit
        return self

    def script_sort(self, script, sort_type=ScriptSortType.number, sort_direction=SortDirection.ASC):
        self.sort_clauses.append({
            "_script": {
                "type": sort_type,
                "script": script,
                "order": sort_direction
            }
        })
        return self

    def sort(self, field, order=None, missing=None, mode=None):
        if order or mode:
            if order:
                order_field = {'order': order}
            else:
                order_field = {}

            if missing:
                order_field.update({
                    'missing': missing
                })

            if mode:
                order_field.update({
                    'mode': mode
                })

            self.sort_clauses.append({
                field: order_field
            })
        else:
            self.sort_clauses.append(field)
        return self

    def start(self, start_value):
        self.start_value = start_value
        return self

    def proximity_sort(self, field, lat, lon, units=DistanceUnits.MILES, sort_dir=SortDirection.ASC):
        self.sort_clauses.append({
            '_geo_distance': {
                field: {
                    'lat': str(lat),
                    'lon': str(lon)
                },
                'order': sort_dir,
                'unit': units,
                'distance_type': 'plane'
            }
        })
        return self

    def source(self, fields):
        self.source_fields = fields
        return self

    def track_total_hits(self, value):
        self.track_total_hits_value = value
        return self

    def build(self):
        body = {}
        if self.query_term is not None:
            body['query'] = self.query_term.build()

        if self.slice_term is not None:
            body['slice'] = self.slice_term.build()

        if self.suggest_term is not None:
            body['suggest'] = self.suggest_term

        if self.aggs_terms:
            aggs = {}
            for aggs_term in self.aggs_terms:
                aggs = merge(aggs, aggs_term.build())
            body['aggs'] = aggs

        if self.limit is not None:
            body['size'] = self.limit

        if self.start_value is not None:
            body['from'] = self.start_value

        if self.sort_clauses:
            body['sort'] = self.sort_clauses

        if self.source_fields:
            body['_source'] = self.source_fields

        if self.track_total_hits_value is not None:
            body['track_total_hits'] = self.track_total_hits_value

        return body


class Reindex:
    def __init__(self, proceed_conflicts=True):
        self.source_dict = {}
        self.dest_dict = {}
        self.proceed_conflicts = proceed_conflicts

    def source(self, index, query=None):
        self.source_dict['index'] = index
        if query:
            self.source_dict['query'] = query.build()
        return self

    def dest(self, index):
        self.dest_dict['index'] = index
        return self

    def build(self):
        body = {
            "source": self.source_dict,
            "dest": self.dest_dict,
        }
        if self.proceed_conflicts:
            body["conflicts"] = "proceed"
        return body
