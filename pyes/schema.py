import inspect
import numbers
import re
import types
from datetime import datetime

from pyfunk.pyfunk import get, count, first, apply, empty
from pyes.utils import enumerate_class, EMAIL_REGEX


class SchemaError(Exception):
    pass


def trim_value(value):
    if value and len(str(value)) > 256:
        return str(value)[:256]
    return value


def validate_dictionary(schema, d):
    validate(dictionary, d)
    for k, s in schema.items():
        if string(k):
            validate(s, get(d, k))
        else:
            for dk, dv in d.items():
                if dk not in schema.keys():
                    try:
                        validate(k, dk)
                        key_match = True
                    except SchemaError:
                        key_match = False
                    if key_match:
                        validate(s, dv)


def validate_list(schema, l):
    validate(sequence, l)
    if count(schema) == 1:
        for x in l:
            validate(first(schema), x)
    else:
        for s, i in enumerate(schema):
            try:
                li = l[i]
                validate(s, li)
            except:
                raise SchemaError("Schema length is {0} but, provided value is of length {1}".format(
                    count(schema),
                    count(l)
                ))


def validate(schema, v):
    if isinstance(schema, Validator):
        schema.validate(v)
    elif dictionary(schema):
        validate_dictionary(schema, v)
    elif sequence(schema):
        validate_list(schema, v)
    else:
        if not schema(v):
            raise SchemaError("Value '{0}' does not match schema".format(trim_value(v)))
    return True


def s_and(*args):
    def pred(x):
        for arg in args:
            validate(arg, x)
        return True
    return pred


def s_or(*args):
    def pred(x):
        for arg in args:
            try:
                validate(arg, x)
                return True
            except SchemaError:
                pass
        raise SchemaError("Value '{0}' passed none of the s_or validations".format(x))
    return pred


def not_empty(x):
    return not empty(x)


def dictionary(x):
    return isinstance(x, dict)


def sequence(x):
    return isinstance(x, list)


def any_value(x):
    return True


def boolean(x):
    return isinstance(x, bool)


def method(x):
    return isinstance(x, types.FunctionType)


def string(x):
    return isinstance(x, str)


def non_empty_string(x):
    return string(x) and not empty(x)


def empty_string(x):
    return x == ''


def sized_string(gte=None, lte=None):
    def wrapped(x):
        string_length = count(x)
        return string(x) and (not gte or string_length >= gte) and (not lte or string_length <= lte)
    return wrapped


def number(x):
    return isinstance(x, numbers.Number)


def integer(x):
    return isinstance(x, int)


def positive(x):
    return x >= 0


def negative(x):
    return x <= 0


def strict_positive(x):
    return x > 0


def strict_negative(x):
    return x < 0


def s_datetime(x):
    return isinstance(x, datetime)


positive_number = s_and(number, positive)
strict_positive_number = s_and(number, strict_positive)
negative_number = s_and(number, negative)
strict_negative_number = s_and(number, strict_negative)

positive_integer = s_and(integer, positive)
strict_positive_integer = s_and(integer, strict_positive)
negative_integer = s_and(integer, negative)
strict_negative_integer = s_and(integer, strict_negative)


def at_least(n):
    def pred(x):
        return x >= n
    return pred


def at_most(n):
    def pred(x):
        return x <= n
    return pred


percentage = s_and(positive_number, at_least(0), at_most(100))


def function(x):
    return callable(x)


def double(x):
    try:
        float(x)
        return True
    except:
        return False


def enum(*args):
    def enum_pred(x):
        return x in args
    return enum_pred


def class_enum(class_name):
    return apply(enum, enumerate_class(class_name).values())


def nillable(pred):
    def nillable_pred(x):
        return x is None or validate(pred, x)
    return nillable_pred


def type_of(class_name):
    def pred(x):
        return isinstance(x, class_name)
    return pred


number_or_nil = nillable(number)
string_or_nil = nillable(string)
boolean_or_nil = nillable(boolean)
list_or_nil = nillable(sequence)


def matches_regex(regex):
    def pred(x):
        return re.search(regex, x)
    return pred


s_email = matches_regex(EMAIL_REGEX)


class Validator:
    def validate(self, v):
        return v


class Keys(Validator):
    def __init__(self, required=None, optional=None, forbidden=None):
        self.required = required
        self.optional = optional
        self.forbidden = forbidden

    def validate(self, d):
        if self.required:
            self.required.validate(d)
        if self.optional:
            self.optional.validate(d)
        if self.forbidden:
            self.forbidden.validate(d)
        return d


class OptionalKeys(Validator):
    def __init__(self, **kwargs):
        self.keys = kwargs

    def validate(self, d):
        for k, value in d.items():
            pred = get(self.keys, k)

            try:
                is_valid = pred and validate(pred, value)
            except SchemaError:
                raise SchemaError("Value: '{0}' does not match a schema for key '{1}'".format(trim_value(value), k))

            if pred and not is_valid:
                raise SchemaError("Value: '{0}' for key '{1}', does not conform to schema".format(trim_value(value), k))
        return d


class RequiredKeys(Validator):
    def __init__(self, **kwargs):
        self.keys = kwargs

    def validate(self, d):
        for k, pred in self.keys.items():
            if k not in d.keys():
                raise SchemaError("Missing key: {0}".format(k))

            value = get(d, k)

            try:
                is_valid = pred and validate(pred, value)
            except SchemaError:
                raise SchemaError("Value: '{0}' does not match a schema for key '{1}'".format(trim_value(value), k))

            if not is_valid:
                raise SchemaError("Value: '{0}' for key '{1}', does not conform to schema".format(trim_value(value), k))
        return d


class ForbiddenKeys(Validator):
    def __init__(self, *args):
        self.keys = args

    def validate(self, d):
        for k in self.keys:
            if k in d.keys():
                raise SchemaError("Disallowed key: {0}".format(k))
        return d


def checkargs(function):
    def _f(*arguments, **kwargs):
        sig = inspect.signature(function)
        args = sig.parameters.keys()
        defaults_keymap = {k: v.default for k,v in sig.parameters.items() if not v.default is v.empty}
        for index, argument in enumerate(args):
            if index < len(arguments):
                value = arguments[index]
            elif argument in kwargs:
                value = kwargs[argument]
            else:
                value = get(defaults_keymap, argument)
            schema = get(function.__annotations__, argument)
            if schema is not None:
                validate(schema, value)
        return function(*arguments, **kwargs)
    _f.__doc__ = function.__doc__
    _f.__name__ = function.__name__
    _f.__module__ = function.__module__
    return _f

