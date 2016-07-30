
"""Model classes."""

from collections import OrderedDict
from copy import deepcopy
import itertools as it
from pathlib import Path

from jsonschema import Draft4Validator as Validator, FormatChecker
from jsonschema.exceptions import ValidationError
import pymongo

from .io import YamlManager
from .misc_utils import starfilter
from .text_utils import _text_from_sp


class InsertError(Exception):
    """Error raised when a database insert fails."""


def _compact(self):
    """Filter out boolean `False` values, returning a copy of the `BaseRecord`.

    >>> (_compact(BaseRecord(_raw_data={'a': {'b': {'c': 1}}, 'd': ''}))
    ...  .data == {'a': {'b': {'c': 1}}})
    True
    >>> (_compact(_unwrap(BaseRecord(_raw_data={'a': {'b': {'c': 1}},
    ...                                         'd': {'e': ''}})))
    ...  .data == {'a.b.c': 1})
    True
    """
    return self.__class__(
        _raw_data=self.data.__class__(starfilter(lambda _, v: v,
                                                 self.data.items())))


def _sort(data):
    """Traverse `data` to sort it and all sub-dicts alphabetically.

    >>> _sort({'c': 1, 'a': 4, 'b': [{'β': 2, 'α': 3}]})  # doctest: +NORMALIZE_WHITESPACE
    OrderedDict([('a', 4), ('b', [OrderedDict([('α', 3), ('β', 2)])]),
                 ('c', 1)])
    """
    def sort(value):
        if isinstance(value, dict):
            return OrderedDict(it.starmap(lambda k, v: (k, sort(v)),
                                          sorted(value.items())))
        elif isinstance(value, list):
            return list(map(sort, value))
        return value

    return sort(data)


def _unwrap(self):
    """Flatten a `BaseRecord` recursively, returning a copy of it.

    >>> (_unwrap(BaseRecord(_raw_data={'a': {'b': {'c': [1, 2], 'd': 3}},
    ...                                'e': {'f': ''}}))
    ...  .data == {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''})
    True
    """
    def rekey(value, pk=''):
        if isinstance(value, dict):
            for k in value:
                yield from rekey(value[k], '.'.join((pk, k)) if pk else k)
        else:
            yield pk, value

    return self.__class__(_raw_data=self.data.__class__(rekey(self.data)))


class _prepare_record(type):

    def __new__(cls, name, bases, cls_dict):
        cls = super().__new__(cls, name, bases, cls_dict)
        cls.template = _sort(getattr(cls, 'template', {}))
        return cls


class BaseRecord(metaclass=_prepare_record):
    """A base class for our records.

    Both class attributes, `template` and `required_properties`, are optional.
    The `template` is the default structure of the record.
    Initialising `BaseRecord` with properties not found in the `template`'s
    keys will result in a `ValueError`.  Initialising `BaseRecord` without
    any of the `required_properties` (or with any of them being falsy)
    will also result in an error.

    >>> class Base(BaseRecord):
    ...     template = {'a': None, 'b': None, 'c': None}
    ...     required_properties = {'a', 'b'}

    >>> Base(a=1, b=2)
    <Base: OrderedDict([('a', 1), ('b', 2), ('c', None)])>
    """

    def __init__(self, *, _raw_data={}, **kwargs):
        if _raw_data:
            self.data = deepcopy(_raw_data)
            return
        self.data = deepcopy(self.template)
        self.data.update(kwargs)

    def __repr__(self):
        return '<{}: {!r}>'.format(self.__class__.__name__, self.data)


class _prepare_insertable_record(_prepare_record):

    def __new__(cls, name, bases, cls_dict):
        if name == 'InsertableRecord':
            return super().__new__(cls, name, bases, cls_dict)

        schema = cls_dict['schema']
        if isinstance(schema, str):
            schema = YamlManager.load(str(Path(__file__).parent
                                          /'data'/'schemas'/(schema + '.yaml')))
        return super().__new__(cls, name, bases,
                               {**cls_dict,
                                'schema': schema,
                                'template': {**cls_dict['template'], '_id': None},
                                'validator': Validator(schema, format_checker=
                                                       FormatChecker(('email',)))})


class InsertableRecord(BaseRecord, metaclass=_prepare_insertable_record):
    """A record that can be inserted into the database.

    To interface with the database, `InsertableRecord`s must define a
    `collection` name.

    Set up the testing environment.

        >>> from uuid import uuid4
        >>> from . import Db \

        >>> test_db_name = uuid4().hex
        >>> test_db = Db.get('mongodb://localhost:27017/' + test_db_name)

    Test basic operation.

        >>> class Insertable(InsertableRecord):
        ...     collection = test_db.test
        ...     template = {'some_field': 'some_data'}
        ...     schema = {'type': 'object',
        ...               'properties': {'some_field': {'type': 'string'}}} \

        ...     def generate__id(self):
        ...         return 'insertable_test' \

        ...     def generate_inserts(self, merge):
        ...         data = yield
        ...         yield {'$set': data}

        >>> foo = Insertable()
        >>> foo.insert()
        OrderedDict([('_id', 'insertable_test'), ('some_field', 'some_data')])
        >>> foo.exists
        True
        >>> foo.data['some_field'] = 'some_other_data'
        >>> foo.insert()     # doctest: +NORMALIZE_WHITESPACE
        OrderedDict([('_id', 'insertable_test'),
                     ('some_field', 'some_other_data')])
        >>> foo.collection.count()
        1
        >>> foo.delete()     # doctest: +NORMALIZE_WHITESPACE
        OrderedDict([('_id', 'insertable_test'),
                     ('some_field', 'some_other_data')])
        >>> foo.exists
        False
        >>> foo.data['some_field'] = 1
        >>> foo.insert()     # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
        ...
        Traceback (most recent call last):
        ...
        jsonschema.exceptions.ValidationError: 1 is not of type 'string'
        ...
        >>> del foo.data['_id']
        >>> foo.insert()   # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        ValueError: No `_id` provided in <Insertable: OrderedDict([('some_field', 1)])>

    Test that, absent of an existing document in the database,
    `insert(merge=True)` will raise `InsertError`.

        >>> bar = Insertable()
        >>> bar.insert(merge=True)    # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        scrapers.records.InsertError: Unable to insert or merge
        <Insertable: OrderedDict([('_id', 'insertable_test'),
                                  ('some_field', 'some_data')])>
        with operation {'$set': OrderedDict([('some_field', 'some_data')])}

    Tear it all down.

        >>> test_db.command('dropDatabase') == {'dropped': test_db_name, 'ok': 1.0}
        True
    """

    def __init__(self, *, _raw_data={}, **kwargs):
        super().__init__(_raw_data=_raw_data, **kwargs)
        if not _raw_data and not self.data['_id']:
            self.data['_id'] = self.generate__id()

    @property
    def _id(self):
        """The record's primary key."""
        return self.data.get('_id')

    def delete(self, **kwargs):
        """Delete the document from the database."""
        return self.collection.find_one_and_delete(filter={'_id': self._id},
                                                   **kwargs)

    @property
    def exists(self):
        """
        Check to see whether a record with the same `self._id` exists in
        the database.
        """
        return bool(self.collection.find_one(self._id))

    def generate__id(self):
        """Override to create an `_id`."""
        raise NotImplementedError

    def generate_inserts(self, merge):
        """Override to construct database inserts.

        `generate_inserts` is a coroutine called from within `self.insert`.
        A boolean `merge` flag indicates whether the data has been squashed
        in preparation for a merge operation.  Where `merge` is `False`,
        self.data == (initial) data.

        Refer to <https://docs.mongodb.org/manual/reference/operator/update/>
        for the mongodb update syntax.  The simplest use case is:

            def generate_inserts(self, merge):
                data = yield            # Grab the data;
                yield {'$set': data}    # and yield an update
        """
        raise NotImplementedError

    def insert(self, merge=False):
        """Insert a record into the database.

        `insert` returns the resultant document on success and raises
        `InsertError` on failure.  If `merge` is `True`, the record will not
        be inserted unless it already exists in the database.
        """
        if not self._id:
            raise ValueError('No `_id` provided in ' + repr(self))
        new = not merge
        orig_data = self.collection.find_one(self._id)
        if new:
            self.delete()
            data = deepcopy(self.data)
        else:
            data = _compact(_unwrap(self)).data

        inserts = self.generate_inserts(merge)
        for _ in inserts:
            data.pop('_id')
            insert = inserts.send(data)
            data = self.update(insert, upsert=new)
            if not data:
                raise InsertError('Unable to insert or merge ' + repr(self) +
                                  ' with operation ' + repr(insert))
        try:
            self.validator.validate(data)
        except:
            # Roll back the changes if validation has failed
            self.delete()
            if orig_data:
                self.update({'$set': orig_data}, upsert=True)
            raise
        self.data = data
        return data

    def replace(self, data=None, **kwargs):
        """A low-level `replace` that bypasses the generated inserts."""
        return self.collection\
            .find_one_and_replace(filter={'_id': self._id},
                                  replacement=data or self.data,
                                  return_document=pymongo.ReturnDocument.AFTER,
                                  **kwargs)

    def update(self, update, **kwargs):
        """A low-level `update` that bypasses the generated inserts."""
        return self.collection\
            .find_one_and_update(filter={'_id': self._id},
                                 update=update,
                                 return_document=pymongo.ReturnDocument.AFTER,
                                 **kwargs)

    @classmethod
    def export(cls, format):
        if format == 'json':
            return _text_from_sp(
                ('mongoexport', '--jsonArray',
                                '--db=' + cls.collection.database.name,
                                '--collection=' + cls.collection.name))
        elif format == 'csv':
            return _text_from_sp(
                ('mongoexport', '--type=' + format,
                                '--db=' + cls.collection.database.name,
                                '--collection=' + cls.collection.name,
                                '--fields=' + ','.join(sorted(cls.template))))
        raise ValueError('Invalid format ' + repr(format))

    @classmethod
    def validate(cls):
        for i in cls.collection.find():
            try:
                cls.validator.validate(i)
            except ValidationError as e:
                raise ValueError('Unable to validate ' + repr(i)) from e

    InsertError = InsertError


class _prepare_sub_record(_prepare_record):

    def __new__(cls, name, bases, cls_dict):
        cls = super().__new__(cls, name, bases, cls_dict)
        cls._construct = type(cls.__name__, (BaseRecord,), dict(cls.__dict__))
        return cls


class SubRecord(metaclass=_prepare_sub_record):
    """A record contained within another record.

    A SubRecord returns its `self.data` on initialisation.

    >>> class Sub(SubRecord):
    ...     template = {'a': None, 'b': None, 'c': None}

    >>> Sub(a=1, b=2)
    OrderedDict([('a', 1), ('b', 2), ('c', None)])
    """

    def __new__(cls, *, _raw_data={}, **kwargs):
        return _sort(cls._construct(_raw_data=_raw_data, **kwargs)).data
