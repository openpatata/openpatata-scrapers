
"""Model classes."""

from collections import OrderedDict
from copy import deepcopy
import itertools as it

import pymongo

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
        cls.properties = set(cls.template)
        cls.required_properties = sorted(getattr(cls, 'required_properties',
                                                 ()))
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
    >>> Base()   # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    ValueError: 'a', 'b' are required properties in
    <Base: OrderedDict([('a', None), ('b', None), ('c', None)])>
    >>> Base(d=4, e=5)    # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    ValueError: 'd', 'e' are extraneous in <Base: OrderedDict(...)>
    """

    def __init__(self, *, _raw_data={}, **kwargs):
        if _raw_data:
            self.data = deepcopy(_raw_data)
            return
        self.data = deepcopy(self.template)
        self.data.update(kwargs)
        # In the interest of getting things done within a reasonable time
        # frame, we're punting on (very) light validation.  Notably,
        # there's no type checking.  More thorough tests are found in
        # the data repo
        extraneous_properties = self.data.keys() - self.properties
        if extraneous_properties:
            raise ValueError(', '.join(map(repr,
                                           sorted(extraneous_properties))) +
                             ' are extraneous in ' + repr(self))
        if self.required_properties and not all(map(self.data.get,
                                                    self.required_properties)):
            raise ValueError(', '.join(map(repr, self.required_properties)) +
                             ' are required properties in ' + repr(self))

    def __repr__(self):
        return '<{}: {!r}>'.format(self.__class__.__name__, self.data)


class _prepare_insertable_record(_prepare_record):

    def __new__(cls, name, bases, cls_dict):
        return super().__new__(
            cls, name, bases,
            {**cls_dict, 'template': {**cls_dict.get('template', {}), '_id': None}})


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
        ...     template = {'some_field': 'some_data'} \

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
        >>> del foo.data['_id']
        >>> foo.insert()   # doctest: +ELLIPSIS
        Traceback (most recent call last):
          ...
        ValueError: No `_id` provided in <Insertable: OrderedDict([('some_field', 'some_other_data')])>

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
        if not _raw_data:
            self.data['_id'] = self.generate__id()

    @property
    def _id(self):
        """The record's primary key."""
        return self.data.get('_id')

    def delete(self):
        """Delete the document from the database."""
        return self.collection.find_one_and_delete(filter={'_id': self._id})

    @property
    def exists(self):
        """
        Check to see whether a record with the same `self._id` exists in
        the database.
        """
        return bool(self.collection.find_one(filter={'_id': self._id}))

    @classmethod
    def export(cls, format_='csv'):
        if format_ == 'json':
            return _text_from_sp(
                ('mongoexport', '--jsonArray',
                                '--db=' + cls.collection.database.name,
                                '--collection=' + cls.collection.name))
        return _text_from_sp(
            ('mongoexport', '--type=' + format_,
                            '--db=' + cls.collection.database.name,
                            '--collection=' + cls.collection.name,
                            '--fields=' + ','.join(sorted(cls.properties))))

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
        if new:
            self.delete()
            data = deepcopy(self.data)
        else:
            data = _compact(_unwrap(self)).data

        inserts = self.generate_inserts(merge)
        for _ in inserts:
            data.pop('_id')
            insert = inserts.send(data)
            data = self.collection.find_one_and_update(
                filter={'_id': self._id}, update=insert, upsert=new,
                return_document=pymongo.ReturnDocument.AFTER)
            if not data:
                raise InsertError('Unable to insert or merge ' + repr(self) +
                                  ' with operation ' + repr(insert))
        self.data = data
        return data

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
    ...     required_properties = {'a', 'b'}

    >>> Sub(a=1, b=2)
    OrderedDict([('a', 1), ('b', 2), ('c', None)])
    >>> Sub()   # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    ValueError: 'a', 'b' are required properties in
    <Sub: OrderedDict([('a', None), ('b', None), ('c', None)])>
    """

    def __new__(cls, *, _raw_data={}, **kwargs):
        return _sort(cls._construct(_raw_data=_raw_data, **kwargs)).data
