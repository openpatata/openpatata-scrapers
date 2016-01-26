
"""Models for our records."""

from collections import OrderedDict
from copy import deepcopy
import itertools as it

import pymongo

from . import db
from .misc_utils import starfilter


class InsertError(Exception):
    """Error raised when a database insert fails."""


def _compact(self):
    """Filter out boolean `False` values, returning a copy of the `BaseRecord`.

    >>> _compact(BaseRecord({'a': {'b': {'c': 1}}, 'd': ''},
    ...                     is_raw=True)).data == {'a': {'b': {'c': 1}}}
    True
    >>> _compact(_unwrap(BaseRecord({'a': {'b': {'c': 1}}, 'd': {'e': ''}},
    ...                             is_raw=True))).data == {'a.b.c': 1}
    True
    """
    return self.__class__(
        self.data.__class__(starfilter(lambda _, v: v, self.data.items())),
        is_raw=True)


def _sort(self):
    """Traverse `self.data` to sort it and all sub-dicts alphabetically."""
    def sort(value):
        if isinstance(value, dict):
            return OrderedDict(it.starmap(lambda k, v: (k, sort(v)),
                                          sorted(value.items())))
        elif isinstance(value, list):
            return list(map(sort, value))
        return value

    return self.__class__(OrderedDict(sort(self.data)), is_raw=True)


def _unwrap(self):
    """Flatten the `BaseRecord` recursively, returning a copy of it.

    >>> (_unwrap(BaseRecord({'a': {'b': {'c': [1, 2], 'd': 3}}, 'e': {'f': ''}},
    ...                    is_raw=True))
    ...  .data == {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''})
    True
    """
    def rekey(value, pk=''):
        if isinstance(value, dict):
            for k in value:
                yield from rekey(value[k], '.'.join((pk, k)) if pk else k)
        else:
            yield pk, value

    return self.__class__(self.data.__class__(rekey(self.data)),
                          is_raw=True)


class BaseRecord:
    """A base class for our records."""

    def __init__(self, data, is_raw=False):
        if hasattr(self, 'collection'):
            self.collection = db[self.collection]
        self._value_in_db = None

        if is_raw is True:
            self.data = deepcopy(data)
            return
        self.data = deepcopy(self.template)
        self.data.update(data)
        for update in self._on_init_transforms():
            self.data.update(update)
        if not all(map(self.data.get, it.chain(self.required_properties,
                                               ('_filename', '_sources')
                                               ))):
            raise ValueError(', '.join(map(repr, self.required_properties)) +
                             ", '_filename' and '_sources' are required in " +
                             repr(self))

    def __repr__(self):
        return '<{}: {!r}>'.format(self.__class__.__name__, self.data)

    def _on_init_transforms(self):
        raise NotImplementedError

    def _prepare_inserts(self):
        raise NotImplementedError

    @property
    def exists(self):
        """See whether a `BaseRecord` with the same `_filename` already exists."""
        filter_ = {'_filename': self.data['_filename']}
        return bool(self.collection.find_one(filter=filter_))

    def insert(self, merge=False):
        """Insert a `BaseRecord` in the database.

        `insert` returns the resulting document on success and
        raises `InsertError` on failure.
        """
        data = _sort(_unwrap(self)).data
        if merge is True:
            data = _compact(self.__class__(data, is_raw=True)).data

        filter_ = {'_filename': self.data['_filename']}
        if merge is not True:
             self.collection.find_one_and_delete(filter=filter_)

        for insert in self._prepare_inserts(data, merge):
            return_value = self._value_in_db = \
                self.collection.find_one_and_update(
                    filter=filter_, update=insert, upsert=not merge,
                    return_document=pymongo.ReturnDocument.AFTER)
            if not return_value:
                raise InsertError('Unable to insert or merge ' + repr(self))
        return return_value
