
"""Models for our records."""

from collections import defaultdict, OrderedDict
import functools
import itertools
from pathlib import Path
import re

import pymongo

from scrapers import db
from scrapers.misc_utils import starfilter


_after_init_hooks = defaultdict(list)


def _class_name_of_method(method):
    class_name, _, _ = method.__qualname__.rpartition('.')
    return class_name


def _hook_method_to(hook, method):
    hook[_class_name_of_method(method)] += [method]
    del method

_after_init = functools.partial(_hook_method_to, _after_init_hooks)


class InsertError(Exception):
    """Error raised when a database insert fails."""


class _Record(dict):
    """A base class for our records."""

    def _rekey(self, transform):
        # Recursively apply `transform` to the keys of a _Record, returning
        # a copy of it
        def inner(value, pk=''):
            if isinstance(value, dict):
                for k in value:
                    yield from inner(value[k], transform(pk, k))
            else:
                yield pk, value
        return inner(self)

    def _sort(self):
        # Traverse the _Record to sort it and all sub-dicts alphabetically.
        def inner(value):
            if isinstance(value, dict):
                return OrderedDict(itertools.starmap(
                    lambda k, v: (k, inner(v)), sorted(value.items())))
            elif isinstance(value, list):
                return list(map(inner, value))
            return value

        new_type = type(self.__class__.__name__,
                        (self.__class__, OrderedDict), {})
        return new_type(inner(self))

    def _unwrap(self):
        """Flatten the _Record recursively.

        >>> (_Record({'a': {'b': {'c': [1, 2], 'd': 3}}, 'e': {'f': ''}})
        ...  ._unwrap() == {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''})
        True
        """
        return self.__class__(self._rekey(
            lambda a, b: '.'.join((a, b)) if a else b))

    def _compact(self):
        """Filter out boolean `False` values.

        This method is useful when updating an existing record, so as to
        not discard nested siblings.

        >>> (_Record({'a': {'b': {'c': 1}}, 'd': ''})
        ...  ._compact() == {'a': {'b': {'c': 1}}})
        True
        >>> (_Record({'a': {'b': {'c': 1}}, 'd': {'e': ''}})
        ...  ._unwrap()._compact() == {'a.b.c': 1})
        True
        """
        return self.__class__(starfilter(lambda _, v: bool(v), self.items()))

    @property
    def exists(self):
        """See whether a `_Record` with the same `_filename` already exists."""
        return db[self.collection].find_one({'_filename': self['_filename']})

    @property
    def uid(self):
        """The `_Record`'s unique id, defaulting to its filename."""
        return self['_filename']

    def _prepare_insert(self, compact):
        return_value = self._unwrap()
        if compact is True:
            return_value = return_value._compact()
        return return_value._sort()

    def insert(self, compact=False, upsert=True):
        """Insert a `_Record` in the database.

        `insert` returns the resulting document on success and
        raises `InsertError` on failure.
        """
        return_value = db[self.collection]\
            .find_one_and_update(filter={'_filename': self['_filename']},
                                 update=self._prepare_insert(compact),
                                 upsert=upsert,
                                 return_document=pymongo.ReturnDocument.AFTER)
        if not return_value:
            raise InsertError('Unable to insert or merge {!r}'.format(self))
        return return_value

    def merge(self):
        """Convenience wrapper around `self.insert` for merging."""
        return self.insert(compact=True, upsert=False)

    @classmethod
    def from_template(cls, update={}):
        """Pre-fill a _Record using its template."""
        value = cls(eval(cls.template))   # Fair bit easier than deep-copying
        value.update(update)
        for hook in _after_init_hooks[cls.__name__]:
            value = hook(value)
        if not all(map(value.get, cls.required_properties + ('_filename',
                                                             '_sources'))):
            raise ValueError(', '.join(map(repr, cls.required_properties)) +
                             ", '_filename' and '_sources' are required in " +
                             repr(value))
        return value

    def __repr__(self):
        # Print out the `type()` in addition to the `dict`.
        return '<{}: {!r}>'.format(self.__class__.__name__, super().__repr__())


class Bill(_Record):

    collection = 'bills'
    template = """{'actions': [],
                   'identifier': None,
                   'title': None}"""
    required_properties = ('identifier', 'title')

    @_after_init
    def _construct_filename(self):
        self['_filename'] = self['identifier']
        return self

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        ins = {'$set': value,
               '$addToSet': {'_sources': {'$each': value.pop('_sources')}}}
        actions = value.pop('actions', None)
        if actions:
            ins['$addToSet'].update({'actions': {'$each': actions}})
        return ins


class CommitteeReport(_Record):

    collection = 'committee_reports'
    template = """{'attendees': [],
                   'date_circulated': None,
                   'date_prepared': None,
                   'relates_to': [],
                   'text': None,
                   'title': None,
                   'url': None}"""
    required_properties = ('title', 'url')

    @_after_init
    def _construct_filename(self):
        self['_filename'] = '{}_{}'.format(self['date_circulated'] or '_',
                                           Path(self['url']).stem)
        return self

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        return {'$set': value}


class PlenarySitting(_Record):

    collection = 'plenary_sittings'
    template = """{'agenda': {'cap1': [],
                              'cap2': [],
                              'cap4': []},
                   'attendees': [],
                   'date': None,
                   'links': [],
                   'parliamentary_period': None,
                   'session': None,
                   'sitting': None}"""
    required_properties = ('date', 'parliamentary_period')

    @_after_init
    def _construct_filename(self):
        self['_filename'] = '_'.join(map(str, (self['date'],
                                               self['parliamentary_period'],
                                               self['session'],
                                               self['sitting'])))
        return self

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        ins = {'$addToSet': {}, '$set': value}
        source = value.pop('_sources')
        if source:
            ins['$addToSet'].update({'_sources': {'$each': source}})
        if compact:
            debate = value.pop('agenda.debate', None)
            if debate:
                ins['$addToSet'].update({'agenda.debate': {'$each': debate}})

            legislative_work = value.pop('agenda.legislative_work', None)
            if legislative_work:
                ins['$addToSet'].update(
                    {'agenda.legislative_work': {'$each': legislative_work}})

            links = value.pop('links', None)
            if links:
                ins['$addToSet'].update({'links': {'$each': links}})
        return ins


class Question(_Record):

    collection = 'questions'
    template = """{'answers': [],
                   'by': [],
                   'date': None,
                   'heading': None,
                   'identifier': None,
                   'text': None}"""
    required_properties = ('date', 'heading', 'identifier', 'text')

    @_after_init
    def _construct_filename(self):
        filename = self['identifier']
        other = db[self.collection].count(
            {'_filename': re.compile(r'{}(_\d+)?$'.format(filename))})
        if other:
            self['_filename'] = '{}_{}'.format(filename, other+1)
        else:
            self['_filename'] = filename
        return self

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        return {'$set': value}
