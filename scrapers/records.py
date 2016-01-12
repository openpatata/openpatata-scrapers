
"""Models for our records."""

from collections import OrderedDict
from copy import deepcopy
import itertools
from pathlib import Path
import re

import pymongo

from scrapers import db
from scrapers.misc_utils import starfilter


class InsertError(Exception):
    """Error raised when a database insert fails."""


class _Record:
    """A base class for our records."""

    def __init__(self, data, is_raw=False):
        if is_raw is True:
            self.data = deepcopy(data)
            return

        self.data = deepcopy(self.template)
        self.data.update(data)
        for update in self._on_init_transforms():
            self.data.update(update)
        if not all(map(self.data.get, (self.required_properties +
                                       ('_filename', '_sources')))):
            raise ValueError(', '.join(map(repr, self.required_properties)) +
                             ", '_filename' and '_sources' are required in " +
                             repr(self))

    def _compact(self):
        """Filter out boolean `False` values, returning a copy of the `_Record`.

        >>> (_Record({'a': {'b': {'c': 1}}, 'd': ''}, is_raw=True)
        ...  ._compact().data == {'a': {'b': {'c': 1}}})
        True
        >>> (_Record({'a': {'b': {'c': 1}}, 'd': {'e': ''}}, is_raw=True)
        ...  ._unwrap()._compact().data == {'a.b.c': 1})
        True
        """
        return self.__class__(
            self.data.__class__(starfilter(lambda _, v: v, self.data.items())),
            is_raw=True)

    def _on_init_transforms(self):
        raise NotImplementedError

    def _prepare_inserts(self):
        raise NotImplementedError

    def _sort(self):
        """Traverse `self.data` to sort it and all sub-dicts alphabetically."""
        def sort(value):
            if isinstance(value, dict):
                return OrderedDict(itertools.starmap(
                    lambda k, v: (k, sort(v)), sorted(value.items())))
            elif isinstance(value, list):
                return list(map(sort, value))
            return value

        return self.__class__(OrderedDict(sort(self.data)), is_raw=True)

    def _unwrap(self):
        """Flatten the `_Record` recursively, returning a copy of it.

        >>> (_Record({'a': {'b': {'c': [1, 2], 'd': 3}}, 'e': {'f': ''}},
        ...          is_raw=True)
        ...  ._unwrap().data == {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''})
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

    @property
    def exists(self):
        """See whether a `_Record` with the same `_filename` already exists."""
        return db[self.collection]\
            .find_one({'_filename': self.data['_filename']})

    def insert(self, compact=False, upsert=True):
        """Insert a `_Record` in the database.

        `insert` returns the resulting document on success and
        raises `InsertError` on failure.
        """
        data = self._unwrap()
        if compact is True:
            data = data._compact()
        data = data._sort().data

        for insert in self._prepare_inserts(data, compact):
            value = db[self.collection].find_one_and_update(
                filter={'_filename': self.data['_filename']},
                update=insert,
                upsert=upsert,
                return_document=pymongo.ReturnDocument.AFTER)
        if not value:
            raise InsertError('Unable to insert or merge {!r}'.format(self))
        return value

    def merge(self):
        """Convenience wrapper around `self.insert` for merging."""
        return self.insert(compact=True, upsert=False)

    def __repr__(self):
        return '<{}: {!r}>'.format(self.__class__.__name__, self.data)


class Bill(_Record):

    collection = 'bills'
    template = {'actions': [], 'identifier': None, 'title': None}
    required_properties = ('identifier', 'title')

    def _on_init_transforms(self):
        return {'_filename': self.data['identifier']},

    def _prepare_inserts(self, data, compact):
        ins = {'$set': data,
               '$addToSet': {'_sources': {'$each': data.pop('_sources')}}}
        actions = data.pop('actions', None)
        if actions:
            ins['$addToSet'].update({'actions': {'$each': actions}})
        return ins, \
            {'$push': {'_sources': {'$each': [], '$sort': 1},
                       'actions': {'$each': [], '$sort': {'at_plenary': 1}}}}


class CommitteeReport(_Record):

    collection = 'committee_reports'
    template = {'attendees': [],
                'date_circulated': None,
                'date_prepared': None,
                'relates_to': [],
                'text': None,
                'title': None,
                'url': None}
    required_properties = ('title', 'url')

    def _on_init_transforms(self):
        return {'_filename': '_'.join((self.data['date_circulated'] or '_',
                                       Path(self.data['url']).stem))},

    def _prepare_inserts(self, data, compact):
        return {'$set': data},


class PlenarySitting(_Record):

    collection = 'plenary_sittings'
    template = {'agenda': {'cap1': [], 'cap2': [], 'cap4': []},
                'attendees': [],
                'date': None,
                'links': [],
                'parliamentary_period': None,
                'session': None,
                'sitting': None}
    required_properties = ('date', 'parliamentary_period')

    def _on_init_transforms(self):
        data = self.data
        return {'_filename': '_'.join(map(str, (data['date'],
                                                data['parliamentary_period'],
                                                data['session'],
                                                data['sitting'])))},

    def _prepare_inserts(self, data, compact):
        ins = {'$set': data,
               '$addToSet': {'_sources': {'$each': data.pop('_sources')}}}
        if compact:
            debate = data.pop('agenda.debate', None)
            if debate:
                ins['$addToSet'].update({'agenda.debate': {'$each': debate}})

            legislative_work = data.pop('agenda.legislative_work', None)
            if legislative_work:
                ins['$addToSet'].update(
                    {'agenda.legislative_work': {'$each': legislative_work}})

            links = data.pop('links', None)
            if links:
                ins['$addToSet'].update({'links': {'$each': links}})
        return ins, {'$push': {'_sources': {'$each': [], '$sort': 1},
                               'links': {'$each': [], '$sort': {'type': 1}}}}


class Question(_Record):

    collection = 'questions'
    template = {'answers': [],
                'by': [],
                'date': None,
                'heading': None,
                'identifier': None,
                'text': None}
    required_properties = ('date', 'heading', 'identifier', 'text')

    def _on_init_transforms(self):
        filename = self.data['identifier']
        other = db[self.collection]\
            .count({'_filename': re.compile(r'{}(_\d+)?$'.format(filename))})
        if other:
            filename = '_'.join((filename, str(other + 1)))
        return {'_filename': filename},

    def _prepare_inserts(self, data, compact):
        return {'$set': data},
