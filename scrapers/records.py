
"""Models for our records."""

from collections import OrderedDict
import itertools
from pathlib import Path
import re

import pymongo

from scrapers import db


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

    def _construct_filename(self):
        """Subclass to construct the `_filename` at initialisation."""
        raise NotImplementedError

    def _prepare_insert(self, compact):
        """Subclass to prepare a _Record for `self.insert`."""
        return getattr(self, 'compact' if compact else 'unwrap')().ordered

    @property
    def ordered(self):
        """Traverse the _Record to sort it and all sub-dicts alphabetically."""
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

    def unwrap(self):
        r"""Flatten the _Record recursively.

        >>> (_Record({'a': {'b': {'c': [1, 2], 'd': 3}},
        ...           'e': {'f': ''}}).unwrap() ==
        ...  {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''})
        True
        """
        return self.__class__(self._rekey(lambda a, b: (
            '.'.join((a, b)) if a else b)))

    def compact(self):
        r"""Filter out boolean `False` values after flattening.

        This method is useful when updating an existing record, so as to
        not discard nested siblings.

        >>> (_Record({'a': {'b': {'c': 1}}, 'd': {'e': ''}}).compact() ==
        ...  {'a.b.c': 1})
        True
        """
        return self.__class__(filter(lambda i: bool(i[1]),
                                     self.unwrap().items()))

    @property
    def exists(self):
        """See whether a `_Record` with the same `_filename` already exists."""
        return db[self.collection].find_one({'_filename': self['_filename']})

    def insert(self, compact=False, upsert=True):
        """Insert a `_Record` in the database.

        Returns the resulting document on success and raises `InsertError`
        on failure.
        """
        rval = db[self.collection].find_one_and_update(
            filter={'_filename': self['_filename']},
            update=self._prepare_insert(compact), upsert=upsert,
            return_document=pymongo.ReturnDocument.AFTER)
        if not rval:
            raise InsertError('Unable to insert or merge {}'.format(self))
        return rval

    def merge(self):
        """Convenience wrapper around `self.insert` for merging."""
        return self.insert(compact=True, upsert=False)

    @classmethod
    def from_template(cls, sources=None, update=None):
        """Pre-fill a _Record using its template."""
        value = cls(eval(cls.template))   # Fair bit easier than deep-copying
        value['_sources'] = list(sources) if sources else []
        if update:
            value.update(update)
        value['_filename'] = value._construct_filename()
        return value

    def __repr__(self):
        # Print out the `type()` in addition to the `dict`.
        return '<{}: {!r}>'.format(self.__class__.__name__, super().__repr__())


class Bill(_Record):

    collection = 'bills'
    template = """{'identifier': None,
                   'title': None}"""

    def _construct_filename(self):
        return self['identifier']

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        return {'$set': value}


class CommitteeReport(_Record):

    collection = 'committee_reports'
    template = """{'attendees': [],
                   'date_circulated': None,
                   'date_prepared': None,
                   'relates_to': [],
                   'text': None,
                   'title': None,
                   'url': None}"""

    def _construct_filename(self):
        return '{}_{}'.format(self['date_circulated'] or '_',
                              Path(self['url']).stem)

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        return {'$set': value}


class PlenarySitting(_Record):

    collection = 'plenary_sittings'
    template = """{'agenda': {'debate': [],
                              'legislative_work': []},
                   'attendees': [],
                   'date': None,
                   'links': [],
                   'parliamentary_period': None,
                   'session': None,
                   'sitting': None}"""

    def _construct_filename(self):
        return '_'.join(map(str, (self['date'], self['parliamentary_period'],
                                  self['session'], self['sitting'])))

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        value['date'] = value['date'].date if hasattr(value['date'], 'date') \
            else value['date']
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

    def _construct_filename(self):
        filename = self['identifier']
        other = db[self.collection].count(
            {'_filename': re.compile(r'{}(_\d+)?$'.format(filename))})
        if other:
            return '{}_{}'.format(filename, other+1)
        else:
            return filename

    def _prepare_insert(self, compact):
        value = super()._prepare_insert(compact)
        return {'$set': value}
