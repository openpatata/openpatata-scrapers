
"""Models for our records."""

from collections import OrderedDict
import itertools

import pymongo

from scrapers import db


class _Record(dict):
    """A base class for our records."""

    def __rekey(self, transform):
        # Recursively apply `transform` to the keys of a _Record, returning
        # a copy of it
        def _inner(value, pk=''):
            if isinstance(value, dict):
                for k in value:
                    yield from _inner(value[k], transform(pk, k))
            else:
                yield pk, value
        return _inner(self)

    def _prepare(self, compact):
        """Subclass to prepare a _Record for `self.insert`."""
        return getattr(self, 'compact' if compact else 'flatten')().ordered

    @property
    def ordered(self):
        """Traverse the _Record to sort it and all sub-dicts alphabetically."""
        new_type = type(type(self).__name__,
                        (type(self), OrderedDict), {})

        def _inner(value):
            if isinstance(value, dict):
                return OrderedDict(itertools.starmap(
                    lambda k, v: (k, _inner(v)), sorted(value.items())))
            elif isinstance(value, list):
                return list(map(_inner, value))
            return value
        return new_type(_inner(self))

    def flatten(self):
        r"""Flatten the _Record recursively.

        >>> (_Record({'a': {'b': {'c': [1, 2], 'd': 3}},
        ...           'e': {'f': ''}}).flatten() ==
        ...  {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''})
        True
        """
        return type(self)(self.__rekey(lambda a, b: ('.'.join((a, b))
                                                     if a else b)))

    def compact(self):
        r"""Filter out booleans `False` values after flattening.

        This method is useful when updating an existing record, so as to
        not discard nested siblings.

        >>> (_Record({'a': {'b': {'c': 1}}, 'd': {'e': ''}}).compact() ==
        ...  {'a.b.c': 1})
        True
        """
        return type(self)(filter(lambda i: bool(i[1]), self.flatten().items()))

    def insert(self,
               filter=None, compact=False, upsert=True):
        """Insert a _Record in the database, returning the resulting
        document or `None`.
        """
        return db[self.collection].find_one_and_update(
            filter or {'_filename': self['_filename']},
            update=self._prepare(compact), upsert=upsert,
            return_document=pymongo.ReturnDocument.AFTER)

    def merge(self, filter=None):
        """Convenience wrapper around `self.insert` for merging."""
        return self.insert(filter, compact=True, upsert=False)

    @classmethod
    def from_template(cls, update=None):
        """Pre-fill a _Record using its template."""
        value = cls(eval(cls.template))   # Fair bit easier than deep-copying
        if update:
            value.update(update)
        return value


class Bill(_Record):

    collection = 'bills'
    template = """{
        '_filename': None,
        'identifier': None,
        'title': None}"""

    def _prepare(self, compact):
        value = super()._prepare(compact)
        return {'$set': value}


class Committee(_Record):

    collection = 'committees'
    template = """{
        '_filename': None,
        'name': {
            'el': None,
            'en': None}}"""

    def _prepare(self, compact):
        value = super()._prepare(compact)
        return {'$set': value}


class CommitteeReport(_Record):

    collection = 'committee_reports'
    template = """{
        '_filename': None,
        'attendees': [],
        'date_circulated': None,
        'date_prepared': None,
        'relates_to': [],
        'text': None,
        'title': None,
        'url': None}"""

    def _prepare(self, compact):
        value = super()._prepare(compact)
        return {'$set': value}


class PlenarySitting(_Record):

    collection = 'plenary_sittings'
    template = """{
        '_filename': None,
        'agenda': {
            'debate': [],
            'legislative_work': []},
        'attendees': [],
        'date': None,
        'links': [],
        'parliament': None,
        'session': None,
        'sitting': None}"""

    def _version_filename_on_insert(self, value):
        # Version same-day sitting filenames from oldest to newest;
        # extraordinary sittings come last. We're doing this bit of filename
        # trickery 'cause:
        # (a) it's probably a good idea if the filenames were to persist; and
        # (b) Parliament similarly version the transcript filenames, meaning
        # that we can avoid downloading and parsing the PDFs (for now, anyway)
        if 'date' not in value:
            return

        sittings = (
            {(self.from_template(p)['sitting'] or None) for p in
             db[self.collection].find(filter={'date': value['date']})} |
            {value['sitting'] or None})
        sittings = sorted(sittings,
                          key=lambda v: float('inf') if v is None else v)
        for c, sitting in enumerate(sittings):
            if c:
                _filename = '{}_{}.yaml'.format(value['date'], c+1)
            else:
                _filename = '{}.yaml'.format(value['date'])
            db[self.collection].find_one_and_update(
                filter={'date': value['date'], 'sitting': sitting},
                update={'$set': {'_filename': _filename}})

            if value['sitting'] == sitting:
                value['_filename'] = _filename

    def _prepare(self, compact):
        value = super()._prepare(compact)
        self._version_filename(value)

        ins = {'$set': value}
        if compact:
            links = value.pop('links', None)
            if links:
                ins.update({'$addToSet': {'links': {'$each': links}}})

        return ins

    @classmethod
    def select_date(cls, date):
        """See if there was a second plenary on the `date`, returning its
        slug.
        """
        if date.date != date.slug and db[cls.collection].find_one(
                filter={'_filename': '{}.yaml'.format(date.slug)}):
            return date.slug
        else:
            return date.date


class Question(_Record):

    collection = 'questions'
    template = """{
        '_filename': None,
        'answers': [],
        'by': [],
        'date': None,
        'heading': None,
        'identifier': None,
        'text': None}"""

    def _prepare(self, compact):
        value = super()._prepare(compact)
        return {'$set': value}
