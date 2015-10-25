
"""Models for our records."""


class _Record(dict):

    """A wrapper for our records."""

    def __init__(self, pro_forma, insert=None):
        super().__init__(pro_forma)
        if insert:
            self.update(insert)

    @staticmethod
    def _rekey(o, transform):
        # Recursively apply `transform` on the keys of a Record, returning
        # a copy of it
        def _process(o, pk=''):
            if isinstance(o, dict):
                for k in o:
                    yield from _process(o[k], transform(pk, k))
            else:
                yield pk, o
        return dict(_process(o))

    @staticmethod
    def _rm_falsies(o):
        # Recursively remove boolean false values from a Record and enclosed
        # lists and list-alikes, returning a copy of the Record:
        #   {'a': None, 'b': 12} -> {'b': 12}
        def _process(o):
            if isinstance(o, (list, set, tuple)):
                return [_process(i) for i in o if _process(i)]
            elif isinstance(o, dict):
                return {k: _process(v) for k, v in o.items() if _process(v)}
            return o
        return _process(o)

    def flatten(self, t=None):
        r"""Flatten the Record recursively.

        >>> _Record({'a': {'b': {'c': [1, 2], 'd': 3}},
        ...          'e': {'f': ''}}).flatten() == \
        ... {'a.b.c': [1, 2], 'a.b.d': 3, 'e.f': ''}
        True
        """
        return self._rekey(t or self,
                           lambda a, b: '.'.join((a, b)) if a else b)

    def compact(self):
        r"""Both flatten and `_rm_falsies`.

        This method is useful when updating an existing record, so as to
        not blank nested siblings. To illustrate, if we were to execute
        {'a': {'b': 3}} on {'a': {'b': 1, 'c': 2}}, we'd be overwriting
        `a`, and `c` would've been thrown under the bus. Dot notation is
        how mongo's told to navigate inside `a`.

        >>> _Record({'a': {'b': {'c': 1}}, 'd': {'e': ''}}).compact() == \
        ... {'a.b.c': 1}
        True
        """
        return self._rm_falsies(self.flatten())

    def prepare(self):
        """Subclass to construct a mongo insert."""
        raise NotImplementedError


class Bill(_Record):

    def __init__(self, insert):
        super().__init__({
            '_filename': None,
            'identifier': None,
            'title': None}, insert)


class Committee(_Record):

    def __init__(self, insert):
        super().__init__({
            '_filename': None,
            'name': {
                'el': None,
                'en': None}}, insert)


class CommitteeReport(_Record):

    def __init__(self, insert):
        super().__init__({
            '_filename': None,
            'attendees': [],
            'date_circulated': None,
            'date_prepared': None,
            'relates_to': [],
            'text': None,
            'title': None,
            'url': None}, insert)


class PlenarySitting(_Record):

    def __init__(self, insert):
        super().__init__({
            '_filename': None,
            'attendees': [],
            'date': None,
            'agenda': {
                'debate': [],
                'legislative_work': []},
            'links': [],
            'parliament': None,
            'session': None,
            'sitting': None}, insert)

    def prepare(self, compact=True):
        val = getattr(self, 'compact' if compact else 'flatten')()
        links = val.pop('links')
        if links:
            return {'$set': val, '$addToSet': {'links': {'$each': links}}}
        else:
            return {'$set': val}


class Question(_Record):

    def __init__(self, insert):
        super().__init__({
            '_filename': None,
            'answers': [],
            'by': [],
            'date': None,
            'heading': None,
            'identifier': None,
            'text': None}, insert)
