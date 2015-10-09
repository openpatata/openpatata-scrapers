
"""Models for our records."""


class _Record(dict):

    """A wrapper for our records."""

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
    def _rm_falsies(o):   # {'a': '', 'b': 12} -> {'b': 12}
        # Recursively remove boolean false values from a Record and enclosed
        # lists and list-alikes, returning a copy of the Record
        def _process(o):
            if isinstance(o, (list, set, tuple)):
                return [_process(i) for i in o if _process(i)]
            elif isinstance(o, dict):
                return {k: _process(v) for k, v in o.items() if _process(v)}
            return o
        return _process(o)

    def flatten(self, t=None):  # {'a': {'b': 1}} -> {'a.b': 1}
        """Flatten the Record recursively."""
        return self._rekey(t or self,
                           lambda a, b: '.'.join((a, b)) if a else b)

    def compact(self):
        """Both flatten and `_rm_falsies`.

        This method is useful when updating an existing record, so as to
        not blank nested siblings. To illustrate, if we were to execute
        {'a': {'b': 3}} on {'a': {'b': 1, 'c': 2}}, we'd be overwriting
        `a`, and `c` would've been thrown under the bus. Dot notation is
        how mongo's told to navigate inside `a`.
        """
        return self._rm_falsies(self.flatten())


class Bill(_Record):

    def __init__(self, **kwargs):
        super().__init__({
            '_filename': None,
            'identifier': '',
            'title': ''}, **kwargs)


class CommitteeReport(_Record):

    def __init__(self, **kwargs):
        super().__init__({
            '_filename': None,
            'belongs_to': [],
            'date': '',
            'mps_present': [],
            'text': '',
            'title': '',
            'url': ''}, **kwargs)


class PlenarySitting(_Record):

    def __init__(self, **kwargs):
        super().__init__({
            '_filename': None,
            'date': '',
            'agenda': {
                'debate': [],
                'legislative_work': []},
            'links': [],
            'parliament': '',
            'session': '',
            'sitting': ''}, **kwargs)


class Question(_Record):

    def __init__(self, **kwargs):
        super().__init__({
            '_filename': None,
            'answers': [],
            'identifier': '',
            'question': {
              'by': [],
              'date': '',
              'text': [],
              'title': ''}}, **kwargs)
