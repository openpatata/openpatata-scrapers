
"""Models for our records."""


class _Record(dict):

    """A dictionary that sheds any values which evaluate to boolean false
    on request.
    """

    @staticmethod
    def _rm_falsies(o):
        # Recursively remove falsies from dictionaries, lists and list-alikes
        def _process(o):
            if isinstance(o, (list, set, tuple)):
                return [_process(i) for i in o if _process(i)]
            elif isinstance(o, dict):
                return {k: _process(v) for k, v in o.items() if _process(v)}
            return o
        return _process(o)

    def compact(self):
        """Set the Record straight."""
        return self._rm_falsies(self) or {}


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
