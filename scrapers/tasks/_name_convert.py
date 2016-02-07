
from collections import defaultdict
import functools as ft
import itertools as it
import re

import jellyfish

from ._models import MP
from ..text_utils import translit_unaccent_lc


def _permute_declined_name(name, transforms):
    name_parts = re.findall(r'\w+', translit_unaccent_lc(name))
    if not 1 < len(name_parts) <= 3:
        raise ValueError('Too few or too many tokens in {!r}'.format(name))

    first, *middle, last = name_parts
    names = ((fore(first), *(middle and (aft(middle[0]),)), aft(last))
             for fore, aft in transforms)
    names = {' '.join(reversed(name)) for name in names}
    return names


def _generate_names():
    global NAMES, NAMES_NORM

    class _NameDict(dict):

        def __setitem__(self, key, value):
            if key in self:
                raise ValueError('Cannot reassign {!r}, must resolve conflict'
                                 .format(key))
            super().__setitem__(key, value)

    can_names = MP.collection.aggregate(
        [{'$project': {'name': '$name.el', 'other_name': '$name.el'}}])
    alt_names = MP.collection.aggregate(
        [{'$unwind': '$other_names'},
         {'$match': {'other_names.note': re.compile('el-Grek')}},
         {'$project': {'name': '$name.el',
                       'other_name': '$other_names.name'}}])
    NAMES = _NameDict()
    for k, v in ((mp['other_name'], mp['name']) for mp in it.chain(can_names,
                                                                   alt_names)):
        NAMES[k] = v
    NAMES.lengths_ = defaultdict(list)
    for k, v in it.groupby(NAMES.values(), key=len):
        NAMES.lengths_[k].extend(list(v))

    NAMES_NORM = _NameDict()  # {'last first': 'Last First', ...}
    for k, v in ((translit_unaccent_lc(k), v) for k, v in NAMES.items()):
        NAMES_NORM[k] = v
    NAMES_NORM.keys_ = set(NAMES_NORM)

_generate_names()

# ((fore_1.sub, aft_1.sub), (fore_1.sub, aft_2.sub), ...,
#  (fore_2.sub, aft_1.sub), ...)
TRANSFORMS = ([(r'', ''), (r'$', 'ς'), (r'ου$', 'ος'), (r'ς$', '')],  # fore
              [(r'', ''), (r'$', 'ς'), (r'ου$', 'ος')])               # aft
TRANSFORMS = it.product(*([(re.compile(p), r) for p, r in i]
                          for i in TRANSFORMS))
TRANSFORMS = tuple((ft.partial(f[0].sub, f[1]), ft.partial(a[0].sub, a[1]))
                   for f, a in TRANSFORMS)


@ft.lru_cache(maxsize=None)
def c14n_name_from_declined(name):
    """Pair a declined name with a canonical name in the database.

    >>> sorted(_permute_declined_name('Γιαννάκη Ομήρου',
    ...                               TRANSFORMS)) # doctest: +NORMALIZE_WHITESPACE
    ['ομηρος γιαννακη',  'ομηρος γιαννακης', 'ομηρου γιαννακη',
     'ομηρου γιαννακης', 'ομηρους γιαννακη', 'ομηρους γιαννακης']
    >>> c14n_name_from_declined('Ρούλλας Μαυρονικόλα')
    'Μαυρονικόλα Ρούλα'
    >>> c14n_name_from_declined('gibber ish') is None
    True
    >>> c14n_name_from_declined('gibberish')  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Too few or too many tokens in 'gibberish'
    >>> c14n_name_from_declined('a b c d')    # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Too few or too many tokens in 'a b c d'
    """
    match = _permute_declined_name(name, TRANSFORMS) & NAMES_NORM.keys_
    if len(match) > 1:
        raise ValueError('Multiple matches for ' + repr(name))
    if match:
        return NAMES_NORM[match.pop()]


@ft.lru_cache(maxsize=None)
def c14n_name_from_garbled(name, confidence=0.5):
    """Pair a jumbled up MP name with a canonical name in the database.

    >>> c14n_name_from_garbled('Καπθαιηάο Αληξέαο')   # Jesus take the wheel
    'Καυκαλιάς Αντρέας'
    >>> c14n_name_from_garbled('') is None
    True
    """
    if not name:
        return
    try:
        return NAMES[name]
    except KeyError:
        try:
            dist, new_name = max((jellyfish.jaro_distance(name, v), v)
                                 for v in NAMES.lengths_.get(len(name), ()))
        except ValueError:
            return
        if dist > confidence:
            return new_name
