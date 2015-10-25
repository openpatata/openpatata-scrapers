
"""Various stand-alone utilities for manipulating text."""

from collections import Counter
from functools import reduce, lru_cache
import itertools
import re
import subprocess

import icu
import Levenshtein

from . import db


def pdf2text(stream):
    """Parse a bytes object into a PDF and into text."""
    text = subprocess.run(['pdftotext', '-layout', '-', '-'],
                          input=stream, stdout=subprocess.PIPE)
    text = text.stdout.decode(encoding='utf-8')
    return text


class TableParser:
    """A tool for sifting through plain-text tables."""

    def __init__(self, text, max_cols=3):
        self._lines = [line.lstrip() for line in text.splitlines()]
        self._max_cols = max_cols

    @staticmethod
    def _cols_of(line):
        """The leading-edge indices of hypothetical columns within a line.

        >>> TableParser._cols_of('Lorem ipsum   dolor sit   amet')
        (14, 26)
        """
        return tuple(m.end() for m in re.finditer(r'\s{2,}', line))

    @property
    def _col_modes(self):
        """The `self._max_cols` most common indices in `self._lines`.

        >>> TableParser('''
        ... Lorem ipsum   dolor sit   amet
        ... consectetur   adipiscing  totes elit
        ... ''')._col_modes
        (0, 14, 26)
        """
        col_freq = reduce(
            lambda c, v: c + Counter(dict.fromkeys(self._cols_of(v), 1)),
            self._lines,
            Counter({0: float('inf')}))
        return tuple(sorted(dict(col_freq.most_common(self._max_cols))))

    @property
    def _col_edges(self):
        """Generate slices from column indices.

        >>> TableParser('''
        ... Lorem ipsum   dolor sit   amet
        ... consectetur   adipiscing  totes elit
        ... ''')._col_edges
        (slice(0, 14, None), slice(14, 26, None), slice(26, None, None))
        """
        cols = self._col_modes
        cols = itertools.zip_longest(cols, cols[1:])
        return tuple(itertools.starmap(slice, cols))

    @property
    def rows(self):
        """Produce a cols within a row matrix, sans any blank rows.

        >>> tuple(TableParser('''
        ... Lorem ipsum   dolor sit   amet
        ... consectetur   adipiscing  totes elit
        ... ''').rows)       # doctest: +NORMALIZE_WHITESPACE
        (('Lorem ipsum', 'dolor sit',  'amet'),
         ('consectetur', 'adipiscing', 'totes elit'))
        """
        rows = map(lambda l, c: tuple(map(lambda s: l[s].strip(), c)),
                   self._lines, itertools.repeat(self._col_edges))
        return filter(all, rows)

    @property
    def values(self):
        """Parse all values linearly, sans any empty strings.

        >>> tuple(TableParser('''
        ... Lorem ipsum   dolor sit   amet
        ... consectetur   adipiscing  totes elit
        ... ''').values)    # doctest: +NORMALIZE_WHITESPACE
        ('Lorem ipsum', 'dolor sit',  'amet',
         'consectetur', 'adipiscing', 'totes elit')
        """
        values = itertools.starmap(
           lambda l, c: l[c].strip(),
           # it(('Lorem ipsum   dolor ...', slice[n]),
           #    ('Lorem ipsum   dolor ...', slice[n+1]), ...)
           itertools.product(self._lines, self._col_edges))
        return filter(bool, values)


class Translit:
    """Stash away our transliterators.

    >>> Translit.slugify('Ένα duo 3!')
    'ena-duo-3'
    >>> Translit.unaccent_lc('Ένα duo 3!')
    'ενα duo 3!'
    """

    # Create filenames and URL slugs from Greek (or any) text by
    # converting Greek to alphanumeric ASCII; downcasing the input; and
    # replacing any number of consecutive spaces with a hyphen
    slugify = icu.Transliterator.createFromRules('slugify', """
(.*) > &[^[:alnum:][:whitespace:]] any-remove(
    &any-lower(
        &Latin-ASCII(
            &el-Latin($1))));
:: Null;    # Backtrack
[:whitespace:]+ > \-;""").transliterate

    # Remove diacritics and downcase the input
    unaccent_lc = icu.Transliterator.createInstance(
        'NFKD; [:nonspacing mark:] any-remove; any-lower').transliterate


def ungarble_qh(text, _LATN2GREK=str.maketrans('’ABEZHIKMNOPTYXvo',
                                               'ΆΑΒΕΖΗΙΚΜΝΟΡΤΥΧνο')):
    """Ungarble question headings.

    This function will indiscriminately replace Latin characters with
    Greek lookalikes.
    """
    return text.translate(_LATN2GREK)


@lru_cache()
def decipher_name(name,
                  _MPS=[mp['name']['el']
                        for mp in db.mps.find(projection={'name.el': 1})]):
    """Pair a jumbled up name with a canonical name in the database.

    >>> decipher_name('Καπθαιηάο Αληξέαο')   # Jesus take the wheel
    'Καυκαλιάς Αντρέας'
    >>> decipher_name('') is None
    True
    """
    try:
        return min(((Levenshtein.hamming(name, i), i) for i in _MPS
                    if len(i) == len(name)))[1]
    except ValueError:
        return


class NameConverter:
    """Crudely convert names in the gen. and acc. cases to the nominative."""

    # Retrieve all canonical names from the database, normalising them in
    # the process: {'lastname firstname': 'Lastname Firstname', ...}
    _NAMES_NOM = {Translit.unaccent_lc(mp['other_name']): mp['name'] for mp
                  in itertools.chain(
        db.mps.aggregate([
            {'$project': {'name': '$name.el', 'other_name': '$name.el'}}]),
        db.mps.aggregate([
            {'$unwind': '$other_names'},
            {'$match': {'other_names.note': re.compile('el-Grek')}},
            {'$project': {
                'name': '$name.el', 'other_name': '$other_names.name'}}]))}

    _TRANSFORMS = {
        'fore': [(r'', r''), (r'$', r'ς'), (r'ου$', r'ος'), (r'ς$', r'')],
        'aft':  [(r'', r''), (r'$', r'ς'), (r'ου$', r'ος')]}

    def __init__(self, name):
        name = self._prepare(name)
        self._names = itertools.chain([name],
                                      self._permute(name, self._TRANSFORMS))

    @staticmethod
    def _prepare(name):
        """Clean up, normalise and tokenise a name."""
        orig_name = name
        name = ''.join(c for c in name if not c.isdigit())
        name = Translit.unaccent_lc(name)
        name = re.findall(r'\w+', name)

        # NameConverter can only handle two- and three-part names
        if not 1 < len(name) <= 3:
            raise ValueError("Incompatible name '{}'".format(orig_name))
        return name

    @staticmethod
    def _permute(name, transforms):
        """Apply all combinations of `transforms` to a name."""
        first, *middle, last = name
        for fore, aft in itertools.product(transforms['fore'],
                                           transforms['aft']):
            yield [re.sub(fore[0], fore[1], first),
                   *(middle and [re.sub(aft[0], aft[1], middle[0])]),
                   re.sub(aft[0], aft[1], last)]

    @property
    def names(self):
        """Reverse-concatenate all names in `self._names`, returning a set.

        >>> (NameConverter('Γιαννάκη Ομήρου').names ==
        ...  {'ομηρους γιαννακη', 'ομηρου γιαννακης', 'ομηρους γιαννακης',
        ...   'ομηρου γιαννακη', 'ομηρος γιαννακη', 'ομηρος γιαννακης'})
        True
        >>> (NameConverter('Ροδοθέας Σταυράκου').names ==
        ...  {'σταυρακου ροδοθεα', 'σταυρακου ροδοθεας'})
        True
        """
        return {' '.join(reversed(name)) for name in self._names}

    @classmethod
    @lru_cache()
    def find_match(cls, name):
        """Pair a declined name with a canonical name in the database.

        >>> NameConverter.find_match('Ρούλλας Μαυρονικόλα')
        'Μαυρονικόλα Ρούλα'
        >>> NameConverter.find_match('gibber ish') is None
        True
        >>> NameConverter.find_match('gibberish')  # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        ValueError: Incompatible name 'gibberish'
        >>> NameConverter.find_match('a b c d')    # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        ValueError: Incompatible name 'a b c d'
        """
        for norm_name in cls(name).names:
            if norm_name in cls._NAMES_NOM:
                return cls._NAMES_NOM[norm_name]


def parse_short_date(date_string):
    """Convert a slash-delimited 'short' date into an ISO date.

    >>> parse_short_date('3/52014')
    '2014-05-03'
    >>> parse_short_date('3/5/2014')
    '2014-05-03'
    >>> parse_short_date('03/05/2014')
    '2014-05-03'
    >>> parse_short_date('03/05\/2014')
    '2014-05-03'
    >>> parse_short_date('gibberish')       # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Unable to disassemble date in ...
    """
    date = re.search(r'(\d{1,2})/(\d{1,2})[\\/]{0,2}(\d{4})', date_string)
    try:
        return '{}-{:02d}-{:02d}'.format(*map(int, reversed(date.groups())))
    except AttributeError:
        raise ValueError(
            "Unable to disassemble date in '{}'".format(date_string)) from None


def parse_long_date(date_string, plenary=False):
    """Convert a 'long' date in Greek into an ISO date.

    >>> parse_long_date('3 Μαΐου 2014')
    '2014-05-03'
    >>> parse_long_date('03 Μαΐου 2014')
    '2014-05-03'
    >>> parse_long_date('03 μαιου 2014')
    '2014-05-03'
    >>> parse_long_date('Συμπληρωματική ημερήσια διάταξη'
    ...                  ' 40-11072013')    # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Unable to disassemble date in ...
    >>> parse_long_date('Συμπληρωματική ημερήσια διάταξη 40-11072013',
    ...                  plenary=True)
    '2013-07-11'
    >>> parse_long_date('03 05 2014')      # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Malformed month in date ...
    >>> parse_long_date('gibberish')       # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Unable to disassemble date in ...
    """
    PLENARY_EXCEPTIONS = {
        'Συμπληρωματική ημερήσια διάταξη 40-11072013': '2013-07-11',
        'Συμπληρωματική Η.Δ. 17ης Συνεδρίας - 12 12 2013': '2013-12-12'}
    if plenary and date_string in PLENARY_EXCEPTIONS:
        return PLENARY_EXCEPTIONS[date_string]

    MONTHS = dict(zip(map(Translit.unaccent_lc,
                          icu.DateFormatSymbols(icu.Locale('el')).getMonths()),
                      range(1, 13)))  # {'ιανουαριος': 1, ...}
    try:
        d, m, y = re.search(r'(\d{1,2})(?:ης?)? (\w+) (\d{4})',
                            date_string).groups()
    except AttributeError:
        raise ValueError(
            "Unable to disassemble date in '{}'".format(date_string)) from None
    try:
        return '{}-{:02d}-{:02d}'.format(
            *map(int, (y, MONTHS[Translit.unaccent_lc(m)], d)))
    except KeyError:
        raise ValueError(
            "Malformed month in date '{}'".format(date_string)) from None


def parse_transcript_date(date_string):
    """Extract dates and counters from transcript URLs.

    >>> parse_transcript_date('2013-01-02')
    (('2013-01-02', '2013-01-02'), True)
    >>> parse_transcript_date('2013-01-02-1')
    (('2013-01-02', '2013-01-02_1'), True)
    >>> parse_transcript_date('http://www2.parliament.cy/parliamentgr/008_01/'
    ...                        '008_02_IC/praktiko2013-12-30.pdf')
    (('2014-01-30', '2014-01-30'), True)
    >>> parse_transcript_date('gibberish')
    ('gibberish', False)
    """
    success = True

    EXCEPTIONS = {
        'http://www2.parliament.cy/parliamentgr/008_01/'
        '008_02_IC/praktiko2013-12-30.pdf': ('2014-01-30',)*2}
    if date_string in EXCEPTIONS:
        return EXCEPTIONS[date_string], success

    m = re.search(r'(\d{4}-\d{2}-\d{2})(?:-(\d))?', date_string.strip())
    try:
        dates = (m.group(1),
                 '_'.join(i for i in m.groups() if i is not None))
    except AttributeError:
        dates = date_string
        success = False
    return dates, success


def clean_spaces(text, medial_newlines=False):
    r"""Tidy up whitespace in strings.

    >>> clean_spaces('  dfsf\n   ds \n')
    'dfsf\n ds'
    >>> clean_spaces('  dfsf\n   ds \n', medial_newlines=True)
    'dfsf ds'
    """
    if medial_newlines:
        text = text.split()
    else:
        text = re.split(r'[  ]+', text.strip())
    return ' '.join(text)


def truncate_slug(slug, max_length=100, sep='-'):
    """Truncate a slug for `max_length`, but keep whole words intact.

    >>> truncate_slug(Translit.slugify('bir iki üç'),
    ...                max_length=2)    # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Initial component of slug ... is longer than max_length ...
    >>> truncate_slug(Translit.slugify('bir iki üç'),
    ...                max_length=6)
    'bir'
    >>> truncate_slug(Translit.slugify('bir iki üç'),
    ...                max_length=7)
    'bir-iki'
    >>> truncate_slug(Translit.slugify('bir iki üç'),
    ...                max_length=9)
    'bir-iki'
    >>> truncate_slug(Translit.slugify('bir iki üç'),
    ...                max_length=10)
    'bir-iki-uc'
    >>> truncate_slug(Translit.slugify('bir iki üç'))
    'bir-iki-uc'
    """
    orig_slug = slug

    while len(slug) > max_length:
        slug = slug.rpartition(sep)[0]
    if not slug:
        raise ValueError("Initial component of slug '{}' is longer than"
                         " max_length '{}'".format(orig_slug, max_length))
    return slug
