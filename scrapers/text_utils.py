
"""Various stand-alone utilities for manipulating text."""

from collections import Counter, namedtuple
import datetime
import functools
import itertools
import re
import string
import subprocess
from urllib.parse import urldefrag

import icu
import Levenshtein
import lxml.html
import pypandoc

from scrapers import db


def pdf2text(stream):
    """Parse a bytes object into a PDF and into text."""
    text = subprocess.run(('pdftotext', '-layout', '-', '-'),
                          input=stream, stdout=subprocess.PIPE)
    text = text.stdout.decode(encoding='utf-8')
    return text


def parse_html(url, text, clean=False):
    """Parse HTML into an `lxml` tree."""
    if clean:
        text = pypandoc.convert(text, 'html5', format='html')
    html = lxml.html.document_fromstring(text)
    # Endless loops ahoy
    html.rewrite_links(lambda s: None if urldefrag(s).url == url else s,
                       base_href=url)
    return html


class TableParser:
    """A tool for sifting through plain-text tables."""

    def __init__(self, text, max_cols=2):
        self._lines = self._split_lines(text)
        self._col_modes = self._calc_col_modes(max_cols)

    @staticmethod
    def _split_lines(text):
        r"""Split and remove leading whitespace from all lines.

        Additionally, remove blank lines.

        >>> TableParser('''
        ...
        ... Lorem ipsum   dolor sit   amet
        ...
        ... consectetur   adipiscing  totes elit
        ...
        ... ''')._lines         # doctest: +NORMALIZE_WHITESPACE
        ('Lorem ipsum   dolor sit   amet',
         'consectetur   adipiscing  totes elit')
        """
        lines = (line.rstrip() for line in text.splitlines())
        return tuple(filter(None, lines))

    @staticmethod
    def _cols_of(line):
        """The leading-edge indices of hypothetical columns within a line.

        >>> TableParser._cols_of('Lorem ipsum   dolor sit   amet')
        (14, 26)
        """
        return tuple(m.end() for m in re.finditer(r'\s{2,}', line))

    def _calc_col_modes(self, max_cols):
        r"""The `self._max_cols` most common column indices in the table.

        >>> TableParser('''\
        ... Lorem ipsum   dolor sit   amet
        ... consectetur   adipiscing  totes elit
        ... ''', 3)._col_modes
        (14, 26)
        """
        col_freq = functools.reduce(lambda c, v: c + Counter(self._cols_of(v)),
                                    self._lines, Counter())
        return tuple(sorted(dict(col_freq.most_common(max_cols-1))))

    @staticmethod
    def _adjust_col(line, col):
        """Shift the `col` left if it overlaps a letter."""
        part = line[:col]
        if part == line:   # If the trailing cell's empty, take a shortcut
            return col

        part = itertools.dropwhile(lambda v: v[1] not in string.whitespace,
                                   reversed(tuple(enumerate(part))))
        return next(part)[0]

    @classmethod
    def _chop_line(cls, line, cols):
        """Split the `line` at `cols`.

        We assume the leftmost column is flush with the edge of the page
        'cause it's easier that way.  Misaligned columns are translated to
        the left.

        >>> TableParser('')._chop_line('Lorem ipsum   dolor sit   amet',
        ...                            (14, 26))
        ('Lorem ipsum', 'dolor sit', 'amet')
        >>> TableParser('')._chop_line('Lorem ipsum   dolor sit   amet',
        ...                            (16, 26))  # col '16' overlaps 'dolor'
        ('Lorem ipsum', 'dolor sit', 'amet')
        """
        cols = tuple(itertools.chain((0,), map(cls._adjust_col,
                                               itertools.repeat(line), cols)))
        cols = itertools.starmap(slice, itertools.zip_longest(cols, cols[1:]))
        return tuple(map(lambda line, slice_: line[slice_].strip(),
                         itertools.repeat(line), cols))

    @property
    def rows(self):
        r"""Produce a cols within a row matrix, sans any blank rows.

        >>> TableParser('''\
        ... Lorem ipsum   dolor sit   amet
        ... consectetur   adipiscing  totes elit
        ... ''', 3).rows       # doctest: +NORMALIZE_WHITESPACE
        (('Lorem ipsum', 'dolor sit',  'amet'),
         ('consectetur', 'adipiscing', 'totes elit'))
        """
        rows = map(self._chop_line,
                   self._lines, itertools.repeat(self._col_modes))
        return tuple(filter(any, rows))

    @property
    def values(self):
        r"""Parse all values linearly, sans any empty strings.

        >>> TableParser('''\
        ... Lorem ipsum   dolor sit   amet
        ... consectetur   adipiscing  totes elit
        ... ''', 3).values     # doctest: +NORMALIZE_WHITESPACE
        ('Lorem ipsum', 'dolor sit',  'amet',
         'consectetur', 'adipiscing', 'totes elit')
        """
        values = itertools.chain.from_iterable(self.rows)
        return tuple(filter(None, values))


def translit_slugify(s):
    """Slugify a given string.

    Create filenames and URL slugs from Greek (or any) text by
    converting Greek to alphanumeric ASCII; downcasing the input; and
    replacing any number of consecutive spaces with a hyphen.

    >>> translit_slugify('Ένα duo 3!')
    'ena-duo-3'
    """
    return icu.Transliterator.createFromRules('slugify', r"""
        (.*) > &[^[:alnum:][:whitespace:]] any-remove(
            &any-lower(
                &Latin-ASCII(
                    &el-Latin($1))));
        :: Null;    # Backtrack
        [:whitespace:]+ > \-;""").transliterate(s)


def translit_unaccent_lc(s):
    """Remove diacritics and downcase the input.

    >>> translit_unaccent_lc('Ένα duo 3!')
    'ενα duo 3!'
    """
    return icu.Transliterator.createInstance(
        'NFKD; [:nonspacing mark:] any-remove; any-lower').transliterate(s)


def ungarble_qh(text, _LATN2GREK=str.maketrans('’ABEZHIKMNOPTYXvo',
                                               'ΆΑΒΕΖΗΙΚΜΝΟΡΤΥΧνο')):
    """Ungarble question headings.

    This function will indiscriminately replace Latin characters with
    Greek lookalikes.
    """
    return text.translate(_LATN2GREK)


@functools.lru_cache()
def decipher_name(name,
                  _MPS=[mp['name']['el']
                        for mp in db.mps.find(projection={'name.el': 1})]):
    """Pair a jumbled up MP name with a canonical name in the database.

    >>> decipher_name('Καπθαιηάο Αληξέαο')   # Jesus take the wheel
    'Καυκαλιάς Αντρέας'
    >>> decipher_name('') is None
    True
    """
    try:
        return min((Levenshtein.hamming(name, i), i) for i in _MPS
                   if len(i) == len(name))[1]
    except ValueError:
        return


class _NameConverter:
    """Crudely convert names in the gen. and acc. cases to the nominative."""

    # Retrieve all canonical names from the database, normalising them in
    # the process: {'lastname firstname': 'Lastname Firstname', ...}
    _NAMES_NOM = itertools.chain(
        db.mps.aggregate([{'$project': {'name': '$name.el',
                                        'other_name': '$name.el'}}]),
        db.mps.aggregate([{'$unwind': '$other_names'},
                          {'$match': {'other_names.note': re.compile('el-Grek')
                                      }},
                          {'$project': {'name': '$name.el',
                                        'other_name': '$other_names.name'}}]))
    _NAMES_NOM = {translit_unaccent_lc(mp['other_name']): mp['name']
                  for mp in _NAMES_NOM}

    _TRANSFORMS = itertools.product(
        [(r'', ''), (r'$', 'ς'), (r'ου$', 'ος'), (r'ς$', '')],   # fore
        [(r'', ''), (r'$', 'ς'), (r'ου$', 'ος')])                # aft
    _TRANSFORMS = tuple(itertools.starmap(
        lambda fore, aft: (lambda s: re.sub(fore[0], fore[1], s),
                           lambda s: re.sub(aft[0],  aft[1], s)), _TRANSFORMS))

    @staticmethod
    def _prepare(name):
        """Clean up, normalise and tokenise the `name`."""
        name = ''.join(c for c in name if not c.isdigit())
        name = translit_unaccent_lc(name)
        return re.findall(r'\w+', name)

    @staticmethod
    def _permute(name, transforms):
        r"""Apply all of the `transforms` to a `name`, returning a set.

        >>> _NameConverter._permute(_NameConverter._prepare('Γιαννάκη Ομήρου'),
        ...                         _NameConverter._TRANSFORMS) == \
        ... {'ομηρους γιαννακη', 'ομηρου γιαννακης', 'ομηρους γιαννακης',
        ...  'ομηρου γιαννακη', 'ομηρος γιαννακη', 'ομηρος γιαννακης'}
        True
        """
        first, *middle, last = name
        names = ((fore(first), *(middle and (aft(middle[0]),)), aft(last))
                 for fore, aft in transforms)
        return {' '.join(reversed(name)) for name in names}

    @classmethod
    @functools.lru_cache()
    def find_match(cls, name):
        """Pair a declined name with a canonical name in the database.

        >>> _NameConverter.find_match('Ρούλλας Μαυρονικόλα')
        'Μαυρονικόλα Ρούλα'
        >>> _NameConverter.find_match('gibber ish') is None
        True
        >>> _NameConverter.find_match('gibberish')  # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        ValueError: Incompatible name 'gibberish'
        >>> _NameConverter.find_match('a b c d')    # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        ValueError: Incompatible name 'a b c d'
        """
        orig_name, name = name, cls._prepare(name)
        # NameConverter can only handle two- and three-part names
        if not 1 < len(name) <= 3:
            raise ValueError('Incompatible name {!r}'.format(orig_name))

        names = cls._permute(name, cls._TRANSFORMS)
        match = names & cls._NAMES_NOM.keys()
        if match:
            return cls._NAMES_NOM[match.pop()]

match_declined_name = _NameConverter.find_match


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
            'Unable to disassemble date in {!r}'.format(date_string)) from None


def parse_long_date(date_string, plenary=False):
    """Convert a 'long' date in Greek into an ISO date.

    >>> parse_long_date('3 Μαΐου 2014')
    '2014-05-03'
    >>> parse_long_date('03 Μαΐου 2014')
    '2014-05-03'
    >>> parse_long_date('03 μαιου 2014')
    '2014-05-03'
    >>> parse_long_date('Συμπληρωματική ημερήσια διάταξη'
    ...                 ' 40-11072013')    # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Unable to disassemble date in ...
    >>> parse_long_date('Συμπληρωματική ημερήσια διάταξη 40-11072013',
    ...                 plenary=True)
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

    MONTHS = dict(zip(map(translit_unaccent_lc,
                          icu.DateFormatSymbols(icu.Locale('el')).getMonths()),
                      range(1, 13)))  # {'ιανουαριος': 1, ...}
    try:
        d, m, y = re.search(r'(\d{1,2})(?:ης?)? (\w+) (\d{4})',
                            date_string).groups()
    except AttributeError:
        raise ValueError(
            'Unable to disassemble date in {!r}'.format(date_string)) from None
    try:
        return '{}-{:02d}-{:02d}'.format(
            *map(int, (y, MONTHS[translit_unaccent_lc(m)], d)))
    except KeyError:
        raise ValueError(
            'Malformed month in date {!r}'.format(date_string)) from None


def parse_transcript_date(date_string):
    """Extract dates and counters from transcript URLs.

    >>> parse_transcript_date('2013-01-02')
    (Date(date='2013-01-02', slug='2013-01-02'), True)
    >>> parse_transcript_date('2013-01-02-2')
    (Date(date='2013-01-02', slug='2013-01-02_2'), True)
    >>> parse_transcript_date('http://www2.parliament.cy/parliamentgr/008_01/'
    ...                       '008_02_IC/praktiko2013-12-30.pdf')
    (Date(date='2014-01-30', slug='2014-01-30'), True)
    >>> parse_transcript_date('gibberish')
    ('gibberish', False)
    """
    Date = namedtuple('Date', 'date, slug')
    success = True

    EXCEPTIONS = {
        'http://www2.parliament.cy/parliamentgr/008_01/'
        '008_02_IC/praktiko2013-12-30.pdf': ('2014-01-30',)*2}
    if date_string in EXCEPTIONS:
        return Date(*EXCEPTIONS[date_string]), success

    m = re.search(r'(\d{4}-\d{2}-\d{2})(?:-(\d))?', date_string.strip())
    try:
        dates = Date(m.group(1), '_'.join(filter(None, m.groups())))
    except AttributeError:
        dates = date_string
        success = False
    return dates, success


def date2dato(date_string):
    """Convert a clear-text ISO date into a `datetime.date` object."""
    return datetime.datetime.strptime(date_string, '%Y-%m-%d').date()


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

    >>> truncate_slug(translit_slugify('bir iki üç'),
    ...               max_length=2)    # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: Initial component of slug ... is longer than `max_length` ...
    >>> truncate_slug(translit_slugify('bir iki üç'),
    ...               max_length=6)
    'bir'
    >>> truncate_slug(translit_slugify('bir iki üç'),
    ...               max_length=7)
    'bir-iki'
    >>> truncate_slug(translit_slugify('bir iki üç'),
    ...               max_length=9)
    'bir-iki'
    >>> truncate_slug(translit_slugify('bir iki üç'),
    ...               max_length=10)
    'bir-iki-uc'
    >>> truncate_slug(translit_slugify('bir iki üç'))
    'bir-iki-uc'
    """
    orig_slug = slug

    while len(slug) > max_length:
        slug, _, _ = slug.rpartition(sep)
    if not slug:
        raise ValueError('Initial component of slug {!r} is longer than'
                         ' `max_length` {!r}'.format(orig_slug, max_length))
    return slug


def apply_subs(orig_string, subs):
    """Apply a two-tuple list of substitutions to `orig_string`."""
    return functools.reduce(lambda s, sub: s.replace(*sub), subs, orig_string)
