
"""Various stand-alone utilities for manipulating text."""

from collections import Counter
import datetime
import functools
import itertools
import re
import string
import subprocess
import tempfile
from urllib.parse import urldefrag

import icu
import jellyfish
import lxml.html

from scrapers import db
from scrapers.misc_utils import starfilter


def _text_from_sp(args, input_=None):
    return (subprocess.run(args, input=input_, stdout=subprocess.PIPE)
                      .stdout.decode())


def doc_to_text(buffer):
    """Convert a `.doc` from `buffer` to plain text."""
    return _text_from_sp(('antiword', '-w 0', '-'), buffer)


def docx_to_text(buffer):
    """Convert a `.docx` from `buffer` to plain text."""
    with tempfile.NamedTemporaryFile() as file:   # Pandoc requires the input be a file when it's a binary
        file.write(buffer)
        return _text_from_sp(('pandoc',
                              '--from=docx', '--to=plain', '--no-wrap',
                              file.name))


def pdf_to_text(buffer):
    """Parse a bytes object into a PDF and into text."""
    return _text_from_sp(('pdftotext', '-layout', '-', '-'), buffer)


def html_to_lxml(url, text, clean=False):
    """Parse plain-text HTML into an `lxml` tree."""
    if clean:
        text = _text_from_sp(('pandoc', '--from=html', '--to=html5'),
                             text.encode())
    html = lxml.html.document_fromstring(text)
    # Endless loops ahoy
    html.rewrite_links(lambda s: '' if urldefrag(s).url == url else s,
                       base_href=url)
    return html


class TableParser:
    """A tool for sifting through plain-text tables."""

    def __init__(self, text, max_cols=2):
        self._lines = self._split_lines(text)
        self._col_modes = self._calc_col_modes(max_cols)

    @staticmethod
    def _split_lines(text):
        r"""Split and remove trailing whitespace from all lines.

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
    def _find_cols(line):
        """The leading-edge indices of hypothetical columns within a line.

        >>> TableParser._find_cols('Lorem ipsum   dolor sit   amet')
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
        col_freq = functools.reduce(
            lambda c, v: c + Counter(self._find_cols(v)),
            self._lines, Counter())
        return tuple(sorted(dict(col_freq.most_common(max_cols-1))))

    @staticmethod
    def _adjust_col(line, col):
        """Shift the `col` to the left if it overlaps a letter."""
        part = line[:col]
        if part == line:   # If the trailing cell's empty, take a shortcut
            return col

        part = starfilter(lambda _, c: c in string.whitespace,
                          reversed(tuple(enumerate(part))))
        new_col, _ = next(part)
        return new_col

    @classmethod
    def _chop_line(cls, line, cols):
        """Split the `line` at `cols`.

        We assume the leftmost column is flush with the edge of the page
        'cause it's easier that way.  Misaligned columns are translated to
        the left.

        >>> TableParser._chop_line('Lorem ipsum   dolor sit   amet',
        ...                        (14, 26))
        ('Lorem ipsum', 'dolor sit', 'amet')
        >>> TableParser._chop_line('Lorem ipsum   dolor sit   amet',
        ...                        (16, 26))  # col '16' overlaps 'dolor'
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


def ungarble_qh(text, _LATN2GREK=str.maketrans('’AB∆EZHIKMNOPTYXvo',
                                               'ΆΑΒΔΕΖΗΙΚΜΝΟΡΤΥΧνο')):
    """Ungarble question headings.

    This function will indiscriminately replace Latin characters with
    Greek lookalikes.
    """
    return text.translate(_LATN2GREK)


class CanonicaliseName:

    NAMES = itertools.chain(
        db.mps.aggregate([{'$project': {'name': '$name.el',
                                        'other_name': '$name.el'}}]),
        db.mps.aggregate([{'$unwind': '$other_names'},
                          {'$match': {'other_names.note': re.compile('el-Grek')
                                      }},
                          {'$project': {'name': '$name.el',
                                        'other_name': '$other_names.name'}}]))
    NAMES = {mp['other_name']: mp['name'] for mp in NAMES}

    # {'lastname firstname': 'Lastname Firstname', ...}
    NAMES_NORM = {translit_unaccent_lc(k): v for k, v in NAMES.items()}

    # ((fore_1.sub, aft_1.sub), (fore_1.sub, aft_2.sub), ...,
    #  (fore_2.sub, aft_1.sub), ...)
    TRANSFORMS = ([(r'', ''), (r'$', 'ς'), (r'ου$', 'ος'), (r'ς$', '')],  # fore
                  [(r'', ''), (r'$', 'ς'), (r'ου$', 'ος')])               # aft
    TRANSFORMS = itertools.product(*([(re.compile(p), r) for p, r in v]
                                     for v in TRANSFORMS))
    TRANSFORMS = tuple((functools.partial(fore[0].sub, fore[1]),
                        functools.partial(aft[0].sub, aft[1]))
                       for fore, aft in TRANSFORMS)

    @staticmethod
    def _prepare_declined(name):
        """Clean up, normalise and tokenise the `name`."""
        name = ''.join(c for c in name if not c.isdigit())
        name = translit_unaccent_lc(name)
        return re.findall(r'\w+', name)

    @staticmethod
    def _permute_declined(name, transforms):
        r"""Apply all of the `transforms` to a `name`, returning a set.

        >>> (CanonicaliseName._permute_declined(
        ...      CanonicaliseName._prepare_declined('Γιαννάκη Ομήρου'),
        ...      CanonicaliseName.TRANSFORMS) ==
        ...  {'ομηρους γιαννακη', 'ομηρου γιαννακης', 'ομηρους γιαννακης',
        ...   'ομηρου γιαννακη', 'ομηρος γιαννακη', 'ομηρος γιαννακης'})
        True
        """
        first, *middle, last = name
        names = ((fore(first), *(middle and (aft(middle[0]),)), aft(last))
                 for fore, aft in transforms)
        names = {' '.join(reversed(name)) for name in names}
        return names

    @classmethod
    @functools.lru_cache(maxsize=None)
    def from_declined(cls, name_):
        """Pair a declined name with a canonical name in the database.

        >>> CanonicaliseName.from_declined('Ρούλλας Μαυρονικόλα')
        'Μαυρονικόλα Ρούλα'
        >>> CanonicaliseName.from_declined('gibber ish') is None
        True
        >>> CanonicaliseName.from_declined('gibberish')  # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        ValueError: Too few or too many tokens in 'gibberish'
        >>> CanonicaliseName.from_declined('a b c d')    # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        ValueError: Too few or too many tokens in 'a b c d'
        """
        name = cls._prepare_declined(name_)
        if not 1 < len(name) <= 3:
            raise ValueError('Too few or too many tokens in {!r}'.format(name_))

        match = cls._permute_declined(name, cls.TRANSFORMS) & \
            cls.NAMES_NORM.keys()
        if match:
            return cls.NAMES_NORM[match.pop()]

    @classmethod
    @functools.lru_cache(maxsize=None)
    def from_garbled(cls, name):
        """Pair a jumbled up MP name with a canonical name in the database.

        >>> CanonicaliseName.from_garbled('Καπθαιηάο Αληξέαο')   # Jesus take the wheel
        'Καυκαλιάς Αντρέας'
        >>> CanonicaliseName.from_garbled('') is None
        True
        """
        if not name:
            return
        try:
            return cls.NAMES[name]
        except KeyError:
            try:
                _, new_name = min((jellyfish.hamming_distance(name, v), v)
                                  for v in cls.NAMES.values()
                                  if len(v) == len(name))
                return new_name
            except ValueError:
                return


def parse_short_date(
        date_string,
        _RE_DATE=re.compile(r'(\d{1,2})/(\d{1,2})[\\/]{0,2}(\d{4})')):
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
    date = _RE_DATE.search(date_string)
    try:
        return '{}-{:02d}-{:02d}'.format(*map(int, reversed(date.groups())))
    except AttributeError:
        raise ValueError(
            'Unable to disassemble date in {!r}'.format(date_string)) from None


def parse_long_date(
        date_string, plenary=False,
        _RE_DATE=re.compile(r'(\d{1,2})(?:[αηή]ς?)? (\w+) (\d{4})'),
        _EL_MONTHS=dict(zip(map(translit_unaccent_lc,
                                icu.DateFormatSymbols(icu.Locale('el'))
                                   .getMonths()), range(1, 13)))
         ):
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

    try:
        d, m, y = _RE_DATE.search(date_string).groups()
    except AttributeError:
        raise ValueError(
            'Unable to disassemble date in {!r}'.format(date_string)) from None
    try:
        return '{}-{:02d}-{:02d}'.format(
            *map(int, (y, _EL_MONTHS[translit_unaccent_lc(m)], d)))
    except KeyError:
        raise ValueError(
            'Malformed month in date {!r}'.format(date_string)) from None


def date2dato(date):
    """Convert a clear-text ISO date into a `datetime.date` object."""
    return datetime.datetime.strptime(date, '%Y-%m-%d').date()


def clean_spaces(text, medial_newlines=False,
                 _RE_WHITESPACE=re.compile(r'[  ]+')):
    r"""Tidy up whitespace in strings.

    >>> clean_spaces('  dfsf\n   ds \n')
    'dfsf\n ds'
    >>> clean_spaces('  dfsf\n   ds \n', medial_newlines=True)
    'dfsf ds'
    """
    if medial_newlines:
        text = text.split()
    else:
        text = _RE_WHITESPACE.split(text.strip())
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
