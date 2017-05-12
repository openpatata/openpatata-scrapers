
"""Various stand-alone utilities for manipulating text."""

from collections import Counter
import datetime
import functools as ft
import itertools as it
from pathlib import Path
import re
import string
import subprocess
import tempfile
from urllib.parse import urldefrag

import icu
import lxml.etree
import lxml.html

from .misc_utils import starfilter


def _text_from_sp(args, input_=None):
    return (subprocess.run(args, input=input_, stdout=subprocess.PIPE)
                      .stdout.decode())


def doc_to_text(buffer):
    """Convert a `.doc` from `buffer` to plain text."""
    return _text_from_sp(('antiword', '-w 0', '-'), buffer)


def docx_to_json(buffer):
    """Convert a `.docx` from `buffer` to a pandoc AST."""
    with tempfile.NamedTemporaryFile() as file:   # Pandoc requires the input be a file when it's a binary
        file.write(buffer)
        return _text_from_sp(('pandoc', '--from=docx', '--to=json', file.name))


def pandoc_json_to(json, format_):
    """Convert from pandoc JSON to any other format accepted by pandoc."""
    return _text_from_sp(('pandoc', '--from=json', '--to='+format_),
                         json.encode())


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

    def __init__(self, text, columns=None, confidence=0.9):
        self._columns = columns
        self._confidence = confidence
        self._lines = self._split_lines(text)

    @staticmethod
    def _split_lines(text):
        r"""Split and remove trailing whitespace from all lines.

        Additionally, remove blank lines.

        >>> TableParser('''
        ... Lorem ipsum   dolor sit    amet
        ...
        ... consectetur   adipiscing   totes elit
        ... ''')._lines         # doctest: +NORMALIZE_WHITESPACE
        ('Lorem ipsum   dolor sit    amet',
         'consectetur   adipiscing   totes elit')
        """
        lines = (line.rstrip() for line in text.splitlines())
        return tuple(filter(None, lines))

    @staticmethod
    def _find_cols(line):
        """The leading-edge indices of hypothetical columns within a line.

        >>> TableParser._find_cols('Lorem ipsum   dolor sit    amet')
        (14, 27)
        """
        return tuple(m.end() for m in re.finditer(r'\s{2,}', line))

    def _calc_col_modes(self):
        r"""The most common column indices in the table.

        >>> TableParser('''
        ... Lorem ipsum   dolor sit    amet
        ... consectetur   adipiscing   totes elit
        ... ''')._calc_col_modes()
        (14, 27)
        """
        col_freq = ft.reduce(lambda c, v: c + Counter(self._find_cols(v)),
                             self._lines, Counter())
        if not col_freq:
            return ()

        if self._columns:
            return tuple(sorted(i for i, _ in
                                col_freq.most_common(self._columns-1)))
        else:
            (_, most_common), = col_freq.most_common(1)
            return tuple(sorted(k for k, v in col_freq.items()
                                if v/most_common >= self._confidence))

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

        >>> TableParser._chop_line('Lorem ipsum   dolor sit    amet',
        ...                        (14, 27))
        ('Lorem ipsum', 'dolor sit', 'amet')
        >>> TableParser._chop_line('Lorem ipsum   dolor sit    amet',
        ...                        (16, 27))  # col '16' overlaps 'dolor'
        ('Lorem ipsum', 'dolor sit', 'amet')
        """
        cols = it.tee(it.chain((0,), map(cls._adjust_col,
                                         it.repeat(line), cols)))
        cols = it.starmap(slice, it.zip_longest(cols[0],
                                                it.islice(cols[1], 1, None)))
        return tuple(map(lambda line, slice_: line[slice_].strip(),
                         it.repeat(line), cols))

    @property
    def rows(self):
        r"""Produce a cols within a row matrix, sans any blank rows.

        >>> TableParser('''
        ... Lorem ipsum   dolor sit    amet
        ... consectetur   adipiscing   totes elit
        ... ''').rows       # doctest: +NORMALIZE_WHITESPACE
        (('Lorem ipsum', 'dolor sit',  'amet'),
         ('consectetur', 'adipiscing', 'totes elit'))
        """
        rows = map(self._chop_line,
                   self._lines, it.repeat(self._calc_col_modes()))
        return tuple(filter(any, rows))

    @property
    def values(self):
        r"""Parse all values linearly, sans any empty strings.

        >>> TableParser('''
        ... Lorem ipsum   dolor sit    amet
        ... consectetur   adipiscing   totes elit
        ... ''').values     # doctest: +NORMALIZE_WHITESPACE
        ('Lorem ipsum', 'dolor sit',  'amet',
         'consectetur', 'adipiscing', 'totes elit')
        """
        values = it.chain.from_iterable(self.rows)
        return tuple(filter(None, values))


class _Translit:
    """A space to keep ICU transliterators.

    1. Create filenames and URL slugs from Greek (or any) text by
       converting Greek to lowercase, alphanumeric ASCII and
       replacing any number of consecutive spaces with a hyphen.

       >>> translit_slugify('Ένα duo 3!')
       'ena-duo-3'

    2. Remove diacritics from and downcase the input.

       >>> translit_unaccent_lc('Ένα duo 3!')
       'ενα duo 3!'

    3. Romanise a Greek name.

       >>> translit_elGrek2Latn('Ομήρου Γιαννάκης')
       'Omirou Giannakis'

    4. Transliterate a Greek name into Turkish.

       >>> translit_el2tr('Ομήρου Γιαννάκης')
       'Omíru Yiannákis'
    """

    translit_slugify = icu.Transliterator.createFromRules(
        'translit_slugify',
        r'''(.*) > &[^[:alnum:][:whitespace:]-] any-remove(
                &any-lower(
                    &Latin-ASCII(
                        &el-Latin($1))));
            :: Null;    # Backtrack
            [[:whitespace:]-]+ > \-;
         ''').transliterate

    translit_unaccent_lc = icu.Transliterator.createInstance(
        'NFKD; [:nonspacing mark:] any-remove; any-lower').transliterate

    translit_elGrek2Latn = icu.Transliterator.createInstance(
        'Greek-Latin/UNGEGN; Latin-ASCII').transliterate

    with (Path(__file__).parent/'data'/'translit_Greek-Turkish.xml').open('rb') \
            as _file:
        _xml = lxml.etree.fromstring(_file.read())
    translit_el2tr = icu.Transliterator.createFromRules(
        'translit_el2tr',
        '\n'.join(e.text for e in _xml.xpath('//tRule'))).transliterate

translit_slugify = _Translit.translit_slugify
translit_unaccent_lc = _Translit.translit_unaccent_lc
translit_elGrek2Latn = _Translit.translit_elGrek2Latn
translit_el2tr = _Translit.translit_el2tr


def ungarble_qh(text, _LATN2GREK=str.maketrans('’AB∆EZHIKMNOPTYXvo',
                                               'ΆΑΒΔΕΖΗΙΚΜΝΟΡΤΥΧνο')):
    """Ungarble question headings.

    This function will indiscriminately replace Latin characters with
    Greek lookalikes.
    """
    return text.translate(_LATN2GREK)


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
        raise ValueError(f'Unable to disassemble date in {date_string!r}') \
            from None


_EL_MONTHS = dict(zip(
    map(translit_unaccent_lc, icu.DateFormatSymbols(icu.Locale('el')).getMonths()),
    range(1, 13)))

def parse_long_date(date_string, plenary=False,
                    _RE_DATE=re.compile(r'(\d{1,2})(?:[αηή]ς?)? (\w+) (\d{4})'),
                    _EL_MONTHS=_EL_MONTHS):
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
        raise ValueError(f'Unable to disassemble date in {date_string!r}') \
            from None
    try:
        return '{}-{:02d}-{:02d}'.format(
            *map(int, (y, _EL_MONTHS[translit_unaccent_lc(m)], d)))
    except KeyError:
        raise ValueError(f'Malformed month in date {date_string!r}') from None


def date2dato(date):
    """Convert a clear-text ISO date into a `datetime.date` object."""
    try:
        return datetime.datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')


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
        raise ValueError(f'Initial component of slug {orig_slug!r} is longer than'
                         f' `max_length` {max_length!r}')
    return slug


def apply_subs(orig_string, subs):
    """Apply a two-tuple list of substitutions to `orig_string`."""
    return ft.reduce(lambda s, sub: s.replace(*sub), subs, orig_string)
