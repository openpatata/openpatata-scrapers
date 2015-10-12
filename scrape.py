
"""Cypriot-parliament scraper.

An XPath and regex soup in a misguided attempt to parse unstructured
structures. This module crawls Parliament's website to collect the
little that is made available and updates existing records on the
openpatata-data repo.

Usage:
  scrape.py init
  scrape.py run <task> [--debug]
  scrape.py dump <collection> <path_on_disk>

Options:
  -h --help  Show this screen.
"""

import asyncio
from collections import OrderedDict as od
from functools import lru_cache
from itertools import chain, product
import logging
import os
import re
from urllib.parse import urldefrag

import aiohttp
from docopt import docopt
import lxml.html
import icu
import pymongo
import pypandoc

from scrapers import records
from scrapers.in_out import dump_collection, populate_db

TASKS = ...
db = pymongo.MongoClient()['openpatata-data']


class Requester:
    """Limit concurrent connections to 15 to avoid flooding the server."""

    semaphore = asyncio.Semaphore(15)

    @classmethod
    async def get_text(cls, url, form_data=None, request_method='get'):
        """Postpone the request until a slot has become available."""
        with await cls.semaphore:
            response = await aiohttp.request(request_method, url,
                                             data=form_data)
            return await response.text()


class Translit:
    """Stash away our transliterators."""

    # Remove diacritics and downcase the input
    rmd_dc = icu.Transliterator.createInstance(
        'NFKD; [:nonspacing mark:] any-remove; any-lower',
        icu.UTransDirection.FORWARD).transliterate


class GlyphNorm:
    """Lazily replace Latin characters within Greek lexemes with their
    visual Greek equivalents. Parliament keep mixing them up. Somehow.
    """

    _LATN2GREK_c = ('ABEZHIKMNOPTYXvo',
                    'ΑΒΕΖΗΙΚΜΝΟΡΤΥΧνο')
    _LATN2GREK_o = str.maketrans(*_LATN2GREK_c)

    # icu.LocaleData.getExemplarSet([[0, ]1])
    # (Yes, really, that _is_ the function's signature: a second positional
    # argument shifts the first one to the right.)
    #
    # (0: options) -> icu.USET_IGNORE_SPACE = 1
    #                 icu.USET_CASE_INSENSITIVE = 2
    #                 icu.USET_ADD_CASE_MAPPINGS = 4
    # Mystical transformations. See
    # https://ssl.icu-project.org/apiref/icu4c/uset_8h.html for the juicy
    # details.
    #
    # (1: extype)  -> icu.ULocaleDataExemplarSetType.ES_STANDARD = 0
    #                 icu.ULocaleDataExemplarSetType.ES_AUXILIARY = 1
    #                 icu.ULocaleDataExemplarSetType.ES_INDEX = 2
    #                 !ULOCDATA_ES_PUNCTUATION = 3
    #                 !ULOCDATA_ES_COUNT = 4
    # The icu4c PUNCTUATION and COUNT bitmasks are not exposed (as constants)
    # in pyicu; use their corresponding ints.
    # See http://cldr.unicode.org/translation/characters for an explanation of
    # each of `extype`, and
    # https://ssl.icu-project.org/apiref/icu4c/ulocdata_8h_source.html#l00041
    # for their values, if they're ever to change.
    #
    # `getExemplarSet` returns an instance of `icu.UnicodeSet`, which can be
    # cast to a string to produce a matching pattern, in the form of a range
    # (e.g. `[a-z]`); or to a list to produce a list of all characters
    # (codepoints) the pattern encapsulates. Alternatively, a `UnicodeSet`
    # can be consumed by an `icu.UnicodeSetIterator`.
    _EL_GLYPHS = icu.LocaleData('el').getExemplarSet(2, 0)
    _EN_GLYPHS = icu.UnicodeSet().addAll(_LATN2GREK_c[0])

    # Match one or more of _LATN2GREK_c[0] if preceded or followed by any
    # Greek character
    _S_PATTERN = re.compile(r'(?:(?<={el})({en}+)|({en}+)(?={el}))'.format(
        el=_EL_GLYPHS, en=_EN_GLYPHS))

    @classmethod
    def ungarble(cls, garbled_string):
        """Ungarble a given string."""
        return cls._S_PATTERN.sub(
            lambda m: (m.group(1) or m.group(2)).translate(cls._LATN2GREK_o),
            garbled_string)


class NameConverter:
    """Transform an MP's name in various ways.

    MPs' names in the headings of questions are given in the genitive or
    accusative case. This class hosts a set of rules to change the
    endings of forenames and surnames, depending on gender, so that at
    least one of possible combinations declines the full name in the
    nominative case.
    """

    # {'lastname firstname': 'Lastname Firstname', ...}
    _NAMES_NOM = {
        Translit.rmd_dc(mp['other_name']): mp['name']
        for mp in chain(
            db.mps.aggregate([
                {'$project': {
                    'name': '$name.el', 'other_name': '$name.el'}}]),
            db.mps.aggregate([
                {'$unwind': '$other_names'},
                {'$match': {'other_names.note': {'$in': [
                    'Alternative spelling (el-Grek)',
                    'Short form (el-Grek)']}}},
                {'$project': {
                    'name': '$name.el', 'other_name': '$other_names.name'}}]))}

    _TRANSFORMS_F = (
        # before     after
        [(r'ς$',     r'')],
        [(r'',       r'')])
    _TRANSFORMS_M = (
        [(r'',       r''),
         (r'$',      r'ς'),
         (r'ο[υύ]$', r'ος')],
        [(r'',       r''),
         (r'$',      r'ς'),
         (r'ο[υύ]$', r'ος')])

    _PP2T = {
        'τη':  _TRANSFORMS_F, 'το':  _TRANSFORMS_M,
        'της': _TRANSFORMS_F, 'του': _TRANSFORMS_M}

    def __init__(self, name, pp):
        self._orig_name = re.sub(r'\d', r'',
                                 re.sub(r'’', r'Ά', name))
        self._orig_name = re.findall('\w+', self._orig_name)
        if len(self._orig_name) > 3:
            raise ValueError("Malformed name: '{}'".format(name))

        self._names = [self._orig_name]  # [['Firstname', 'Lastname'], ...]
        self._compute(self._PP2T[pp])

    def _compute(self, transforms):
        for fore, aft in product(transforms[0], transforms[1]):
            first, *middle, surname = self._orig_name
            self._names.append([
                re.sub(fore[0], fore[1], first),
                *(middle and [re.sub(aft[0], aft[1], middle[0])]),
                re.sub(aft[0], aft[1], surname)])

    @property
    def names(self):  # -> {'lastname firstname', ...}
        """Return a set of all computed names, each having been
        reversed, stripped of its diacritics and downcased.
        """
        return {Translit.rmd_dc(' '.join(reversed([part for part in name])))
                for name in self._names}

    @classmethod
    @lru_cache()
    def find_match(cls, name, pp):  # -> 'Lastname Firstname' or None
        """Pair a declined name with a canonical name in the database."""
        for norm_name in cls(name, pp).names:
            if norm_name in cls._NAMES_NOM:
                return cls._NAMES_NOM[norm_name]


def _exec_blocking(func, *args):
    """Execute blocking operations independently of the async loop."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, func, *args)


def _parse_long_date(date_string, plenary=False):  # -> 'YYYY-MM-DD'
    """Convert a long date in Greek into an ISO date."""
    PLENARY_EXCEPTIONS = {
        'Συμπληρωματική ημερήσια διάταξη 40-11072013': '2013-07-11',
        'Συμπληρωματική Η.Δ. 17ης Συνεδρίας - 12 12 2013': '2013-12-12'}
    if plenary and date_string in PLENARY_EXCEPTIONS:
        return PLENARY_EXCEPTIONS[date_string]

    MONTHS = dict(zip(map(Translit.rmd_dc,
                          icu.DateFormatSymbols(icu.Locale('el')).getMonths()),
                      range(1, 13)))  # {'ιανουαριος': 1, ...}
    try:
        d, m, y = re.search(r'(\d{1,2})(?:ης?)? (\w+) (\d{4})',
                            date_string).groups()
    except AttributeError:
        raise ValueError("Unable to parse date '{}'".format(date_string))
    try:
        return '{}-{:02d}-{:02d}'.format(
            *map(int, (y, MONTHS[Translit.rmd_dc(m)], d)))
    except KeyError:
        raise ValueError("Malformed month in date '{}'".format(date_string))


def _parse_transcript_date(date_string):  # -> 'YYYY-MM-DD'
    """Extract dates from transcript URLs in the ISO format."""
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


def _clean_spaces(text):
    """Convert non-breaking spaces to regular spaces, reduce consecutive
    spaces down to one, and strip off leading and trailing whitespace.
    """
    return re.sub(r'[  ]+', ' ', text).strip()


def parse_agenda(url, text):
    """Create plenary records and bills from agendas."""
    html = lxml.html.document_fromstring(text)
    body_text = _clean_spaces(html.xpath('string(//div[@class="articleBox"])'))

    def _extract_parliament():
        try:
            return re.search(r'(\w+)[\'΄] ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ',
                             body_text).group(1)
        except AttributeError:
            logging.error("Could not extract parliamentary period"
                          " of '{}'".format(url))

    def _extract_session():
        try:
            return re.search(r'ΣΥΝΟΔΟΣ (\w+)[\'΄]', body_text).group(1)
        except AttributeError:
            logging.error("Could not extract session"
                          " of '{}'".format(url))

    def _extract_sitting():
        try:
            return int(re.search(r'(\d+)[ηή] ?συνεδρίαση',
                                 body_text).group(1))
        except AttributeError:
            logging.error("Could not extract sitting number"
                          " of '{}'".format(url))

    bills = []  # [records.Bill(), ...]
    plenary = records.PlenarySitting(
        date=_parse_long_date(_clean_spaces(html.xpath('string(//h1)')),
                              plenary=True),
        links=[od([('type', 'agenda'), ('url', url)])],
        parliament=_extract_parliament(),
        session=_extract_session(),
        sitting=_extract_sitting())

    for e in html.xpath('//div[@class="articleBox"]//tr/td[last()]'):
        try:
            title, *ext, id_ = (_clean_spaces(e.text_content())
                                for e in e.xpath('*[self::div or self::p]'))
        except ValueError:
            # Presumably a faux header; skip it
            continue
        else:
            id_ = [id_] + ext

        title = title.rstrip('.')
        id_ = (re.sub(r'[^0-9\.\-]', '', i).strip('.') for i in id_)
        for i in id_:
            try:
                doc_type = re.match(r'23\.(\d{2})', i).group(1)
            except AttributeError:
                continue
            if doc_type in {'04', '05'}:
                plenary['agenda']['debate'].append(i)
            else:
                plenary['agenda']['legislative_work'].append(i)

                bill = records.Bill(**{'_filename': '{}.yaml'.format(i),
                                       'identifier': i, 'title': title})
                bills.append(bill)
            break
        else:
            logging.error("Could not extract document type"
                          " of '{}' in '{}'".format(title, url))

    # Version sitting filenames from oldest to newest; extraordinary
    # sittings come last
    sittings = \
        {(records.PlenarySitting(**p)['sitting'] or None) for p in
         db.plenary_sittings.find(filter={'date': plenary['date']})} | \
        {plenary['sitting'] or None}
    sittings = sorted(sittings, key=lambda v: float('inf') if v is None else v)
    for i, sitting in enumerate(sittings):
        if i:
            _filename = '{}_{}.yaml'.format(plenary['date'], i+1)
        else:
            _filename = '{}.yaml'.format(plenary['date'])
        if plenary['sitting'] == sitting:
            plenary['_filename'] = _filename
        db.plenary_sittings.find_one_and_update(
            filter={'date': plenary['date'], 'sitting': sitting},
            update={'$set': {'_filename': _filename}})

    result = db.plenary_sittings.find_one_and_update(
        filter={'_filename': plenary['_filename']},
        update=plenary.compact(),
        upsert=True,
        return_document=pymongo.ReturnDocument.AFTER)
    if not result:
        logging.warning("Could not insert or update plenary on '{}'"
                        " with URL '{}'".format(plenary['date'], url))

    for bill in bills:
        result = db.bills.find_one_and_update(
            filter={'_filename': bill['_filename']},
            update={'$set': bill},
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER)
        if not result:
            logging.warning(
                "Could not insert or update bill with id '{}'"
                " and title '{}' in '{}'".format(bill['identifier'],
                                                 bill['title'], url))


async def process_agenda(url):
    try:
        text = await Requester.get_text(url)
    except UnicodeDecodeError:
        # Probably a PDF
        logging.error("Could not decode '{}'".format(url))
        return
    pages = _exec_blocking(parse_agenda, url, text)


async def process_agenda_listing(url, form_data=None, lpass=1):
    text = await Requester.get_text(url, form_data=form_data,
                                    request_method='post')
    html = lxml.html.document_fromstring(text)
    html.make_links_absolute(url)

    if lpass == 1:
        pagination = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
        if not pagination:
            await process_agenda_listing(url, lpass=2)
        else:
            await asyncio.gather(*{
                process_agenda_listing(
                    url,
                    form_data={'page': ''.join(c for c in s
                                               if str.isdigit(c))},
                    lpass=2)
                for s in pagination})
    elif lpass == 2:
        await asyncio.gather(*{
            process_agenda(href)
            for href in html.xpath('//a[@class="h3Style"]/@href')})


async def process_agenda_index(url):
    text = await Requester.get_text(url)
    html = lxml.html.document_fromstring(text)
    html.make_links_absolute(url)

    await asyncio.gather(*{
        process_agenda_listing(href)
        for href in html.xpath('//a[@class="h3Style"]/@href')})


def parse_qa_listing(url, text):
    """Create individual question records from a question listing."""
    text = pypandoc.convert(text, 'html5', format='html')
    html = lxml.html.document_fromstring(text)
    html.make_links_absolute(url)

    seen = set()

    def _extract_qs():
        heading, body, footer = (), [], []
        q_listing = (
                (e, GlyphNorm.ungarble(_clean_spaces(e.text_content())))
                for e in html.xpath('//tr//p'))
        while True:
            e = next(q_listing, None)
            if not e or e[1].startswith('Ερώτηση με αρ.'):
                if heading and body:
                    yield heading, body, footer
                else:
                    logging.warning(
                        "Heading and/or body empty in question"
                        " '{}' in '{}'".format((heading, body, footer), url))
                    if not e:
                        break
                heading = e
                body = []
                footer = []
            elif e[1].startswith('Απάντηση'):
                footer.append(e)
            else:
                body.append(e)

    for heading, body, footer in _extract_qs():
        question = records.Question()

        m = re.match(
            r'Ερώτηση με αρ\. (?P<id>[\d\.]+),? ημερομηνίας'
            r' (?P<date>[\w ]+),? (?P<pp>της|του) βουλευτ(?:ού|ή)'
            r' εκλογικής περιφέρειας \w+ κ\. (?P<mp>[\.\-\w’ ]+)',
            heading[1])
        # Format prior to 2002
        m = m or re.match(
            r'Ερώτηση με αρ\. (?P<id>[\d\.]+) που υποβλήθηκε από'
            r' (?P<pp>τη|το) βουλευτή (?:εκλογικής περιφέρειας )?\w+'
            r' κ\. (?P<mp>[\.\-\w’ ]+) (?:την|στις) (?P<date>[\w ]+)',
            heading[1])
        if not m:
            logging.error("Could not parse heading '{}' in '{}'".format(
                heading[1], url))
            continue

        try:
            question['question']['date'] = _parse_long_date(m.group('date'))
        except ValueError:
            logging.error("Could not convert date of question with id"
                          " '{}' in '{}'".format(question['identifier'], url))
            continue

        question['_filename'] = '{}.yaml'.format(m.group('id'))
        question['identifier'] = m.group('id')
        question['question']['text'] = '\n\n'.join(
            (p_text for _, p_text in body)).strip()
        question['question']['title'] = heading[1]

        try:
            mp = NameConverter.find_match(m.group('mp'), m.group('pp'))
            if not mp:
                raise ValueError
        except ValueError:
            logging.warning(
                "Could not insert name '{}' in question with id '{}'"
                " in '{}'".format(m.group('mp'), m.group('id'), url))
        else:
            question['question']['by'].append(mp)

        for a, _ in footer:
            try:
                question['answers'].append(a.xpath('.//a/@href')[0])
            except IndexError:
                logging.warning(
                    "Could not extract URL of answer to question with"
                    " id '{}' in '{}'".format(question['identifier'], url))

        if question['identifier'] in seen:
            question = question.compact()
            logging.warning("Question with id '{}' in '{}' processed"
                            " repeatedly".format(question['identifier'], url))
        else:
            seen.add(question['identifier'])

        result = db.questions.find_one_and_update(
            filter={'_filename': question['_filename']},
            update={'$set': question},
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER)
        if not result:
            logging.warning("Could not insert or update question '{}' from"
                            " '{}'".format(question, url))


async def process_qa_listing(url):
    text = await Requester.get_text(url)
    _exec_blocking(parse_qa_listing, url, text)

    await asyncio.gather(process_qa_index(url))


async def process_qa_index(url):
    text = await Requester.get_text(url)
    html = lxml.html.document_fromstring(text)
    # Infinite loops ahoy
    html.rewrite_links(lambda s: None if urldefrag(s).url == url else s,
                       base_href=url)

    await asyncio.gather(*{
        process_qa_listing(href)
        for href in html.xpath('//a[contains(@href, "chronological")]/@href')})


def parse_transcript_listing(url, text):
    """Add links to the transcript PDFs to corresponding plenaries."""
    html = lxml.html.document_fromstring(text)
    html.make_links_absolute(url)

    for href, date, date_success in (
            (href, *_parse_transcript_date(href))
            for href in html.xpath('//a[contains(@href, "praktiko")]/@href')):
        if not date_success:
            logging.error("Could not extract date '{}' from transcript"
                          " index at '{}'".format(date, url))
            continue

        if date[0] != date[1] and db.plenary_sittings.find_one(
                    filter={'_filename': '{}.yaml'.format(date[1])}):
            date = date[1]
        else:
            date = date[0]

        result = db.plenary_sittings.find_one_and_update(
            filter={'_filename': '{}.yaml'.format(date)},
            update={'$addToSet': {
                # Objs need to be arranged in the same (alphabetical)
                # order to be considered identical; and hence the OrderedDict
                'links': od([('type', 'transcript'), ('url', href)])}})
        if not result:
            logging.warning("Could not locate or update plenary for date '{}'"
                            " of transcript".format(date))


async def process_transcript_listing(url):
    text = await Requester.get_text(url)
    _exec_blocking(parse_transcript_listing, url, text)


async def process_transcript_index(url):
    text = await Requester.get_text(url)
    html = lxml.html.document_fromstring(text)
    html.make_links_absolute(url)

    await asyncio.gather(*{
        process_transcript_listing(href)
        for href in html.xpath('//a[@class="h3Style"]/@href')})


def main():
    """The CLI."""
    TASKS = {
        'agendas': (process_agenda_index,
                    'http://www.parliament.cy/easyconsole.cfm/id/290'),
        'qas': (process_qa_index,
                'http://www2.parliament.cy/parliamentgr/008_02.htm'),
        'transcripts': (process_transcript_index,
                        'http://www.parliament.cy/easyconsole.cfm/id/159')}

    args = docopt(__doc__)
    if args['init']:
        populate_db(db)
    elif args['run']:
        loop = asyncio.get_event_loop()
        loop.set_debug(enabled=args['--debug'])
        loop.run_until_complete(
            TASKS[args['<task>']][0](*TASKS[args['<task>']][1:]))
    elif args['dump']:
        dump_collection(db, args['<collection>'], args['<path_on_disk>'])

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
