
"""An XPath and regex soup in a misguided attempt to parse unstructured
structures. This module crawls Parliament's website to collect the
little that is made available and updates existing records on the
openpatata-data repo.

TODO: committee reports; transcript parsing.

Usage:
  scraper.py run <task>
  scraper.py dump <collection>

Options:
  -h --help  Show this screen.
"""

import asyncio
from collections import OrderedDict as od
from functools import lru_cache
from itertools import product
import logging
import os
import re
from urllib.parse import urldefrag

import aiohttp
from docopt import docopt
import lxml.html
import icu
import pymongo
import yaml

DATA_DIR = './data-new/'
TASKS = ...

db = pymongo.MongoClient()['openpatata-data']
# A transliterator to split out any diacritics (`NFKD`) and strip them
# off (`[:nonspacing mark:] any-remove`), and to downcase the
# input (`any-lower`)
tr_norm = icu.Transliterator.createInstance(
    'NFKD; [:nonspacing mark:] any-remove; any-lower',
    icu.UTransDirection.FORWARD).transliterate


class NameConverter:

    """Transform an MP's name in various ways.

    MPs' names in the headings of questions are given in the genitive or
    accusative case. This class hosts a set of rules to change the
    endings of forenames and surnames, depending on gender, so that at
    least one of possible combinations declines the full name in the
    nominative.

    >>> PersonNames('Ανδρούλας Βασιλείου', 'της').names
    {'βασιλειου ανδρουλα', 'βασιλειου ανδρουλας'}

    >>> PersonNames('Γιώργου Προκοπίου', 'του').names
    {'προκοπιος γιωργος',
     'προκοπιος γιωργου',
     'προκοπιος γιωργους',
     'προκοπιου γιωργος',
     'προκοπιου γιωργου',
     'προκοπιου γιωργους',
     'προκοπιους γιωργος',
     'προκοπιους γιωργου',
     'προκοπιους γιωργους'}
    """

    _PP2G = {'της': 'F', 'του': 'M',
             'τη':  'F', 'το':  'M'}
    _NAMES_NOM = {tr_norm(mp['name']['el']): mp['name']['el']
                  for mp in db.mps.find(projection={'name.el': True})}

    _TRANSFORMS_F = (
        # before     after
        ((r'ς$',     r''),),
        ((r'',       r''),))
    _TRANSFORMS_M = (
        ((r'',       r''),
         (r'$',      r'ς'),
         (r'ο[υύ]$', r'ος')),
        ((r'',       r''),
         (r'$',      r'ς'),
         (r'ο[υύ]$', r'ος')))

    def __init__(self, name, pp):
        self._orig_name = name.strip().split(' ')
        if len(self._orig_name) > 3:
            raise ValueError("Malformed name: '{}'".format(name))

        self._names = [self._orig_name]
        self._compute(getattr(self, '_TRANSFORMS_' + self._PP2G[pp]))

    def _compute(self, transforms):
        for fore, aft in product(transforms[0], transforms[1]):
            first, *middle, surname = self._orig_name
            self._names.append([
                re.sub(fore[0], fore[1], first),
                *(middle and [re.sub(aft[0], aft[1], middle[0])]),
                re.sub(aft[0], aft[1], surname)])

    @property
    def names(self):
        return set(' '.join(reversed([tr_norm(part) for part in name]))
                   for name in self._names)

    @classmethod
    @lru_cache()
    def find_match(cls, name, pp):
        """Pair an MP's name with their normative, nominative name."""
        for norm_name in cls(name, pp).names:
            if norm_name in cls._NAMES_NOM:
                return cls._NAMES_NOM[norm_name]
        raise ValueError("No match found for name '{}'".format(name))


class Record(dict):

    """A recursive defaultdict for indolent programmers."""

    def __missing__(self, key):
        self[key] = type(self)()
        return self[key]


def _parse_long_date(date_string, plenary=False):
    """Convert a long date in Greek into an ISO date."""
    PLENARY_EXCEPTIONS = {
        'Συμπληρωματική ημερήσια διάταξη 40-11072013': '2013-07-11'}
    if plenary and date_string in PLENARY_EXCEPTIONS:
        return PLENARY_EXCEPTIONS[date_string]

    MONTHS = dict(zip(map(tr_norm,
                          icu.DateFormatSymbols(icu.Locale('el')).getMonths()),
                      range(1, 13)))
    try:
        d, m, y = re.search(r'(\d{1,2})(?:ης?)?[  ]+(\w+)[  ]+(\d{4})',
                            date_string).groups()
    except AttributeError:
        raise ValueError("Date is likely invalid: '{}'".format(date_string))
    try:
        return '{}-{:02d}-{:02d}'.format(
            *map(int, (y, MONTHS[tr_norm(m)], d)))
    except KeyError:
        raise ValueError("Malformed month in date '{}'".format(date_string))


def _parse_transcript_date(date_string):
    """Extract dates from transcript URLs in the ISO format."""
    success = True

    EXCEPTIONS = {
        'http://www2.parliament.cy/parliamentgr/008_01/'
        '008_02_IC/praktiko2013-12-30.pdf': '2014-01-30'}
    if date_string in EXCEPTIONS:
        return EXCEPTIONS[date_string], success

    try:
        date_string = re.search(r'(\d{4}-\d{2}-\d{2})',
                                date_string.strip()).group(1)
    except AttributeError:
        success = False
    return date_string, success


def _normalise_glyphs(garbled_string):
    """Replace Latin characters within Greek lexemes with their visual
    Greek equivalents. Parliament keep mixing them up. Somehow.
    """
    EN_TO_EL = {'A': 'Α', 'B': 'Β', 'E': 'Ε', 'Z': 'Ζ', 'H': 'Η',
                'I': 'Ι', 'K': 'Κ', 'M': 'Μ', 'N': 'Ν', 'O': 'Ο',
                'P': 'Ρ', 'T': 'Τ', 'Y': 'Υ', 'X': 'Χ', 'v': 'ν',
                'o': 'ο'}

    def _en_to_el(s):
        s = list(s.group(1) or s.group(2))
        for pos, char in enumerate(s):
            s[pos] = EN_TO_EL[char]
        return ''.join(s)

    # icu.LocaleData.getExemplarSet([[0, ]1])
    # (Yes, really, that _is_ the function's signature: a second positional
    # argument shifts the first one to the right.)
    #
    # (0: options) -> USET_IGNORE_SPACE = 1
    #                 USET_CASE_INSENSITIVE = 2
    #                 USET_ADD_CASE_MAPPINGS = 4
    # Mystical transformations. See
    # https://ssl.icu-project.org/apiref/icu4c/uset_8h.html for the juicy
    # details.
    # The icu4c bitmasks are not exposed (as constants) in pyicu; use the ints.
    #
    # (1: extype)  -> ULOCDATA_ES_STANDARD = 0
    #                 ULOCDATA_ES_AUXILIARY = 1
    #                 ULOCDATA_ES_INDEX = 2
    #                 ULOCDATA_ES_PUNCTUATION = 3
    #                 ULOCDATA_ES_COUNT = 4
    # The icu4c bitmasks are not exposed (as constants) in pyicu.
    # See http://cldr.unicode.org/translation/characters
    # for an explanation of each of `extype`, and
    # https://ssl.icu-project.org/apiref/icu4c/ulocdata_8h_source.html#l00041
    # for their values, if they're ever to change.
    #
    # `getExemplarSet` returns an instance of `icu.UnicodeSet`, which can be
    # cast to a string to produce a matching pattern, in the form of a range
    # (e.g. `[a-z]`); or to a list to produce a list of all characters
    # (codepoints) the pattern encapsulates. Alternatively, a `UnicodeSet`
    # can be consumed by a `icu.UnicodeSetIterator`.
    el_glyphs = str(icu.LocaleData('el').getExemplarSet(2, 0))
    en_glyphs = '[{}]'.format(''.join(EN_TO_EL))

    return re.sub(
        r'(?:(?<={0})({1}+)|({1}+)(?={0}))'.format(el_glyphs, en_glyphs),
        _en_to_el, garbled_string)


async def parse_agenda(url):
    """Create plenary records from agendas."""
    response = await aiohttp.get(url)
    try:
        html = lxml.html.document_fromstring(await response.text())
    except UnicodeDecodeError:
        # Probably a PDF
        logging.exception("Could not decode '{}'".format(url))
        return

    plenary = Record()
    plenary['date'] = _parse_long_date(html.xpath('//h1/text()')[0],
                                       plenary=True)

    agenda_items = html.xpath('//div[@class="articleBox"]//tr/td[last()]')
    for i in agenda_items:
        try:
            title, *ext, ident = [e.text_content() for e in i.xpath('div|p')]
        except ValueError:
            # Presumably a faux header; skip it
            continue

        title = re.sub(r'[  ]+', ' ', title).rstrip('.')
        ident = re.sub(
            r'[^0-9\.\-]', '', ident.strip() or ext[0]).strip('.')
        try:
            doc_type = re.match(r'23\.(\d{2})', ident).group(1)
        except AttributeError:
            logging.error(
                "Could not extract document type of '{}'"
                " with id '{}' in '{}'".format(title, ident, url))
        else:
            if doc_type in {'04', '05'}:
                plenary['agenda']['debate'] = plenary['agenda']['debate'] or []
                plenary['agenda']['debate'].append(ident)
            else:
                plenary['agenda']['legislative_work'] = \
                    plenary['agenda']['legislative_work'] or []
                plenary['agenda']['legislative_work'].append(ident)

                bill_filename = '{}.yaml'.format(ident)
                result = db.bills.find_one_and_update(
                    filter={'_filename': bill_filename},
                    update={'$set': {'identifier': ident, 'title': title}},
                    upsert=True,
                    return_document=pymongo.ReturnDocument.AFTER)
                if not result:
                    logging.warning(
                        "Could not insert or update bill with id '{}'"
                        " and title '{}' in '{}'".format(ident, title, url))

    plenary['links'] = [{'type': 'agenda', 'url': url}]

    subheader = html.xpath('//h1/../following-sibling::*[1]')[0].text_content()
    try:
        plenary['parliament'], plenary['session'] = re.search(
            r'(\w)[\'΄].*?(\w)[\'΄]', subheader).groups()
    except AttributeError:
        logging.error("Could not extract parliamentary period and session"
                      " of '{}' from '{}'".format(url, subheader))

    try:
        plenary['sitting'] = re.search(
            r'(\d+)η[  ]+συνεδρίαση', html.text_content()).group(1)
        plenary['sitting'] = int(plenary['sitting'])
    except AttributeError:
        logging.error("Could not extract sitting number of '{}'".format(url))

    plenary['_filename'] = '{}.yaml'.format(plenary['date'])
    result = db.plenary_sittings.find_one_and_update(
        filter={'_filename': plenary['_filename']},
        update={'$set': plenary},
        upsert=True,
        return_document=pymongo.ReturnDocument.AFTER)
    if not result:
        logging.warning("Could not insert or update plenary on '{}'"
                        " with URL '{}'".format(plenary['date'], url))


async def parse_agenda_session_index(url):
    response = await aiohttp.get(url)
    html = lxml.html.document_fromstring(await response.text())
    html.make_links_absolute(url)

    await asyncio.gather(*(
        parse_agenda(href)
        for href in html.xpath('//a[@class="h3Style"]/@href')))


async def parse_agenda_index(url):
    response = await aiohttp.get(url)
    html = lxml.html.document_fromstring(await response.text())
    html.make_links_absolute(url)

    await asyncio.gather(*(
        parse_agenda_session_index(href)
        for href in html.xpath('//a[@class="h3Style"]/@href')))
    logging.info('Crawled agendas')


async def parse_qa_list(url):
    """Create individual question records from a question listing."""
    response = await aiohttp.get(url)

    html = lxml.html.document_fromstring(await response.text())
    html.make_links_absolute(url)
    text = _normalise_glyphs(html.body.text_content())

    SUBS = ((r'\r\n',                    '\n'),
            (r'(?<=\n)[  ]+',            ''),
            (r'[  ]+(?=\n)',             ''),
            (r'(?<!\n)\n(?=\d+\.)',      '\n\n'),
            (r'(?<!\n)\n(?!\n)',         ' '),
            (r'\n{3,}',                  '\n\n'),
            (r'(?<=«) +',                ''),
            (r' +(?=»)',                 ''),
            (r'(?<=Ερώτηση με αρ\.)\n+', ' '),
            (r'(?<=») +(?=Απάντηση\n)',  '\n'),
            (r'(?<=\d{4}\.) (?=«)',      '\n\n'),
            (r'\n+(?=\d{4}\.\n)',        ' '),
            (r'(?<=κ\. )’',              'Ά'))  # ....
    for pattern, repl in SUBS:
        text = re.sub(pattern, repl, text)

    question = Record()
    for line in text.splitlines():
        if line.startswith('Ερώτηση με αρ.'):
            m1 = re.match(
                r'Ερώτηση με αρ\. (?P<id>[\d\.]+),? ημερομηνίας'
                r' (?P<date>[ \d\w]+), (?P<pp>του|της) βουλευτ(?:ή|ού)'
                r' εκλογικής περιφέρειας \w+ κ\. (?P<mp>[\.\w ]+)', line)
            # Prior to 2002
            m2 = re.match(
                r'Ερώτηση με αρ\. (?P<id>[\d\.]+) που υποβλήθηκε από'
                r' (?P<pp>το|τη) βουλευτή (?:εκλογικής περιφέρειας )?\w+'
                r' κ\. (?P<mp>[\.\w ]+) (?:την|στις) (?P<date>[ \d\w]+)', line)

            m = m1 or m2
            if not m:
                logging.error("Could not parse heading '{}' in '{}'".format(
                    line, url))
                continue

            question['identifier'] = m.group('id')
            try:
                question['question']['by'] = [
                    NameConverter.find_match(m.group('mp'), m.group('pp'))]
            except ValueError:
                loggging.warning('')

            question['question']['date'] = m.group('date')
            try:
                question['question']['date'] = _parse_long_date(
                    question['question']['date'])
            except ValueError:
                logging.error("Could not convert date of question with id"
                              " '{}' in '{}'".format(question['identifier'],
                                                     url))
            else:
                question['_filename'] = '{}.yaml'.format(
                    question['identifier'])

                try:
                    question['answer'] = html.xpath(
                        '//a[contains(@href, "{}")]/@href'.format(
                            question['identifier'].replace('.', '_')))[0]
                except IndexError:
                    question['answer'] = None
                    logging.warning(
                        "Could not extract URL of answer to question with"
                        " id '{}' in '{}'".format(question['identifier'], url))

            question['question']['title'] = line
            continue

        m = re.match(r'Απάντηση$', line)
        if m:
            if question['question']['text'] and not question['_filename']:
                logging.error(r"Unpaired text '{}' in '{}'".format(
                    question['question']['text'], url))
            else:
                question['question']['text'] = \
                    question['question']['text'].strip()
                result = db.questions.find_one_and_update(
                    filter={'_filename': question['_filename']},
                    update={'$set': question},
                    upsert=True,
                    return_document=pymongo.ReturnDocument.AFTER)
                if not result:
                    logging.warning(
                        "Could not insert or update question '{}'"
                        " from '{}'".format(question, url))
            question = Record()
        else:
            question['question']['text'] = question['question']['text'] or ''
            question['question']['text'] += '\n' + line

    await asyncio.gather(parse_qa_index(url))


async def parse_qa_index(url):
    response = await aiohttp.get(url)
    html = lxml.html.document_fromstring(await response.text())
    # Infinite loops ahoy
    html.rewrite_links(lambda s: None if urldefrag(s).url == url else s,
                       base_href=url)

    await asyncio.gather(*(
        parse_qa_list(href)
        for href in html.xpath('//a[contains(@href, "chronological")]/@href')))
    logging.info("Crawled Q/As")


async def parse_transcript_list(url):
    """Add links to the transcript PDFs to corresponding plenaries."""
    response = await aiohttp.get(url)
    html = lxml.html.document_fromstring(await response.text())
    html.make_links_absolute(url)

    for href, date, date_success in (
            (href, *_parse_transcript_date(href))
            for href in html.xpath('//a[contains(@href, "praktiko")]/@href')):
        if not date_success:
            logging.error("Could not extract date '{}' from transcript"
                          " index at '{}'".format(date, url))
            continue

        result = db.plenary_sittings.find_one_and_update(
            filter={'_filename': '{}.yaml'.format(date)},
            update={'$addToSet': {
                # Objs need to be arranged in the same (alphabetical)
                # order to be considered identical, and hence the OrderedDict
                'links': od([('type', 'transcript'), ('url', href)])}})
        if not result:
            logging.warning("Could not locate or update plenary for date '{}'"
                            " of transcript".format(date))


async def parse_transcript_index(url):
    response = await aiohttp.get(url)
    html = lxml.html.document_fromstring(await response.text())
    html.make_links_absolute(url)

    await asyncio.gather(*(
        parse_transcript_list(href)
        for href in html.xpath('//a[@class="h3Style"]/@href')))
    logging.info('Crawled transcript indices')


def _yaml_dump(data, path):
    """Save a document to disk as YAML."""
    path = os.path.join(DATA_DIR, path)
    head = os.path.dirname(path)
    if not os.path.exists(head):
        os.makedirs(head)
    with open(path, 'w') as file:
        yaml.dump(data, file,
                  allow_unicode=True, default_flow_style=False)


def dump_collection(collection):
    for doc in db[collection].find(projection={'_id': False}):
        filename = doc['_filename']
        del doc['_filename']
        _yaml_dump(doc, os.path.join(collection, filename))

if __name__ == '__main__':
    TASKS = {
        'agendas': (parse_agenda_index,
                    'http://www.parliament.cy/easyconsole.cfm/id/290'),
        'qas': (parse_qa_index,
                'http://www2.parliament.cy/parliamentgr/008_02.htm'),
        'transcripts': (parse_transcript_index,
                        'http://www.parliament.cy/easyconsole.cfm/id/159')}

    args = docopt(__doc__)
    if args['run']:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            TASKS[args['<task>']][0](*TASKS[args['<task>']][1:]))
    elif args['dump']:
        dump_collection(args['<collection>'])
