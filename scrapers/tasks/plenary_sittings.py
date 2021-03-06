
from collections import defaultdict, namedtuple
import csv
import datetime as dt
import functools as ft
from io import StringIO
import itertools as it
import json
import logging
import os
import re

import pandocfilters

from ..client import Task
from ..models import Bill, MP, PlenarySitting as PS
from ..reconciliation import pair_name, load_pairings
from ..text_utils import apply_subs, clean_spaces, parse_datetime, \
                         pandoc_json_to, parse_long_date, \
                         TableParser, translit_unaccent_lc, ungarble_qh


logger = logging.getLogger(__name__)


class PlenaryAgendas(Task):
    """Parse plenary agendas into bill and plenary-sitting records."""

    ignore = ['http://www.parliament.cy/images/media/redirectfile/07-1407016 -  agenda .pdf',
              'http://www.parliament.cy/images/media/redirectfile/18- 0302017- agenda doc.pdf']  # 404

    async def process_agenda_index(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/290'
        html = await self.c.get_html(url)

        agenda_urls = await self.c.gather(self.process_multi_page_listing(href)
                                          for href in html.xpath('//a[@class="h3Style"]/@href'))
        agenda_urls = set(it.chain.from_iterable(agenda_urls))
        return await self.c.gather(self.process_agenda(href)
                                   for href in agenda_urls
                                   if not href in self.ignore)

    __call__ = process_agenda_index

    async def process_multi_page_listing(self, url):
        if url.endswith('.pdf'):
            return [url]

        html = await self.c.get_html(url)
        pages = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
        if pages:
            pages = {self.process_multi_page_page(url, form_data={'page': ''.join(filter(str.isdigit, p))})
                     for p in pages}
            return it.chain.from_iterable(await self.c.gather(pages))
        else:
            return await self.process_multi_page_page(url)

    async def process_multi_page_page(self, url, form_data=None):
        html = await self.c.get_html(url, form_data=form_data, request_method='post')
        return html.xpath('//a[@class="h3Style"]/@href')

    async def process_agenda(self, url):
        if url.endswith('.pdf'):
            _, payload = await self.c.get_payload(url, decode=True)
            return parse_pdf_agenda, (url, payload)
        else:
            html = await self.c.get_html(url)
            return parse_agenda, (url, html)

    def after(output):
        for url, date, text, agenda_items in \
                filter(None, (fn(url, c) for fn, (url, c) in output)):
            plenary_sitting = PS(
                _sources=[url],
                agenda=PS.PlenaryAgenda(cap1=[i for i, _ in agenda_items.cap1],
                                        cap4=[i for i, _ in agenda_items.cap4]),
                links=[PS.Link(type='agenda', url=url)],
                parliamentary_period_id=extract_parliamentary_period(url, text),
                session=extract_session(url, text),
                sitting=extract_sitting(url, text),
                start_date=date)
            try:
                plenary_sitting.insert(merge=plenary_sitting.exists)
            except plenary_sitting.InsertError as e:
                logger.error(e)

            for id_, title in agenda_items.bills_and_regs:
                bill = Bill(_sources=[url], identifier=id_, title=title)
                try:
                    bill.insert(merge=bill.exists)
                except bill.InsertError as e:
                    logger.error(e)


class AgendaItems:
    """Group agenda items according to type."""

    class _AgendaItemDict(defaultdict):

        def __setitem__(self, key, value):
            if key in self:
                super().__setitem__(key, self[key] + value)
            else:
                super().__setitem__(key, value)

        def __getitem__(self, key):
            if isinstance(key, tuple):
                return it.chain.from_iterable(map(super().__getitem__, key))
            else:
                return super().__getitem__(key)

    ITEM_TYPES = (r'(10\.04\.003)',       # 'Ειδικό ένταλμα πληρωμής'
                  r'(10\.04\.002\.001)',  # Ministerial report
                  r'(13\.06)',   # Budget-related decision—maybe?
                  r'(23\.01)\.(?:\d{3}\.|0\d{1,2}\.\d{3}-)\d{4}$',  # Government bill
                  r'(23\.02)\.(?:\d{3}\.|0\d{1,2}\.\d{3}-)\d{4}$',  # Members' bill
                  r'(23\.03)\.(?:\d{3}\.|0\d{1,2}\.\d{3}-)\d{4}$',  # Draft regulations
                  r'(23\.04)',   # Something or other to do with committees
                  r'(23\.05)',   # Debate topic
                  r'(23\.10)',   # Decision or draft resolution
                  r'(23\.15)')   # Rules of order of the House
    ITEM_TYPES = tuple(re.compile(i).match for i in ITEM_TYPES)

    @classmethod
    def _group(cls, item):
        item_type = filter(None, (m(item[0]) for m in cls.ITEM_TYPES))
        try:
            return next(item_type).group(1)
        except StopIteration:
            return

    _AgendaItems = namedtuple('AgendaItems', 'cap1 cap4 bills_and_regs')

    def __new__(cls, url, agenda_items_):
        agenda_items = cls._AgendaItemDict(tuple)
        for k, v in it.groupby(agenda_items_, key=cls._group):   # __init__ bypasses __setitem__ (maybe)
            agenda_items[k] = tuple(v)
        if None in agenda_items:
            logger.debug(f'Unparsed items '
                         f'{tuple(k for k, *_ in agenda_items[None])} '
                         f'in {url!r}')

        return cls._AgendaItems(
            *map(lambda v: sorted(v, key=agenda_items_.index),
                 (agenda_items['13.06', '23.01', '23.02', '23.03', '23.10',
                               '23.15'],
                  agenda_items['23.04', '23.05'],
                  agenda_items['23.01', '23.02', '23.03']
                  )))


RE_ID = re.compile(r'([12]3\.[0-9.-]+)')
RE_ITEM_NO = re.compile(r'^\d+\. *')
RE_TITLE_OTHER = re.compile(r'\. *\(.*')


def extract_id_and_title(url, item):
    if not item or item.startswith('ΚΕΦΑΛΑΙΟ'):  # Skip headings
        return

    id_ = RE_ID.search(item)
    if not id_:
        logger.debug(f'Unable to extract document type of {item!r} in {url!r}')
        return

    # Concatenate the rows into a title string, extract the number,
    # and throw out the junk
    id_ = id_.group(1)
    title = RE_ITEM_NO.sub('', RE_TITLE_OTHER.sub('', item)).rstrip('. ')
    if id_ and title:
        return id_, title
    else:
        logger.debug(f'Number or title empty in {(id_, title)} in {url!r}')


RE_PPERIOD = re.compile(r'_?(\w+?)[\'’΄´] ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟ[Δ∆]ΟΣ')
RE_SESSION = re.compile(r'ΣΥΝΟ[Δ∆]ΟΣ (\w+)[\'’΄´]')
RE_SITTING_NUMBER = re.compile(r'(\d+)[ηή] ?συνεδρίαση')


def extract_parliamentary_period(url, text):
    """Extract and beautify the legislative period.

    >>> extract_parliamentary_period(..., 'Ι΄ ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ - ΣΥΝΟΔΟΣ Ε΄')
    'Ι'
    >>> extract_parliamentary_period(..., 'I΄ ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ -'
    ...                                   ' ΣΥΝΟΔΟΣ Ε΄')   # Latin u/c 'I'
    'Ι'
    >>> extract_parliamentary_period(..., '') is None
    True
    """
    match = RE_PPERIOD.search(text)
    if match:
        return ungarble_qh(match.group(1))
    logger.warning(f'Unable to extract parliamentary period of {url!r}')


def extract_session(url, text):
    """Extract and beautify the session number.

    >>> extract_session(..., 'Ι΄ ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ - ΣΥΝΟΔΟΣ Ε΄')
    'Ε'
    """
    match = RE_SESSION.search(text)
    if match:
        return ungarble_qh(match.group(1))
    logger.warning(f'Unable to extract session of {url!r}')


def extract_sitting(url, text):
    """Extract the sitting number.

    >>> extract_sitting(..., '17η συνεδρίαση')
    17
    """
    match = RE_SITTING_NUMBER.search(text)
    if match:
        return int(match.group(1))
    logger.warning(f'Unable to extract sitting number of {url!r}')


RE_JUNK = re.compile(r'^ *([\.…]+)', re.MULTILINE)


def parse_agenda(url, html):
    text = clean_spaces(html.xpath('string(//div[@class="articleBox"])'))

    agenda_items = (clean_spaces(RE_JUNK.sub('', agenda_item.text_content()),
                                 medial_newlines=True)
                    for agenda_item in html.xpath('//div[@class="articleBox"]'
                                                  '//tr'))
    agenda_items = filter(None, map(extract_id_and_title,
                                    it.repeat(url), agenda_items))
    agenda_items = AgendaItems(url, tuple(agenda_items))
    return (url,
            parse_long_date(clean_spaces(html.xpath('string(//h1)')), plenary=True),
            text,
            agenda_items)


RE_PAGE_NO = re.compile(r'^ +(?:\d+|\w)$', re.MULTILINE)


def _group_items_of_pdf():
    store = None

    def inner(key):
        nonlocal store
        # Return a previous 'valid' key until a new one is found
        if not key[0]:
            return store
        else:
            store = key[0]
            return store
    return inner


def clean_title_text(text):
    return ft.reduce(lambda s, j: ''.join((s[:j.start()], ' '*len(j.group(1)),
                                           s[j.end():])),
                     RE_JUNK.finditer(text), text)


def parse_pdf_agenda(url, text):
    if (url == 'http://www.parliament.cy/images/media/redirectfile/'
               '13-0312015- agenda ΤΟΠΟΘΕΤΗΣΕΙΣ doc.pdf'):
        # `TableParser` chokes on its mixed two- and three-col layout
        logger.debug(f'Skipping {url!r} in `parse_pdf_agenda`')
        return

    # Get rid of the page numbers 'cause they might intersect items in
    # the list
    pages = RE_PAGE_NO.sub('', clean_title_text(text))
    # And split the text at page breaks 'cause the table shifts from page to
    # page
    pages = tuple(filter(None, pages.split('\x0c')))

    rows_ = it.chain.from_iterable(TableParser(page).rows for page in pages)
    # Group rows into tuples, using the leftmost cell as a key, which oughta
    # either contain a list number or be left blank
    agenda_items = (' '.join(i[-1] for i in v)
                    for _, v in it.groupby(rows_, key=_group_items_of_pdf()))
    agenda_items = filter(None, map(extract_id_and_title,
                                    it.repeat(url), agenda_items))
    agenda_items = AgendaItems(url, tuple(agenda_items))
    return (url,
            parse_long_date(clean_spaces(pages[0], medial_newlines=True), plenary=True),
            text,
            agenda_items)


class PlenaryTranscripts(Task):
    """Extract MPs in attendance at plenary sittings from transcripts."""

    NAMES = load_pairings('attendance_names.csv')

    ignore = ['praktiko2002-07.04parartima.doc',
              'praktiko2011-06-30.doc',
              'praktiko1999-01-22.pdf',  # 404
              'praktiko1999-01-28.pdf',  # 404
              'praktiko1999-02-04.pdf',  # 404
              ]

    async def __call__(self):
        html = await self.c.get_html('http://www2.parliament.cy/parliamentgr/008_01.htm')

        transcript_urls = await self.c.gather(self.process_transcript_listing(url)
                                              for url in html.xpath('''\
//a[starts-with(@href, "http://www2.parliament.cy/parliamentgr/008_01_01")]/@href'''))
        return await self.c.gather(self.process_transcript(url)
                                   for url in set(it.chain.from_iterable(transcript_urls))
                                   if not any(i in url for i in self.ignore))

    async def process_transcript_listing(self, url):
        html = await self.c.get_html(url)
        return html.xpath('//a[contains(@href, "praktiko")]/@href')

    async def process_transcript(self, url):
        func, content = await self.c.get_payload(url, decode=True)
        return url, func, content

    def after(output):
        for url, text, heading, date, cap2, bills in \
                filter(None, (parse_transcript(*t) for t in output)):
            attendees = filter(None,
                               ((PlenaryTranscripts.NAMES.get(v) or
                                 logger.debug(f'No match found for {v!r}'))
                                for v in extract_attendees(url, text, heading, date)))
            plenary_sitting = \
                PS(_sources=[url],
                   agenda=PS.PlenaryAgenda(cap2=cap2),
                   attendees=[{'mp_id': a} for a in attendees],
                   links=[PS.Link(type='transcript', url=url)],
                   parliamentary_period_id=extract_parliamentary_period(url, heading),
                   session=extract_session(url, heading),
                   sitting=extract_sitting_from_tr(url, heading),
                   start_date=date)
            plenary_sitting.insert(merge=plenary_sitting.exists)

            for bill in bills:
                try:
                    submit = Bill.Submission(plenary_sitting_id=plenary_sitting._id,
                                             sponsors=bill.sponsors,
                                             committees_referred_to=bill.committees,
                                             title=bill.title)
                except ValueError:
                    # Discard likely malformed bills
                    logger.error(f'Unable to parse {bill!r} into a bill')
                    continue

                bill = Bill(_sources=[url], actions=[submit],
                            identifier=bill.number, title=bill.title)
                bill.insert(merge=bill.exists)


class ReconcileAttendanceNames(PlenaryTranscripts):

    def after(output):
        names_and_ids = {i['_id']: i['name']['el'] for i in MP.collection.find()}
        names = sorted(set(it.chain.from_iterable(
            extract_attendees(u, t, h, d)
            for u, t, h, d, *_ in
            filter(None, (parse_transcript(*t) for t in output)))))
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(('name', 'id'))
        csv_writer.writerows(pair_name(n, names_and_ids, PlenaryTranscripts.NAMES)
                             for n in names)
        print(output.getvalue())


# http://www.parliament.cy/easyconsole.cfm/id/194
PRESIDENTS = (([(1991, 5, 30), (1996,  5, 26)],  'Γαλανός Αλέξης'),
              ([(1996, 6,  6), (2001, 12, 19)],  'Κυπριανού Σπύρος'),
              ([(2001, 6,  7), (2008,  2, 28)],  'Χριστόφιας Δημήτρης'),
              ([(2008, 3,  6), (2011,  4, 22)],  'Κάρογιαν Μάριος'),
              ([(2011, 6,  2), (2016,  4, 14)],  'Ομήρου Γιαννάκης'),
              ([(2016, 6,  8), dt.date.today()], 'Συλλούρης Δημήτρης'),)
PRESIDENTS = tuple(((dt.date(*s) + dt.timedelta(days=1),
                     dt.date(*e) + dt.timedelta(days=1) if isinstance(e, tuple) else e),
                    name)
                   for (s, e), name in PRESIDENTS)
PRESIDENTS = tuple((range(*map(dt.date.toordinal, dates)), name)
                   for dates, name in PRESIDENTS)


def select_president(date):
    date = parse_datetime(date).toordinal()
    try:
        return next(name for date_range, name in PRESIDENTS
                    if date in date_range)
    except StopIteration:
        raise ValueError(f'No President found for {date!r}')


ATTENDEE_SUBS = (('|', ' '),
                 ('των παρόντων με αλφαβητική σειρά:', '🌮'),
                 ('Παρόντες βουλευτές', '🌮'),
                 ('Παρόντες  βουλευτές', '🌮'),
                 ('ΠΑΡΌΝΤΕΣ ΒΟΥΛΕΥΤΈΣ', '🌮'),      # pandoc
                 ('(Ώρα λήξης: 6.15 μ.μ.)', '🌮'),  # 2015-03-19
                 ('Παρόντες\n', '🌮'),  # 2006-07-06
                 ('Παρόντες αντιπρόσωποι θρησκευτικών ομάδων', '🌯'),
                 ('Αντιπρόσωποι Θρησκευτικών Ομάδων', '🌯'),
                 ('Aντιπρόσωποι Θρησκευτικών Ομάδων', '🌯'),
                 ('Αντιπρόσωποι θρησκευτικών ομάδων', '🌯'),  # 2014-10-23
                 ('Παρόντες αντιπ΄.ρόσωποι θρησκευτικών ομάδων', '🌯'),
                 ('Περιεχόμενα', '🌯'),
                 ('ΠΕΡΙΕΧΟΜΕΝΑ', '🌯'),
                 ('ΠΙΝΑΚΑΣ ΠΕΡΙΕΧΟΜΕΝΩΝ', '🌯'),)


def extract_attendees(url, text, heading, date):
    # Split at page breaks 'cause the columns will have likely shifted
    # and strip off leading whitespace
    *_, attendees = apply_subs(text, ATTENDEE_SUBS).rpartition('🌮')
    attendees, *_ = attendees.partition('🌯')
    attendees = ('\n'.join(l.lstrip() for l in s.splitlines())
                 for s in attendees.split('\x0c'))
    attendees = set(filter(lambda i: i and not i.isdigit(),
                           (clean_spaces(a, medial_newlines=True)
                            for t in attendees
                            for a in TableParser(t).values)))
    if 'ΠΡΟΕΔΡΟΣ:' in heading:  # The President's not listed among the attendees
        attendees = attendees | {select_president(date)}
    return sorted(attendees)


RE_SITTING_NUMBER_TR = re.compile(r'Αρ\. (\d+)')


def extract_sitting_from_tr(url, text):
    match = RE_SITTING_NUMBER_TR.search(text)
    if match:
        return int(match.group(1))
    logger.warning(f'Unable to extract sitting number of {url!r}')


_Cap2Item = namedtuple('Cap2Item', 'number title sponsors committees')


def extract_cap2_item(url, item):
    if not item:
        return

    try:
        title, sponsors, committees = item
    except ValueError:
        logger.debug(f'Unable to parse Chapter 2 item {item} in {url!r}')
        return

    id_ = RE_ID.search(title)
    if not id_:
        logger.debug(f'Unable to extract document type of {item} in {url!r}')
        return
    return _Cap2Item(id_.group(1), RE_TITLE_OTHER.sub('', title).rstrip('. '),
                     sponsors, committees)


RE_CAP2 = re.compile(r'\(Η κατάθεση νομοσχεδίων και εγγράφων\)(.*?)'
                     r'(?:ΠΡΟΕΔΡΟΣ|ΠΡΟΕΔΡΕΥΩΝ|ΠΡΟΕΔΡΕΥΟΥΣΑ)', re.DOTALL)


def _item_groupper():
    counter, prev_length = 0, 0

    def inner(item):
        nonlocal counter, prev_length
        new_length = sum(1 for v in item if v)  # Count 'truthy' values in list
        # The assumption here's that every new list item will contain all
        # three of title, sponsor and committee on the first line, and that
        # fewer than three items will wrap onto the next line.  Ya, it's pretty
        # wonky.  Given how tight the columns are, it's extremely unlikely that
        # the title's gonna fit inside the first line, but it _is_ probable
        # that all three are gonna be of equal length (> 1) - especially if
        # the bill's got multiple sponsors or is referred to several
        # committees.  We're gonna have to resort to using a probabilistic
        # parser (eventually), but regexes and my shitty logic do the job for
        # now (well, sort of)
        if new_length > prev_length:
            counter += 1
        prev_length = new_length
        return counter
    return inner


def extract_cap2(url, text):
    try:
        item_table = RE_CAP2.search(text).group(1)
    except AttributeError:
        logger.debug(f'Unable to extract Chapter 2 table in {url!r}')
        return

    item_table = RE_PAGE_NO.sub('', item_table).replace('|', ' ').split('\x0c')

    items = it.chain.from_iterable(TableParser(t, columns=4).rows
                                   for t in item_table)
    # ((<title>, <sponsor>, <committee>), ...)
    items = (tuple(clean_spaces(' '.join(x), medial_newlines=True)
                   for x in it.islice(zip(*v), 1, None))
             for _, v in it.groupby(items, key=_item_groupper()))
    items = (extract_cap2_item(url, item) for item in items)
    # (Bill(<number>, <title>, <sponsor>, <committee>), ...)
    items = tuple(filter(None, items))
    if items:
        # ((<number>, <number>, ...), (Bill, Bill, ...))
        return next(zip(*items)), AgendaItems(url, items).bills_and_regs


def _walk_pandoc_ast(v):
    return pandocfilters.walk(v, _stringify_pandoc_dicts, None, None)


pandoc_ListNumberMarker = object()

RE_LIST_NUMBER = re.compile(r'\d{1,2}\.$')

PANDOC_TRANSFORMS =  {
    'AlignDefault': lambda _: None,
    'OrderedList': lambda _: [pandoc_ListNumberMarker],
    'Para': lambda v: _walk_pandoc_ast(v),
    'Period': lambda _: '.',
    'Plain': lambda v: _walk_pandoc_ast(v),
    'Space': lambda _: ' ',
    'Str': lambda v: pandoc_ListNumberMarker if RE_LIST_NUMBER.match(v) else v,
    'Strong': lambda v: _walk_pandoc_ast(v),
    'Table': lambda v: v,}


def _stringify_pandoc_dicts(key, value, format_, meta):
    return_value = PANDOC_TRANSFORMS[key](value)
    if isinstance(return_value, list) and \
            all(isinstance(i, str) for i in return_value):
        return_value = [''.join(return_value)]
    return return_value


def _locate_cap2_table(node):
    if node['t'] != 'Table':
        return
    if pandocfilters.stringify(node).startswith('Νομοσχέδια'):
        return True


def extract_pandoc_items(url, list_):
    # Extract agenda items from the pandoc AST
    for x in list_:
        if not x:
            continue
        if isinstance(x[0], list):
            if x[0] == [pandoc_ListNumberMarker]:
                *title, number = x[1]
                title, sponsors, committees = map(' '.join, (title, *x[2:]))
                yield _Cap2Item(RE_ID.search(number).group(1),
                                RE_TITLE_OTHER.sub('', title).rstrip('. '),
                                sponsors, committees)
            else:
                yield from extract_pandoc_items(url, x)


def extract_pandoc_cap2(url, content):
    tables = filter(_locate_cap2_table, json.loads(content)['blocks'])
    try:
        table = next(tables)
    except StopIteration:
        logger.debug(f'Unable to extract Chapter 2 table in {url!r}')
        return

    items = tuple(extract_pandoc_items(url, _walk_pandoc_ast([table])))
    if items:
        return next(zip(*items)), AgendaItems(url, items).bills_and_regs


def p_to_24(hour, p):
    if p == 'μ':
        if hour < 12:
            hour += 12
    elif hour == 12:
        hour = 0
    return hour


time_match = re.compile(r'ωρα\s+εναρξης:\s*'
                        r'(?P<h>\d{1,2})[.:](?P<m>\d{2})\s+(?P<p>[πμ])')

def extract_start_time(heading):
    h, m, p = time_match.search(translit_unaccent_lc(heading)).groups()
    return (parse_datetime(parse_long_date(heading)) +
            dt.timedelta(hours=p_to_24(int(h), p), minutes=int(m))).isoformat()


def parse_transcript(url, func, content):
    if func == 'docx_to_json':
        text = pandoc_json_to(content, 'plain')
    else:
        text = content

    heading = clean_spaces(text[:(text.find('ΠΡΟΕΔΡ') + 9)],
                           medial_newlines=True)
    try:
        date = extract_start_time(heading)
    except Exception as e:
        logger.error(f'{e}; skipping {url!r}')
        return

    if func == 'docx_to_json':
        cap2, bills_and_regs = (extract_pandoc_cap2(url, content) or
                                ((), ()))
    else:
        cap2, bills_and_regs = extract_cap2(url, text) or ((), ())
    return url, text, heading, date, cap2, bills_and_regs


class FirstReading(Task):

    remote_scraper = 'https://morph.io/wfdd/cypriot-parliament-1r-scraper'

    async def __call__(self):
        url = 'https://api.morph.io/wfdd/cypriot-parliament-1r-scraper/data.json'
        params = {'key': os.environ['MORPH_API_KEY'],
                  'query': 'SELECT * FROM first_reading'}
        return await self.c.get_payload(url, params=params)

    def after(output):
        docs = json.loads(output.decode())
        docs = ((d, tuple(i)) for d, i in
                it.groupby(sorted(docs, key=lambda i: i['date_tabled']),
                           key=lambda i: i['date_tabled']))
        for date, items in docs:
            if PS.collection.count({'start_date': re.compile(date)}) != 1:
                logger.debug('No single match found for {date!r}')
                continue

            PS(_id=PS.collection.find_one({'start_date': re.compile(date)})['_id'],
               _sources=[FirstReading.remote_scraper],
               agenda=PS.PlenaryAgenda(cap2=[i['number']
                                             for i in items])).insert(merge=True)
            # for item in items:
            #     bill = Bill(_sources=[FirstReading.remote_scraper],
            #                 actions=[Bill.Submission(plenary_sitting_id=ps._id,
            #                                          sponsors=item['sponsors'],
            #                                          committees_referred_to=item['committees'],
            #                                          title=item['title'].rstrip('.'))],
            #                 identifier=item['number'],
            #                 title=item['title'].rstrip('.'))
            #     bill.insert(merge=bill.exists)
