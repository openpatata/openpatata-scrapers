
from collections import defaultdict, namedtuple
import itertools
import logging
import re

from scrapers.crawling import Task
from scrapers import records
from scrapers.text_utils import (clean_spaces, parse_long_date, TableParser,
                                 ungarble_qh)

logger = logging.getLogger(__name__)


class PlenaryAgendas(Task):
    """Parse plenary agendas into bill and plenary-sitting records."""

    async def process_agenda_index(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/290'
        html = await self.crawler.get_html(url)

        agenda_urls = await self.c.gather({self.process_agenda_listing(
            href) for href in html.xpath('//a[@class="h3Style"]/@href')})
        agenda_urls = itertools.chain.from_iterable(agenda_urls)
        return await self.c.gather({self.process_agenda(href)
                                    for href in agenda_urls})

    __call__ = process_agenda_index

    async def process_agenda_listing(self, url, form_data=None, pass_=1):
        html = await self.c.get_html(url, form_data=form_data,
                                     request_method='post')

        # Do a first pass to grab the URLs of all the numbered pages
        if pass_ == 1:
            pages = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
            if pages:
                return itertools.chain.from_iterable(
                    await self.c.gather({self.process_agenda_listing(
                        url,
                        form_data={'page': ''.join(filter(str.isdigit, p))},
                        pass_=2) for p in pages}))
            else:
                return await self.process_agenda_listing(url, pass_=2)
        elif pass_ == 2:
            return html.xpath('//a[@class="h3Style"]/@href')

    async def process_agenda(self, url):
        if url[-4:] == '.pdf':
            payload = await self.c.get_payload(url, decode=True)
            return _parse_pdf_agenda, (url, payload)
        else:
            html = await self.c.get_html(url)
            return _parse_agenda, (url, html)

    @staticmethod
    def after(output):
        # We've got a race condition between `Record.exists` and
        # `Record.insert` and `merge`, so we gotta insert the agendas and
        # the bills one at a time
        bills_and_regs = itertools.chain.from_iterable(fn(*args)
                                                       for fn, args in output)
        for i in bills_and_regs:
            _parse_agenda_bill(*i)


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
                return itertools.chain.from_iterable(map(super().__getitem__,
                                                         key))
            else:
                return super().__getitem__(key)

    ITEM_TYPES = ('13.06',   # Budget-related decision—maybe?
                  '23.01',   # Government bill
                  '23.02',   # Members' bill
                  '23.03',   # Draft regulations
                  '23.04',   # Something or other to do with committees
                  '23.05',   # Debate topic
                  '23.10',   # Decision or draft resolution
                  '23.15')   # Rules of order of the House

    @classmethod
    def _group(cls, item):
        key = item[0][:5]
        if key in cls.ITEM_TYPES:
            return key

    _AgendaItems = namedtuple('AgendaItems', 'part_a part_d bills_and_regs')

    def __new__(cls, url, agenda_items_):
        agenda_items = cls._AgendaItemDict(tuple)
        for k, v in itertools.groupby(agenda_items_, key=cls._group):   # __init__ bypasses __setitem__ (maybe)
            agenda_items[k] = tuple(v)
        if None in agenda_items:
            logger.warning('Unparsed items {} in {!r}'.format(
                tuple(k for k, *_ in agenda_items[None]), url))

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


def _extract_id_and_title(url, item):
    if not item or item.startswith('ΚΕΦΑΛΑΙΟ'):  # Skip headings
        return

    id_ = RE_ID.search(item)
    if not id_:
        logger.info('Unable to extract document type'
                    ' of {!r} in {!r}'.format(item, url))
        return

    # Concatenate the rows into a title string, extract the number,
    # and throw out the junk
    id_, title = id_.group(1), \
        RE_ITEM_NO.sub('', RE_TITLE_OTHER.sub('', item)).rstrip('. ')
    if id_ and title:
        return id_, title
    else:
        logger.warning('Number or title empty in {} in {!r}'.format(
            (id_, title), url))


RE_PP = re.compile(r'_?(\w+?)[\'΄´] ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟ[Δ∆]ΟΣ')
RE_SE = re.compile(r'ΣΥΝΟ[Δ∆]ΟΣ (\w+)[\'΄´]')
RE_SI = re.compile(r'(\d+)[ηή] ?συνεδρίαση')


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
    match = RE_PP.search(text)
    return ungarble_qh(match.group(1)) if match else logger.warning(
        'Unable to extract parliamentary period of {!r}'.format(url))


def extract_session(url, text):
    """Extract and beautify the session number.

    >>> extract_session(..., 'Ι΄ ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ - ΣΥΝΟΔΟΣ Ε΄')
    'Ε'
    """
    match = RE_SE.search(text)
    return ungarble_qh(match.group(1)) if match else logger.warning(
        'Unable to extract session of {!r}'.format(url))


def extract_sitting(url, text):
    """Extract the sitting number.

    >>> extract_sitting(..., '17η συνεδρίαση')
    17
    """
    match = RE_SI.search(text)
    return int(match.group(1)) if match else logger.warning(
        'Unable to extract sitting number of {!r}'.format(url))


def _parse_agenda(url, html):
    agenda_items = (clean_spaces(agenda_item.text_content(),
                                 medial_newlines=True)
                    for agenda_item in html.xpath('//div[@class="articleBox"]'
                                                  '//tr'))
    agenda_items = filter(None, map(_extract_id_and_title,
                                    itertools.repeat(url), agenda_items))
    agenda_items = AgendaItems(url, tuple(agenda_items))

    text = clean_spaces(html.xpath('string(//div[@class="articleBox"])'))

    plenary_sitting = records.PlenarySitting.from_template(
        sources=(url,),
        update={'agenda': {'debate': tuple(i for i, _ in agenda_items.part_d),
                           'legislative_work': tuple(i for i, _ in
                                                     agenda_items.part_a)},
                'date': parse_long_date(clean_spaces(
                    html.xpath('string(//h1)')), plenary=True),
                'links': [{'type': 'agenda', 'url': url}],
                'parliamentary_period': extract_parliamentary_period(url,
                                                                     text),
                'session': extract_session(url, text),
                'sitting': extract_sitting(url, text)})
    try:
        plenary_sitting.merge() if plenary_sitting.exists else \
            plenary_sitting.insert()
    except records.InsertError as e:
        logger.error(e)

    bills_and_regs = dict(agenda_items.bills_and_regs)
    return zip(itertools.repeat(url),
               bills_and_regs.keys(), bills_and_regs.values())


RE_PAGE_NO = re.compile(r'^ +(?:\d+|\w)\n')


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


def _parse_pdf_agenda(url, text):
    if (url == 'http://www.parliament.cy/images/media/redirectfile/'
               '13-0312015- agenda ΤΟΠΟΘΕΤΗΣΕΙΣ doc.pdf'):
        # `TableParser` chokes on its mixed two- and three-col layout
        logger.warning('Skipping {!r} in `_parse_pdf_agenda`'.format(url))
        return ()

    # Split the text at page breaks 'cause the table shifts from page to page
    pages = text.split('\x0c')

    # Getting rid of the page numbers 'cause they might intersect items in the
    # list
    pages_ = (RE_PAGE_NO.sub('', page) for page in pages)
    rows_ = itertools.chain.from_iterable(TableParser(page, 2).rows
                                          for page in pages_)
    # Group rows into tuples, using the leftmost cell as a key, which oughta
    # either contain a list number or be left blank
    agenda_items = (' '.join(i[-1] for i in v)
                    for _, v in itertools.groupby(rows_,
                                                  key=_group_items_of_pdf()))
    agenda_items = filter(None, map(_extract_id_and_title,
                                    itertools.repeat(url), agenda_items))
    agenda_items = AgendaItems(url, tuple(agenda_items))

    plenary_sitting = records.PlenarySitting.from_template(
        sources=(url,),
        update={'agenda': {'debate': tuple(i for i, _ in agenda_items.part_d),
                           'legislative_work': tuple(i for i, _ in
                                                     agenda_items.part_a)},
                'date': parse_long_date(clean_spaces(
                    pages[0], medial_newlines=True), plenary=True),
                'links': [{'type': 'agenda', 'url': url}],
                'parliamentary_period': extract_parliamentary_period(url,
                                                                     text),
                'session': extract_session(url, text),
                'sitting': extract_sitting(url, text)})
    try:
        plenary_sitting.merge() if plenary_sitting.exists else \
            plenary_sitting.insert()
    except records.InsertError as e:
        logger.error(e)

    bills_and_regs = dict(agenda_items.bills_and_regs)
    return zip(itertools.repeat(url),
               bills_and_regs.keys(), bills_and_regs.values())


def _parse_agenda_bill(url, id_, title):
    bill = records.Bill.from_template(
        sources=(url,), update={'identifier': id_, 'title': title})
    if not bill.exists:
        try:
            bill.insert()
        except records.InsertError as e:
            logger.error(e)
