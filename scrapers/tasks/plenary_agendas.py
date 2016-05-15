
from collections import defaultdict, namedtuple
import functools as ft
import itertools as it
import logging
import re

from ._models import Bill, PlenaryAgenda, PlenaryAgendaLink, PlenarySitting
from ..crawling import Task
from ..text_utils import (clean_spaces, parse_long_date, TableParser,
                          ungarble_qh)

logger = logging.getLogger(__name__)


class PlenaryAgendas(Task):
    """Parse plenary agendas into bill and plenary-sitting records."""

    async def process_agenda_index(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/290'
        html = await self.c.get_html(url)

        agenda_urls = await self.c.gather(
            {self.process_multi_page_listing(href)
             for href in html.xpath('//a[@class="h3Style"]/@href')})
        agenda_urls = it.chain.from_iterable(agenda_urls)
        return await self.c.gather({self.process_agenda(href)
                                    for href in agenda_urls})

    __call__ = process_agenda_index

    async def process_multi_page_listing(self, url):
        html = await self.c.get_html(url)
        pages = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
        if pages:
            pages = {self.process_multi_page_page(
                      url, form_data={'page': ''.join(filter(str.isdigit, p))})
                     for p in pages}
            return it.chain.from_iterable(await self.c.gather(pages))
        else:
            return await self.process_multi_page_page(url)

    async def process_multi_page_page(self, url, form_data=None):
        html = await self.c.get_html(url, form_data=form_data,
                                     request_method='post')
        return html.xpath('//a[@class="h3Style"]/@href')

    async def process_agenda(self, url):
        if url[-4:] == '.pdf':
            _, payload = await self.c.get_payload(url, decode=True)
            return _parse_pdf_agenda, (url, payload)
        else:
            html = await self.c.get_html(url)
            return _parse_agenda, (url, html)

    def after(output):
        for url, date, text, agenda_items in \
                filter(None, (fn(url, c) for fn, (url, c) in output)):
            plenary_sitting = PlenarySitting(
                _sources=[url],
                agenda=PlenaryAgenda(cap1=[i for i, _ in agenda_items.cap1],
                                     cap4=[i for i, _ in agenda_items.cap4]),
                date=date,
                links=[PlenaryAgendaLink(type='agenda', url=url)],
                parliamentary_period=extract_parliamentary_period(url, text),
                session=extract_session(url, text),
                sitting=extract_sitting(url, text))
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
            logger.warning('Unparsed items {} in {!r}'
                           .format(tuple(k for k, *_ in agenda_items[None]),
                                   url))

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
        logger.info('Unable to extract document type of {!r} in {!r}'
                    .format(item, url))
        return

    # Concatenate the rows into a title string, extract the number,
    # and throw out the junk
    id_, title = id_.group(1), \
        RE_ITEM_NO.sub('', RE_TITLE_OTHER.sub('', item)).rstrip('. ')
    if id_ and title:
        return id_, title
    else:
        logger.warning('Number or title empty in {} in {!r}'
                       .format((id_, title), url))


RE_PPERIOD = re.compile(r'_?(\w+?)[\'΄´] ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟ[Δ∆]ΟΣ')
RE_SESSION = re.compile(r'ΣΥΝΟ[Δ∆]ΟΣ (\w+)[\'΄´]')
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
    return ungarble_qh(match.group(1)) if match else logger.warning(
        'Unable to extract parliamentary period of {!r}'.format(url))


def extract_session(url, text):
    """Extract and beautify the session number.

    >>> extract_session(..., 'Ι΄ ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ - ΣΥΝΟΔΟΣ Ε΄')
    'Ε'
    """
    match = RE_SESSION.search(text)
    return ungarble_qh(match.group(1)) if match else logger.warning(
        'Unable to extract session of {!r}'.format(url))


def extract_sitting(url, text):
    """Extract the sitting number.

    >>> extract_sitting(..., '17η συνεδρίαση')
    17
    """
    match = RE_SITTING_NUMBER.search(text)
    return int(match.group(1)) if match else logger.warning(
        'Unable to extract sitting number of {!r}'.format(url))


RE_JUNK = re.compile(r'^ *([\.…]+)', re.MULTILINE)


def _parse_agenda(url, html):
    text = clean_spaces(html.xpath('string(//div[@class="articleBox"])'))

    agenda_items = (clean_spaces(RE_JUNK.sub('', agenda_item.text_content()),
                                 medial_newlines=True)
                    for agenda_item in html.xpath('//div[@class="articleBox"]'
                                                  '//tr'))
    agenda_items = filter(None, map(_extract_id_and_title,
                                    it.repeat(url), agenda_items))
    agenda_items = AgendaItems(url, tuple(agenda_items))
    return url, \
        parse_long_date(clean_spaces(html.xpath('string(//h1)')), plenary=True), \
        text, agenda_items


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


def _clean_title_text(text):
    return ft.reduce(lambda s, j: ''.join((s[:j.start()], ' '*len(j.group(1)),
                                           s[j.end():])),
                     RE_JUNK.finditer(text), text)


def _parse_pdf_agenda(url, text):
    if (url == 'http://www.parliament.cy/images/media/redirectfile/'
               '13-0312015- agenda ΤΟΠΟΘΕΤΗΣΕΙΣ doc.pdf'):
        # `TableParser` chokes on its mixed two- and three-col layout
        logger.warning('Skipping {!r} in `_parse_pdf_agenda`'.format(url))
        return

    # Get rid of the page numbers 'cause they might intersect items in
    # the list
    pages = RE_PAGE_NO.sub('', _clean_title_text(text))
    # And split the text at page breaks 'cause the table shifts from page to
    # page
    pages = tuple(filter(None, pages.split('\x0c')))

    rows_ = it.chain.from_iterable(TableParser(page).rows for page in pages)
    # Group rows into tuples, using the leftmost cell as a key, which oughta
    # either contain a list number or be left blank
    agenda_items = (' '.join(i[-1] for i in v)
                    for _, v in it.groupby(rows_, key=_group_items_of_pdf()))
    agenda_items = filter(None, map(_extract_id_and_title,
                                    it.repeat(url), agenda_items))
    agenda_items = AgendaItems(url, tuple(agenda_items))
    return url, \
        parse_long_date(clean_spaces(pages[0], medial_newlines=True), plenary=True), \
        text, agenda_items
