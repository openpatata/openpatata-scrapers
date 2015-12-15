
from collections import defaultdict, namedtuple
import functools
import itertools
import logging
import re

from scrapers.crawling import Task
from scrapers import records
from scrapers.text_utils import (clean_spaces, parse_long_date, pdf2text,
                                 TableParser)

logger = logging.getLogger(__name__)


class PlenaryAgendas(Task):
    """Parse plenary agendas into bill and plenary-sitting records."""

    name = 'plenary_agendas'

    async def process_agenda_index(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/290'
        html = await self.crawler.get_html(url)

        agenda_urls = await self.c.gather({self.process_agenda_listing(
            href) for href in html.xpath('//a[@class="h3Style"]/@href')})
        agenda_urls = itertools.chain.from_iterable(agenda_urls)

        bills = await self.c.gather({self.process_agenda(href)
                                     for href in agenda_urls})
        bills = itertools.chain.from_iterable(bills)
        await self.c.gather({self.c.exec_blocking(parse_agenda_bill, *bill)
                             for bill in bills})

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
            payload = await self.c.get_payload(url)
            text = await self.c.exec_blocking(pdf2text, payload)
            return await self.c.exec_blocking(parse_pdf_agenda, url, text)
        else:
            html = await self.c.get_html(url)
            return await self.c.exec_blocking(parse_agenda, url, html)


RE_PP = re.compile(r'(\w+)[\'΄] ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟ[Δ∆]ΟΣ')
RE_SE = re.compile(r'ΣΥΝΟ[Δ∆]ΟΣ (\w+)[\'΄]')
RE_SI = re.compile(r'(\d+)[ηή] ?συνεδρίαση')
RE_ID = re.compile(r'([12]3\. ?[0-9.-]+)')
RE_TITLE_OTHER = re.compile(r'\. *\(.*')


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

    ITEM_TYPES = ('13.06',   # decision?
                  '23.01',   # government bill
                  '23.02',   # member's bill
                  '23.03',   # draft regulations
                  '23.04',   # something or other to do with committees
                  '23.05',   # debate topic
                  '23.10',   # decision or draft resolution
                  '23.15')   # house rules of procedure

    @classmethod
    def _group(cls, item):
        key = item[0][:5]
        if key in cls.ITEM_TYPES:
            return key

    _AgendaItems = namedtuple('AgendaItems', 'part_a part_d bills_and_regs')

    def __new__(cls, url, agenda_items_):
        agenda_items__ = itertools.groupby(agenda_items_, key=cls._group)
        agenda_items = cls._AgendaItemDict(tuple)
        for k, v in agenda_items__:   # __init__ bypasses __setitem__ (maybe)
            agenda_items[k] = tuple(v)
        if None in agenda_items:
            logger.warning('Unparsed items {} in {!r}'.format(
                tuple(k for k, _ in agenda_items[None]), url))

        return cls._AgendaItems(
            *map(functools.partial(sorted, key=agenda_items_.index),
                 (agenda_items['13.06', '23.01', '23.02', '23.03', '23.10',
                               '23.15'],
                  agenda_items['23.04', '23.05'],
                  agenda_items['23.01', '23.02', '23.03']
                  )))


def _extract_id_and_title(url, item):
    if not item:
        return

    id_ = RE_ID.search(item)
    if not id_:
        logger.info('Unable to extract document type'
                    ' of {!r} in {!r}'.format(item, url))
        return

    id_, title = id_.group(1), RE_TITLE_OTHER.sub('', item).rstrip('. ')
    if id_ and title:
        return id_, title
    else:
        logger.warning('Id or title empty in {} in {!r}'.format((id_, title),
                                                                url))


def _extract_parliamentary_period(url, text):
    match = RE_PP.search(text)
    return match.group(1) if match else logger.warning(
        'Unable to extract parliamentary period of {!r}'.format(url))


def _extract_session(url, text):
    match = RE_SE.search(text)
    return match.group(1) if match else logger.warning(
        'Unable to extract session of {!r}'.format(url))


def _extract_sitting(url, text):
    match = RE_SI.search(text)
    return int(match.group(1)) if match else logger.warning(
        'Unable to extract sitting number of {!r}'.format(url))


def parse_agenda(url, html):
    agenda_items = (clean_spaces(agenda_item.text_content(),
                                 medial_newlines=True)
                    for agenda_item in html.xpath('//div[@class="articleBox"]'
                                                  '//tr/td[last()]'))
    agenda_items = filter(None, map(_extract_id_and_title,
                                    itertools.repeat(url), agenda_items))
    agenda_items = AgendaItems(url, tuple(agenda_items))

    text = clean_spaces(html.xpath('string(//div[@class="articleBox"])'))

    plenary_sitting = records.PlenarySitting.from_template(
        filename=None, sources=(url,),
        update={'agenda': {'debate': tuple(i for i, _ in agenda_items.part_d),
                           'legislative_work': tuple(i for i, _ in
                                                     agenda_items.part_a)},
                'date': parse_long_date(clean_spaces(
                    html.xpath('string(//h1)')), plenary=True),
                'links': [{'type': 'agenda', 'url': url}],
                'parliamentary_period': _extract_parliamentary_period(url,
                                                                      text),
                'session': _extract_session(url, text),
                'sitting': _extract_sitting(url, text)})
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


def parse_pdf_agenda(url, text):
    if (url == 'http://www.parliament.cy/images/media/redirectfile/'
               '13-0312015- agenda ΤΟΠΟΘΕΤΗΣΕΙΣ doc.pdf'):
        # `TableParser` chokes on its mixed two- and three-col layout
        logger.warning('Skipping {!r} in `parse_pdf_agenda`'.format(url))
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
    # Concatenate the rows into a title string, extract the id, and throw out
    # the junk; the output's an array of id–title two-tuples
    agenda_items = filter(None, map(_extract_id_and_title,
                                    itertools.repeat(url), agenda_items))
    agenda_items = AgendaItems(url, tuple(agenda_items))

    plenary_sitting = records.PlenarySitting.from_template(
        filename=None, sources=(url,),
        update={'agenda': {'debate': tuple(i for i, _ in agenda_items.part_d),
                           'legislative_work': tuple(i for i, _ in
                                                     agenda_items.part_a)},
                'date': parse_long_date(clean_spaces(
                    pages[0], medial_newlines=True), plenary=True),
                'links': [{'type': 'agenda', 'url': url}],
                'parliamentary_period': _extract_parliamentary_period(url,
                                                                      text),
                'session': _extract_session(url, text),
                'sitting': _extract_sitting(url, text)})
    try:
        plenary_sitting.merge() if plenary_sitting.exists else \
            plenary_sitting.insert()
    except records.InsertError as e:
        logger.error(e)

    bills_and_regs = dict(agenda_items.bills_and_regs)
    return zip(itertools.repeat(url),
               bills_and_regs.keys(), bills_and_regs.values())


def parse_agenda_bill(url, uid, title):
    bill = records.Bill.from_template(None, (url,),
                                      {'identifier': uid, 'title': title})
    if not bill.exists:
        try:
            bill.insert()
        except records.InsertError as e:
            logger.error(e)
