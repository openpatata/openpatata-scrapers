
from collections import OrderedDict as odict
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
RE_ID = re.compile(r'([12]3\.[0-9.-]+)')


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


def _arrange_agenda_items(url, items):
    # 23.01        government bill
    # 23.02        member's bill
    # 23.03        draft regulations
    # 23.04        something or other to do with committees
    # 23.05        debate topic
    # 23.10.100    draft resolution
    # 23.10.200    decision
    # 23.15        house rules of procedure
    bills = odict((k, v) for k, v in items.items()
                  if k.startswith(('23.01', '23.02', '23.03')))
    debate_topics = odict((k, v) for k, v in items.items()
                          if k.startswith('23.05'))
    unparsed = items.keys() - bills.keys() - debate_topics.keys()
    if unparsed:
        logger.warning('Unparsed items {} in {!r}'.format(unparsed, url))
    return bills, debate_topics


def parse_agenda(url, html):
    agenda_items_ = []
    for e in html.xpath('//div[@class="articleBox"]//tr/td[last()]'):
        try:
            title, *ex, id_ = (clean_spaces(e.text_content())
                               for e in e.xpath('*[self::div or self::p]'))
        except ValueError:
            # Presumably a faux header; skip it
            continue

        id_ = RE_ID.search(''.join(ex + [id_]))
        if id_:
            agenda_items_.append((id_.group(1), title.rstrip('.')))
        else:
            logger.warning('Unable to extract document type'
                           ' of {!r} in {!r}'.format(title, url))

    text = clean_spaces(html.xpath('string(//div[@class="articleBox"])'))
    bills, debate_topics = _arrange_agenda_items(url, odict(agenda_items_))

    plenary_sitting = records.PlenarySitting.from_template(
        filename=None, sources=(url,),
        update={'agenda': {'debate': list(debate_topics.keys()),
                           'legislative_work': list(bills.keys())},
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
    return zip(itertools.repeat(url), bills.keys(), bills.values())


RE_PAGE_NO = re.compile(r'^ +(?:\d+|\w)\n')
RE_TITLE_OTHER = re.compile(r'\(Πρόταση νόμου[^\)]*\)')


def _extract_id_and_title(url, item_):
    if not any(item_):
        return

    item = itertools.dropwhile(lambda v: not RE_ID.search(v), reversed(item_))
    item = tuple(reversed(tuple(item)))   # ...
    if not item:
        logger.warning('Unable to extract document type of {}'
                       ' in {!r}'.format(item_, url))
        return

    id_, title = item[-1], item[:-1]
    return (RE_ID.search(id_).group(1),
            RE_TITLE_OTHER.sub('', ' '.join(title)).rstrip('. '))


def _group_items():
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

    pages = text.split('\x0c')

    # Getting rid of the page numbers 'cause they might intersect items in the
    # list
    pages_ = (RE_PAGE_NO.sub('', page) for page in pages)
    rows_ = itertools.chain.from_iterable(TableParser(page, 2).rows
                                          for page in pages_)
    # Group rows into tuples, using the leftmost cell as a key, which oughta
    # either contain a list number or be left blank
    agenda_items_ = (tuple(zip(*v))[-1]
                     for _, v in itertools.groupby(rows_, key=_group_items()))
    # Concatenate the rows into a title string, extract the id, and throw out
    # the junk; the output's an array of id–title two-tuples
    agenda_items_ = filter(None, (_extract_id_and_title(url, agenda_item)
                                  for agenda_item in agenda_items_))
    bills, debate_topics = _arrange_agenda_items(url, odict(agenda_items_))

    plenary_sitting = records.PlenarySitting.from_template(
        filename=None, sources=(url,),
        update={'agenda': {'debate': list(debate_topics.keys()),
                           'legislative_work': list(bills.keys())},
                'date': parse_long_date(clean_spaces(pages[0],
                                                     medial_newlines=True),
                                        plenary=True),
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
    return zip(itertools.repeat(url), bills.keys(), bills.values())


def parse_agenda_bill(url, uid, title):
    bill = records.Bill.from_template(
        filename=None, sources=(url,), update={'identifier': uid,
                                               'title': title})
    try:
        bill.insert()
    except records.InsertError as e:
        logger.error(e)
