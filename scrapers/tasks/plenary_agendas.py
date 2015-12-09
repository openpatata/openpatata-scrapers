
from collections import OrderedDict as odict
import itertools
import logging
import re

from scrapers.crawling import Task
from scrapers.records import Bill, PlenarySitting
from scrapers.text_utils import (clean_spaces, parse_long_date, pdf2text,
                                 TableParser)

logger = logging.getLogger(__name__)


class PlenaryAgendas(Task):
    """Parse plenary agendas into bill and plenary-sitting records."""

    name = 'plenary_agendas'

    async def process_agenda_index(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/290'
        html = await self.crawler.get_html(url)

        agenda_urls = await self.crawler.gather(
            {self.process_agenda_listing(href)
             for href in html.xpath('//a[@class="h3Style"]/@href')})
        agenda_urls = itertools.chain.from_iterable(agenda_urls)
        await self.crawler.gather({self.process_agenda(href)
                                   for href in agenda_urls})

    __call__ = process_agenda_index

    async def process_agenda_listing(self, url, form_data=None, pass_=1):
        html = await self.crawler.get_html(url,
                                           form_data=form_data,
                                           request_method='post')

        # Do a first pass to grab the URLs of all the numbered pages
        if pass_ == 1:
            pages = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
            if pages:
                return itertools.chain.from_iterable(
                    await self.crawler.gather({self.process_agenda_listing(
                        url,
                        form_data={'page': ''.join(filter(str.isdigit, p))},
                        pass_=2) for p in pages}))
            else:
                return await self.process_agenda_listing(url, pass_=2)
        elif pass_ == 2:
            return html.xpath('//a[@class="h3Style"]/@href')

    async def process_agenda(self, url):
        if url[-4:] == '.pdf':
            text = await self.crawler.get_payload(url)
            text = await self.crawler.exec_blocking(pdf2text, text)
            await self.crawler.exec_blocking(parse_pdf_agenda, url, text)
        else:
            html = await self.crawler.get_html(url)
            await self.crawler.exec_blocking(parse_agenda, url, html)


RE_PP = re.compile(r'(\w+)[\'΄] ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ')
RE_SE = re.compile(r'ΣΥΝΟΔΟΣ (\w+)[\'΄]')
RE_SI = re.compile(r'(\d+)[ηή] ?συνεδρίαση')
RE_ID = re.compile(r'([12]3\.[0-9.-]+)')


def _extract_parliamentary_period(url, text):
    match = RE_PP.search(text)
    return match.group(1) if match else logger.warning(
        'Unable to extract parliamentary period of {!r}'.format(url))


def _parse_session(url, text):
    match = RE_SE.search(text)
    return match.group(1) if match else logger.warning(
        'Unable to extract session of {!r}'.format(url))


def _parse_sitting(url, text):
    match = RE_SI.search(text)
    return int(match.group(1)) if match else logger.warning(
        'Unable to extract sitting number of {!r}'.format(url))


def _parse_agenda_items(url, html):
    for e in html.xpath('//div[@class="articleBox"]//tr/td[last()]'):
        try:
            title, *ex, id_ = (clean_spaces(e.text_content())
                               for e in e.xpath('*[self::div or self::p]'))
        except ValueError:
            # Presumably a faux header; skip it
            continue

        id_ = RE_ID.search(''.join(ex + [id_]))
        if id_:
            yield id_.group(1), title.rstrip('.')
        else:
            logger.warning('Unable to extract document type'
                           ' of {!r} in {!r}'.format(title, url))


def parse_agenda(url, html):
    all_items = odict(_parse_agenda_items(url, html))
    bills = odict((k, v) for k, v in all_items.items()
                  if k.startswith(('23.01', '23.02', '23.03')))
    debate_topics = odict((k, v) for k, v in all_items.items()
                          if k.startswith('23.05'))

    unparsed = all_items.keys() - bills.keys() - debate_topics.keys()
    if unparsed:
        logger.warning('Unparsed items {} in {!r}'.format(unparsed, url))

    body_text = clean_spaces(html.xpath('string(//div[@class="articleBox"])'))

    plenary_sitting = PlenarySitting.from_template(
        None,
        {'agenda': {'debate': list(debate_topics.keys()) or None,
                    'legislative_work': list(bills.keys()) or None},
         'date': parse_long_date(clean_spaces(html.xpath('string(//h1)')),
                                 plenary=True),
         'links': [{'type': 'agenda', 'url': url}],
         'parliamentary_period': _extract_parliamentary_period(url, body_text),
         'session': _parse_session(url, body_text),
         'sitting': _parse_sitting(url, body_text)})
    if not plenary_sitting.insert():
        logger.warning('Unable to insert or update plenary on {!r}'
                       ' in {!r}'.format(plenary_sitting['date'], url))

    for uid, title in bills.items():
        parse_agenda_bill(url, uid, title)


RE_PAGENO = re.compile(r'^ +\d+\n')
RE_TITLE_OTHER = re.compile(r'\(Πρόταση νόμου[^\)]*\)')


def parse_pdf_agenda(url, text):
    def _group_by_key(key, _store=[None]):
        # Return a previous valid key until a new valid key is found
        if not key[0]:
            return _store[0]
        else:
            _store[0] = key[0]
            return _store[0]

    def _prepare_item(key, value):
        id_ = RE_ID.search(key)
        if not id_:
            return logger.warning('Unable to extract document type of {}'
                                  ' in {!r}'.format((key, value), url))
        return (id_.group(1),
                RE_TITLE_OTHER.sub('', ' '.join(value)).rstrip('. '))

    if (url == 'http://www.parliament.cy/images/media/redirectfile/13-0312015-'
               ' agenda ΤΟΠΟΘΕΤΗΣΕΙΣ doc.pdf'):
        # `TableParser` chokes on its mixed two- and three-col layout
        logger.warning('Skipping {!r} in `parse_pdf_agenda`'.format(url))
        return

    pages = text.split('\x0c')
    rows = itertools.chain.from_iterable(
        # Get rid of the page numbers first
        TableParser(RE_PAGENO.sub('', page), 2).rows for page in pages)

    # Parse the pages into an array of agenda items, grouped by their list
    # number, each containing an array of table rows
    agenda_items = (tuple(x[-1] for x in v)
                    for _, v in itertools.groupby(rows, key=_group_by_key))
    # Pop from the end of the list to create a two-tuple; the assumption's
    # that the bottom line contains the item's id
    agenda_items = ((v[-1], v[:-1]) for v in agenda_items)
    # Concatenate the rows into a string, extract the id from the key,
    # and throw out the junk
    agenda_items = filter(None, itertools.starmap(_prepare_item, agenda_items))
    agenda_items = odict(agenda_items)

    bills = odict((k, v) for k, v in agenda_items.items()
                  if k.startswith(('23.01', '23.02', '23.03')))
    debate_topics = odict((k, v) for k, v in agenda_items.items()
                          if k.startswith('23.05'))

    unparsed = agenda_items.keys() - bills.keys() - debate_topics.keys()
    if unparsed:
        logger.warning('Unparsed items {} in {!r}'.format(unparsed, url))

    plenary_sitting = PlenarySitting.from_template(
        None,
        {'agenda': {'debate': list(debate_topics.keys()) or None,
                    'legislative_work': list(bills.keys()) or None},
         'date': parse_long_date(clean_spaces(pages[0], medial_newlines=True),
                                 plenary=True),
         'links': [{'type': 'agenda', 'url': url}],
         'parliamentary_period': _extract_parliamentary_period(url, text),
         'session': _parse_session(url, text),
         'sitting': _parse_sitting(url, text)})
    if not plenary_sitting.insert():
        logger.warning('Unable to insert or update plenary on {!r}'
                       ' in {!r}'.format(plenary_sitting['date'], url))

    for uid, title in bills.items():
        parse_agenda_bill(url, uid, title)


def parse_agenda_bill(url, uid, title):
    bill = Bill.from_template(None,
                              {'identifier': uid, 'title': title})
    if not bill.insert():
        logger.warning('Unable to insert or update bill {}'
                       ' in {!r}'.format(bill, url))
