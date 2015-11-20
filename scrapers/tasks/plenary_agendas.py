
"""Parse plenary agendas into bill and plenary-sitting records."""

import logging
import re

from scrapers.records import Bill, PlenarySitting
from scrapers.text_utils import clean_spaces, parse_long_date

logger = logging.getLogger(__name__)

RE_PP = re.compile(r'(\w+)[\'΄] ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ')
RE_SE = re.compile(r'ΣΥΝΟΔΟΣ (\w+)[\'΄]')
RE_SI = re.compile(r'(\d+)[ηή] ?συνεδρίαση')
RE_ID = re.compile(r'([12]3\.[0-9.-]+)')


async def process_agenda_index(crawler):
    url = 'http://www.parliament.cy/easyconsole.cfm/id/290'
    html = await crawler.get_html(url)
    await crawler.enqueue({
        process_agenda_listing(crawler, href)
        for href in html.xpath('//a[@class="h3Style"]/@href')})

HEAD = process_agenda_index


async def process_agenda_listing(crawler, url, form_data=None, pass_=1):
    html = await crawler.get_html(url,
                                  form_data=form_data,
                                  request_method='post')

    # Do a first pass to grab the URLs of all the numbered pages
    if pass_ == 1:
        pagination = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
        if pagination:
            await crawler.enqueue({
                process_agenda_listing(
                    crawler, url,
                    form_data={'page': ''.join(filter(str.isdigit, s))},
                    pass_=2)
                for s in pagination})
        else:
            await process_agenda_listing(crawler, url, pass_=2)
    elif pass_ == 2:
        await crawler.enqueue({
            process_agenda(crawler, href)
            for href in html.xpath('//a[@class="h3Style"]/@href')})


async def process_agenda(crawler, url):
    try:
        html = await crawler.get_html(url)
    except UnicodeDecodeError:
        # Probably a PDF; we might have to insert those manually
        return logger.error('Unable to decode {!r}'.format(url))
    await crawler.exec_blocking(parse_agenda, url, html)


def parse_agenda(url, html):
    """Create plenary records from agendas."""
    def _extract_parliamentary_period(text):
        try:
            return RE_PP.search(text).group()
        except AttributeError:
            logger.error(
                'Unable to extract parliamentary period of {!r}'.format(url))

    def _extract_session(text):
        try:
            return RE_SE.search(text).group()
        except AttributeError:
            logger.error('Unable to extract session of {!r}'.format(url))

    def _extract_sitting(text):
        try:
            return int(RE_SI.search(text).group())
        except AttributeError:
            logger.error(
                'Unable to extract sitting number of {!r}'.format(url))

    def _extract_items(html):
        for e in html.xpath('//div[@class="articleBox"]//tr/td[last()]'):
            try:
                title, *ex, uid = (clean_spaces(e.text_content())
                                   for e in e.xpath('*[self::div or self::p]'))
            except ValueError:
                # Presumably a faux header; skip it
                continue

            title = title.rstrip('.')
            uid = ''.join(ex + [uid])
            try:
                uid = RE_ID.search(uid).group()
                yield uid, title
            except AttributeError:
                logger.error('Unable to extract document type'
                             ' of {!r} in {!r}'.format(title, url))

    body_text = clean_spaces(html.xpath('string(//div[@class="articleBox"])'))

    all_items = dict(_extract_items(html))
    bills = dict(filter(lambda i: i[0].startswith(('23.01', '23.02', '23.03')),
                        all_items.items()))
    debate_topics = dict(filter(lambda i: i[0].startswith('23.05'),
                                all_items.items()))
    unparsed = all_items.keys() - bills.keys() - debate_topics.keys()
    if unparsed:
        logger.warning('Unparsed items {} in {!r}'.format(unparsed, url))

    plenary_sitting = PlenarySitting.from_template({
        'agenda': {'debate': list(debate_topics.keys()),
                   'legislative_work': list(bills.keys())},
        'date': parse_long_date(clean_spaces(html.xpath('string(//h1)')),
                                plenary=True),
        'links': [{'type': 'agenda', 'url': url}],
        'parliamentary_period': _extract_parliamentary_period(body_text),
        'session': _extract_session(body_text),
        'sitting': _extract_sitting(body_text)})
    if not plenary_sitting.insert():
        logger.warning('Unable to insert or update plenary on {!r}'
                       ' in {!r}'.format(plenary_sitting['date'], url))

    for uid, title in bills.items():
        parse_agenda_bill(url, uid, title)


def parse_agenda_bill(url, uid, title):
    """Create records of bills from agendas."""
    bill = Bill.from_template({'_filename': '{}.yaml'.format(uid),
                               'identifier': uid,
                               'title': title})
    if not bill.insert():
        logger.warning(
            'Unable to insert or update bill with id {!r} and title {!r}'
            ' from {!r}'.format(bill['identifier'], bill['title'], url))
