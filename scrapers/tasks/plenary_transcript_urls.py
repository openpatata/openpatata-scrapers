
"""Add transcript links to plenary-sitting records."""

import logging

from scrapers.records import PlenarySitting
from scrapers.text_utils import parse_transcript_date

logger = logging.getLogger(__name__)


async def process_transcript_index(crawler):
    url = 'http://www.parliament.cy/easyconsole.cfm/id/159'
    html = await crawler.get_html(url)
    await crawler.enqueue({
        process_transcript_listing(crawler, href)
        for href in html.xpath('//a[@class="h3Style"]/@href')})

HEAD = process_transcript_index


async def process_transcript_listing(crawler, url):
    html = await crawler.get_html(url)
    await crawler.exec_blocking(parse_transcript_listing, url, html)


def parse_transcript_listing(url, html):
    for href, date, date_success in (
            (href, *parse_transcript_date(href))
            for href in html.xpath('//a[contains(@href, "praktiko")]/@href')):
        if not date_success:
            logger.error('Unable to extract date {!r} from transcript'
                         ' listing at {!r}'.format(date, url))
            continue

        plenary_sitting = PlenarySitting(
            {'_filename': '{}.yaml'.format(PlenarySitting.select_date(date)),
             'links': [{'type': 'transcript', 'url': href}]})
        if not plenary_sitting.merge():
            logger.warning(
                'Unable to insert transcript with URL {!r} in plenary with'
                ' filename {!r}'.format(url, plenary_sitting['_filename']))
