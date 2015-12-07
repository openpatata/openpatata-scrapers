
import itertools
import logging

from scrapers.crawling import Task
from scrapers.records import CommitteeReport
from scrapers.text_utils import clean_spaces, parse_short_date

logger = logging.getLogger(__name__)


class CommitteeReports(Task):
    """Parse committee reports into individual records."""

    name = 'committee_reports'

    async def process_committee_report_index(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/220'
        html = await self.crawler.get_html(url)

        committee_reports = await self.crawler.gather(
            {self.process_committee_report_listing(href)
             for href in html.xpath('//a[@class="h3Style"]/@href')})
        committee_reports = itertools.chain.from_iterable(committee_reports)
        await self.crawler.gather(
            {self.crawler.exec_blocking(parse_committee_report,
                                        url, date, item)
             for date, item in committee_reports})

    __call__ = process_committee_report_index

    async def process_committee_report_listing(self, url):
        html = await self.crawler.get_html(url)
        return await self.crawler.exec_blocking(parse_committee_report_listing,
                                                html, url)


def parse_committee_report_listing(html, url):
    date = None
    for item in html.xpath('//td/*[self::ul or self::p]'):
        if item.tag == 'p':
            try:
                date = parse_short_date(clean_spaces(item.text_content()))
            except ValueError as e:
                logger.warning(e)
        elif item.tag == 'ul':
            yield date, item


def parse_committee_report(url, date, item):
    committee_report = CommitteeReport.from_template(
        None,
        {'date_circulated': date,
         'title': clean_spaces(item.text_content(),
                               medial_newlines=True),
         'url': item.xpath('.//a[1]/@href')[0]})
    if not committee_report.insert():
        logger.error('Unable to insert or update'
                     ' committee report {}'.format(committee_report))
