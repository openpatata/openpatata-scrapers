
import itertools as it
import logging

from . import _models as models
from ..crawling import Task
from ..text_utils import clean_spaces, parse_short_date

logger = logging.getLogger(__name__)


class CommitteeReports(Task):
    """Create committee-report records from their listings."""

    async def process_committee_report_index(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/220'
        html = await self.c.get_html(url)

        committee_reports = \
            await self.c.gather({self.process_committee_report_listing(
                href) for href in html.xpath('//a[@class="h3Style"]/@href')})
        committee_reports = it.chain.from_iterable(committee_reports)
        return committee_reports

    __call__ = process_committee_report_index

    async def process_committee_report_listing(self, url):
        html = await self.c.get_html(url, clean=True)
        return _parse_committee_report_listing(url, html)

    def after(output):
        for committee_report in output:
            _parse_committee_report(*committee_report)


def _parse_committee_report_listing(url, html):
    date = None
    for item in html.xpath('//td/*[self::ul or self::p]'):
        if item.tag == 'p':
            date_ = clean_spaces(item.text_content())
            if not date_:
                continue
            try:
                date = parse_short_date(date_)
            except ValueError as e:
                logger.warning(e)
        elif item.tag == 'ul':
            yield url, date, item


def _parse_committee_report(url, date, item):
    try:
        link = item.xpath('.//a[1]/@href')[0]
    except IndexError:
        logger.error('Unable to extract link'
                     ' from {} in {!r}'.format(item.text_content(), url))
        return

    committee_report = models.CommitteeReport(
        _sources=[url],
        date_circulated=date,
        title=clean_spaces(item.text_content(), medial_newlines=True),
        url=link)
    try:
        committee_report.insert(merge=committee_report.exists)
    except models.InsertError as e:
        logger.error(e)
