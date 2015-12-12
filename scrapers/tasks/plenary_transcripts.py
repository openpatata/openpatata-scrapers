
import itertools
import logging
import re

from scrapers.crawling import Task
from scrapers import records
from scrapers.text_utils import (apply_subs, decipher_name,
                                 parse_transcript_date, pdf2text, TableParser)

logger = logging.getLogger(__name__)


class PlenaryAttendance(Task):
    """Extract MPs in attendance at plenary sittings from transcripts."""

    name = 'plenary_attendance'

    async def process_transcript_listings(self):
        listing_urls = (
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IA.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IB.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IES.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IC.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IDS.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_ID.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IE.htm')

        transcript_urls = await self.c.gather({self.process_transcript_listing(
            url) for url in listing_urls})
        transcript_urls = itertools.chain.from_iterable(transcript_urls)
        await self.c.gather({self.process_transcript(url)
                             for url in transcript_urls})

    __call__ = process_transcript_listings

    async def process_transcript_listing(self, url):
        html = await self.c.get_html(url)
        return html.xpath('//a[contains(@href, "praktiko")]/@href')

    async def process_transcript(self, url):
        if url[-4:] != '.pdf':
            logger.warning('We are only able to parse PDF transcripts;'
                           ' skipping {!r}'.format(url))
            return

        text = await self.c.exec_blocking(pdf2text,
                                          await self.c.get_payload(url))
        await self.c.exec_blocking(parse_transcript, url, text)


RE_ATTENDEES = re.compile(r'[\n\x0c] *🌮(.*?)🌯', re.DOTALL)
SUBS = [('Παρόντες βουλευτές', '🌮'),
        ('Παρόνηες βοσλεσηές', '🌮'),      # 2015-04-02-1
        ('Παξόληεο βνπιεπηέο', '🌮'),      # 2014-10-23, 2015-04-02-2
        ('(Ώρα λήξης: 6.15 μ.μ.)', '🌮'),  # 2015-03-19
        ('Παρόντες αντιπρόσωποι θρησκευτικών ομάδων', '🌯'),
        ('Παρόνηες ανηιπρόζωποι θρηζκεσηικών ομάδων', '🌯'),  # 2015-04-02-1
        ('Παξόληεο αληηπξόζσπνη ζξεζθεπηηθώλ νκάδσλ', '🌯'),  # 2015-04-02-2
        ('Αντιπρόσωποι θρησκευτικών ομάδων', '🌯'),  # 2014-10-23
        ('Περιεχόμενα', '🌯'),
        ('ΠΕΡΙΕΧΟΜΕΝΑ', '🌯'),
        ('ΠΔΡΙΔΥΟΜΔΝΑ', '🌯'),  # 2014-10-23
        # Spelling error in 2014-11-20
        ('Χαμπουλάς Ευγένιος', 'Χαμπουλλάς Ευγένιος')]


def _parse_attendee_name(name, url):
    if name.isdigit():
        return      # Skip page numbers

    new_name = decipher_name(name)
    if not new_name:
        logger.warning('Unable to pair name {!r} with MP on record'
                       ' while processing {!r}'.format(name, url))
        return
    if name != new_name:
        logger.info('Name {!r} converted to {!r} while'
                    ' processing {!r}'.format(name, new_name, url))
    return new_name


def _parse_attendees(attendee_table, date, text, url):
    # Split at page breaks 'cause the columns will have likely shifted
    attendee_table = attendee_table.split('\x0c')

    # Assume 3-col layout 'cause they've all got leading whitespace
    attendees = itertools.chain.from_iterable(TableParser(t, 3).values
                                              for t in attendee_table)
    attendees = filter(None, (_parse_attendee_name(a, url) for a in attendees))
    # The President's not listed among the attendees
    if 'ΠΡΟΕΔΡΟΣ:' in text or date in {'2015-04-02_1', '2015-04-02_2'}:
        attendees = itertools.chain(attendees, ('Ομήρου Γιαννάκης',))
    return sorted(attendees)


def parse_transcript(url, text):
    date, date_success = parse_transcript_date(url)
    if not date_success:
        logger.error('Unable to extract date from filename of'
                     ' transcript at {!r}'.format(url))
        return

    text = apply_subs(text, SUBS)
    try:
        attendee_table = RE_ATTENDEES.search(text).group(1)
    except AttributeError:
        logger.error('Unable to extract attendee table from'
                     ' transcript at {!r}'.format(url))
        return

    plenary_sitting = records.PlenarySitting.from_template(
        filename=date, sources=(url,),
        update={'attendees': _parse_attendees(attendee_table,
                                              date, text, url)})
    try:
        plenary_sitting.merge()
    except records.InsertError as e:
        logger.error(e)


class PlenaryTranscriptUrls(Task):
    """Add transcript links to plenary-sitting records."""

    name = 'plenary_transcript_urls'

    async def process_transcript_index(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/159'
        html = await self.c.get_html(url)
        await self.c.gather({self.process_transcript_listing(
            href) for href in html.xpath('//a[@class="h3Style"]/@href')})

    __call__ = process_transcript_index

    async def process_transcript_listing(self, url):
        html = await self.c.get_html(url)
        await self.c.exec_blocking(parse_transcript_listing, url, html)


def parse_transcript_listing(url, html):
    for href, date, date_success in \
            ((href, *parse_transcript_date(href))
             for href in html.xpath('//a[contains(@href, "praktiko")]/@href')):
        if not date_success:
            logger.error('Unable to extract date {!r} from transcript'
                         ' listing at {!r}'.format(date, url))
            continue

        plenary_sitting = records.PlenarySitting.from_template(
            filename=date, sources=(url,),
            update={'links': [{'type': 'transcript', 'url': href}]})
        try:
            plenary_sitting.merge()
        except records.InsertError as e:
            logger.error(e)
