
"""Extract MPs in attendance at a plenary sitting from their transcripts."""

import itertools
import logging
import re

from scrapers.records import PlenarySitting
from scrapers.text_utils import (apply_subs,
                                 decipher_name,
                                 parse_transcript_date,
                                 pdf2text,
                                 TableParser)

logger = logging.getLogger(__name__)

RE_ATTENDEES = re.compile(r'[\n\x0c] *🌮(.*?)🌯', re.DOTALL)


async def process_transcript_listings(crawler):
    urls = ('http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IA.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IB.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IES.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IC.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IDS.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_ID.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IE.htm')
    await crawler.enqueue({
        process_transcript_listing(crawler, url) for url in urls})

HEAD = process_transcript_listings


async def process_transcript_listing(crawler, url):
    html = await crawler.get_html(url)
    await crawler.enqueue({
        process_transcript(crawler, href)
        for href in html.xpath('//a[contains(@href, "praktiko")]/@href')})


async def process_transcript(crawler, url):
    payload = await crawler.get_payload(url)
    await crawler.exec_blocking(parse_transcript, url, payload)


def parse_transcript(url, payload):
    SUBS = [
        ('Παρόντες βουλευτές', '🌮'),
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
        ('Χαμπουλάς Ευγένιος', 'Χαμπουλλάς Ευγένιος'),
        # Misaligned cols in 2015-06-18
        ('Δημητρίου Μισιαούλη Στέλλα Παπαγεωργίου Πάμπος',
         'Δημητρίου Μισιαούλη Στέλλα   Παπαγεωργίου Πάμπος')]

    def _parse_attendee_name(name):
        # Skip page numbers
        if name.isdigit():
            return

        new_name = decipher_name(name)
        if not new_name:
            logger.warning('Unable to pair name {!r} with MP on record while'
                           ' processing {!r}'.format(name, url))
            return
        if name != new_name:
            logger.info('Name {!r} converted to {!r} while'
                        ' processing {!r}'.format(name, new_name, url))
        return new_name

    def _extract_attendees(attendee_table, date, text):
        # Split at page breaks 'cause the columns will have likely shifted
        attendee_table = attendee_table.split('\x0c')
        attendees = itertools.chain.from_iterable(map(
            lambda t: TableParser(t).values, attendee_table))
        attendees = filter(None, map(_parse_attendee_name, attendees))
        # The President's not listed among the attendees
        if 'ΠΡΟΕΔΡΟΣ:' in text or date in {'2015-04-02_1', '2015-04-02_2'}:
            attendees = itertools.chain(attendees, ('Ομήρου Γιαννάκης',))
        return sorted(attendees)

    if url[-4:] != '.pdf':
        return logger.warning('We are only able to parse PDF transcripts;'
                              ' skipping {!r}'.format(url))

    date, date_success = parse_transcript_date(url)
    if not date_success:
        return logger.error('Unable to extract date from filename of'
                            ' transcript at {!r}'.format(url))

    text = apply_subs(pdf2text(payload), SUBS)
    try:
        attendee_table = RE_ATTENDEES.search(text).group()
    except AttributeError:
        return logger.error('Unable to extract attendee table from'
                            ' transcript at {!r}'.format(url))

    plenary_sitting = PlenarySitting.from_template(
        {'_filename': '{}.yaml'.format(PlenarySitting.select_date(date)),
         'mps_present': _extract_attendees(attendee_table, date, text)})
    if not plenary_sitting.merge():
        logger.warning('Unable to locate or update plenary with'
                       ' filename {!r}'.format(plenary_sitting['_filename']))
