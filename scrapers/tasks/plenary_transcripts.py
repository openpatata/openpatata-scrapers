
from collections import namedtuple
from datetime import date
import itertools
import logging
import re

from scrapers.crawling import Task
from scrapers.misc_utils import starfilter
from scrapers import records
from scrapers.tasks.plenary_agendas import (RE_ID, RE_PAGE_NO, RE_TITLE_OTHER,
                                            AgendaItems,
                                            extract_parliamentary_period,
                                            extract_session)
from scrapers.text_utils import (apply_subs, CanonicaliseName, clean_spaces,
                                 date2dato, parse_long_date, TableParser)

logger = logging.getLogger(__name__)


class PlenaryTranscripts(Task):
    """Extract MPs in attendance at plenary sittings from transcripts."""

    async def process_transcript_listings(self):
        listing_urls = (
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IA.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IB.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IES.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IC.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IDS.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_ID.htm',
            'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IE.htm')

        transcript_urls = await \
            self.c.gather({self.process_transcript_listing(url)
                           for url in listing_urls})
        transcript_urls = itertools.chain.from_iterable(transcript_urls)
        return await self.c.gather({self.process_transcript(url)
                                    for url in transcript_urls})

    __call__ = process_transcript_listings

    async def process_transcript_listing(self, url):
        html = await self.c.get_html(url)
        return html.xpath('//a[contains(@href, "praktiko")]/@href')

    async def process_transcript(self, url):
        return url, await self.c.get_payload(url, decode=True)

    @staticmethod
    def after(output):
        for transcript in output:
            _parse_transcript(*transcript)


def _extract_attendee_name(url, name):
    if name.isdigit():
        return      # Skip page numbers

    new_name = CanonicaliseName.from_garbled(
        clean_spaces(name, medial_newlines=True))
    if not new_name:
        logger.warning('Unable to pair name {!r} with MP on record'
                       ' while parsing {!r}'.format(name, url))
        return
    if name != new_name:
        logger.info('Name {!r} converted to {!r}'
                    ' while parsing {!r}'.format(name, new_name, url))
    return new_name


PRESIDENTS = (((date(2001, 6, 21), date(2007, 12, 19)), 'Χριστόφιας Δημήτρης'),
              ((date(2008, 3, 20), date(2011, 4, 22)), 'Καρογιάν Μάριος'),
              ((date(2011, 6, 9), date.today()), 'Ομήρου Γιαννάκης')
              )
PRESIDENTS = tuple((range(*map(date.toordinal, dates)), {name})
                   for dates, name in PRESIDENTS)


def _select_president(date_):
    match = starfilter((lambda date: lambda date_range, _: date in date_range)
                       (date2dato(date_).toordinal()), PRESIDENTS)
    try:
        _, name = next(match)
        return name
    except StopIteration:
        raise ValueError('No President found for {!r}'.format(date_)) from None


SUBS = (('|', ''),
        ('Παρόντες βουλευτές', '🌮'),
        ('ΠΑΡΌΝΤΕΣ ΒΟΥΛΕΥΤΈΣ', '🌮'),      # pandoc
        ('(Ώρα λήξης: 6.15 μ.μ.)', '🌮'),  # 2015-03-19
        ('Παρόντες αντιπρόσωποι θρησκευτικών ομάδων', '🌯'),
        ('Αντιπρόσωποι θρησκευτικών ομάδων', '🌯'),  # 2014-10-23
        ('Περιεχόμενα', '🌯'),
        ('ΠΕΡΙΕΧΟΜΕΝΑ', '🌯'),
        ('Φακοντής Σοφοκλής', 'Φακοντής Αντρέας')  # s/e in 2011-12-13
        )


def _extract_attendees(url, text, heading, date):
    # Split at page breaks 'cause the columns will have likely shifted
    # and strip off leading whitespace
    attendee_table = (apply_subs(text, SUBS).rpartition('🌮')[2]
                                            .partition('🌯')[0])
    attendee_table = attendee_table.split('\x0c')
    attendee_table = ('\n'.join(l.lstrip() for l in s.splitlines())
                      for s in attendee_table)

    attendees = set(filter(None, (_extract_attendee_name(url, a)
                                  for t in attendee_table
                                  for a in TableParser(t, columns=2).values)))
    # The President's not listed among the attendees
    attendees = attendees | _select_president(date) if 'ΠΡΟΕΔΡΟΣ:' in heading \
        else attendees
    attendees = sorted(attendees)
    return attendees


RE_SI = re.compile(r'Αρ\. (\d+)')


def _extract_sitting(url, text):
    match = RE_SI.search(text)
    return int(match.group(1)) if match else logger.warning(
        'Unable to extract sitting number of {!r}'.format(url))


_Bill = namedtuple('Bill', 'identifier title submitted_by referred_to')


def _extract_cap2_item(url, item):
    try:
        title, *other = item
    except ValueError:
        logger.warning('Unable to parse Chapter 2 item {}'
                       ' in {!r}'.format(tuple(item), url))
        return

    id_ = RE_ID.search(title)
    if not id_:
        logger.info('Unable to extract document type'
                    ' of {!r} in {!r}'.format([title] + other, url))
        return

    try:
        return _Bill(id_.group(1), RE_TITLE_OTHER.sub('', title).rstrip('. '),
                     *other)
    except TypeError:
        logger.warning('Unable to parse Chapter 2 item {}'
                       ' in {!r}'.format([title] + other, url))


RE_CAP2 = re.compile(r'\(Η κατάθεση νομοσχεδίων και εγγράφων\)(.*?)'
                     r'(?:ΠΡΟΕΔΡΟΣ|ΠΡΟΕΔΡΕΥΩΝ|ΠΡΟΕΔΡΕΥΟΥΣΑ)', re.DOTALL)


def _item_groupper():
    counter, prev_length = 0, 0

    def inner(item):
        nonlocal counter, prev_length
        new_length = sum(1 for v in item if v)  # Count 'truthy' values in list
        if new_length > prev_length:
            counter += 1
        prev_length = new_length
        return counter
    return inner


def _extract_cap2(url, text):
    try:
        item_table = RE_CAP2.search(text).group(1)
    except AttributeError:
        logger.warning('Unable to extract Chapter 2 table in {!r}'.format(url))
        return ()

    item_table = item_table.replace('|', ' ').split('\x0c')
    item_table = (RE_PAGE_NO.sub('', page) for page in item_table)

    items = itertools.chain.from_iterable(TableParser(t, columns=4).rows
                                          for t in item_table)
    items = ((clean_spaces(' '.join(x), medial_newlines=True)
              for x in itertools.islice(zip(*v), 1, None))
             for _, v in itertools.groupby(items, key=_item_groupper()))
    items = (_extract_cap2_item(url, item) for item in items)
    return AgendaItems(url, tuple(filter(None, items))).bills_and_regs


def _parse_transcript(url, text):
    heading = clean_spaces(text.partition('(')[0], medial_newlines=True)
    try:
        date = parse_long_date(heading)
    except ValueError as e:
        logger.error('{}; skipping {!r}'.format(e, url))
        return

    plenary_sitting = records.PlenarySitting.from_template(
        sources=(url,),
        update={'attendees': _extract_attendees(url, text, heading, date),
                'date': date,
                'links': [{'type': 'transcript', 'url': url}],
                'parliamentary_period': extract_parliamentary_period(url,
                                                                     heading),
                'session': extract_session(url, heading),
                'sitting': _extract_sitting(url, heading)})
    try:
        plenary_sitting.merge() if plenary_sitting.exists else \
            plenary_sitting.insert()
    except records.InsertError as e:
        logger.error(e)

    for bill_ in _extract_cap2(url, text):
        bill = records.Bill.from_template(
            sources=(url,),
            update={'identifier': bill_.identifier,
                    'title': bill_.title,
                    'actions': [{'action': 'submit',
                                 'at': plenary_sitting['_filename'],
                                 'by': bill_.submitted_by,
                                 'refer_to': bill_.referred_to}]})
        try:
            bill.merge() if bill.exists else bill.insert()
        except records.InsertError as e:
            logger.error(e)
