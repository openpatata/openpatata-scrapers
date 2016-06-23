
from collections import namedtuple
import csv
from datetime import date
from io import StringIO
import itertools as it
import json
import logging
from pathlib import Path
import re

import pandocfilters

from ._models import Bill, MP, PlenarySitting as PS
from .plenary_agendas import \
    (RE_ID, RE_PAGE_NO, RE_TITLE_OTHER,
     AgendaItems, extract_parliamentary_period, extract_session)
from .questions import pair_name
from ..crawling import Task
from ..misc_utils import starfilter
from ..text_utils import \
    (apply_subs, clean_spaces, date2dato, pandoc_json_to, parse_long_date,
     TableParser)

logger = logging.getLogger(__name__)

with (Path(__file__).parent.parent
      /'data'/'reconciliation'/'attendance_names.csv').open() as file:
    NAMES = dict(it.islice(csv.reader(file), 1, None))


class PlenaryTranscripts(Task):
    """Extract MPs in attendance at plenary sittings from transcripts."""

    async def process_transcript_listings(self):
        listing_urls = ('http://www2.parliament.cy/parliamentgr/008_01_01/' + v
                        for v in ('008_01_IE.htm',
                                  '008_01_ID.htm',
                                  '008_01_IDS.htm',
                                  '008_01_IC.htm',
                                  '008_01_IES.htm',
                                  '008_01_IB.htm',
                                  '008_01_IA.htm',
                                #   '008_01_TES.htm',
                                  '008_01_TE.htm',
                                  '008_01_TD.htm',
                                  '008_01_TC.htm',
                                  '008_01_TB.htm',
                                  '008_01_TA.htm',
                                  '008_01_HES.htm',
                                  '008_01_HE.htm',
                                  '008_01_HD.htm',
                                  '008_01_HC.htm',
                                  '008_01_HB.htm',
                                  '008_01_HA.htm',
                                  '008_01_ZES.htm',
                                  '008_01_ZE.htm',
                                  '008_01_ZD.htm',))

        transcript_urls = \
            await self.c.gather({self.process_transcript_listing(url)
                                 for url in listing_urls})
        transcript_urls = it.chain.from_iterable(transcript_urls)
        return await self.c.gather({self.process_transcript(url)
                                    for url in transcript_urls})

    __call__ = process_transcript_listings

    async def process_transcript_listing(self, url):
        html = await self.c.get_html(url)
        return html.xpath('//a[contains(@href, "praktiko")]/@href')

    async def process_transcript(self, url):
        func, content = await self.c.get_payload(url, decode=True)
        return url, func, content

    def after(output):
        for url, text, heading, date, cap2, bills in \
                filter(None, (parse_transcript(*t) for t in output)):
            attendees = filter(None,
                               map(lambda v: NAMES.get(v) or
                                    logger.error('No match found for ' + repr(v)),
                                   extract_attendees(url, text, heading, date)))
            plenary_sitting = \
                PS(_sources=[url],
                   agenda=PS.PlenaryAgenda(cap2=cap2),
                   attendees=[{'mp_id': a} for a in attendees],
                   date=date,
                   links=[PS.PlenaryAgendaLink(type='transcript', url=url)],
                   parliamentary_period_id=extract_parliamentary_period(url, heading),
                   session=extract_session(url, heading),
                   sitting=extract_sitting(url, heading))
            plenary_sitting.insert(merge=plenary_sitting.exists)

            for bill in bills:
                try:
                    actions = Bill.Submission(plenary_id=plenary_sitting._id,
                                              sponsors=bill.sponsors,
                                              committees_referred_to=bill.committees)
                    actions = [actions]
                except ValueError:
                    # Discard likely malformed bills
                    logger.error('Unable to parse {!r} into a bill'
                                 .format(bill))
                    continue

                bill = Bill(_sources=[url], actions=actions,
                            identifier=bill.number, title=bill.title)
                bill.insert(merge=bill.exists)


class ReconcileAttendanceNames(PlenaryTranscripts):

    def after(output):
        names_and_ids = {i['_id']: i['name']['el'] for i in MP.collection.find()}
        names = sorted(set(it.chain.from_iterable(
            extract_attendees(u, t, h, d)
            for u, t, h, d, *_ in
            filter(None, (parse_transcript(*t) for t in output)))))
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(('name', 'id'))
        csv_writer.writerows(pair_name(n, names_and_ids, NAMES) for n in names)
        print(output.getvalue())


PRESIDENTS = (((date(2001, 6, 21), date(2007, 12, 19)), 'Χριστόφιας Δημήτρης'),
              ((date(2008, 3, 20), date(2011,  4, 22)), 'Κάρογιαν Μάριος'),
              ((date(2011, 6,  9), date(2016,  4, 14)), 'Ομήρου Γιαννάκης'),
              ((date(2016, 6,  9), date.today()), 'Συλλούρης Δημήτρης'),)
PRESIDENTS = tuple((range(*map(date.toordinal, dates)), {name})
                   for dates, name in PRESIDENTS)


def _select_president(date):
    match = starfilter((lambda date: lambda date_range, _: date in date_range)
                       (date2dato(date).toordinal()), PRESIDENTS)
    try:
        _, name = next(match)
        return name
    except StopIteration:
        raise ValueError('No President found for {!r}'.format(date)) from None


ATTENDEE_SUBS = (('|', ' '),
                 ('Παρόντες βουλευτές', '🌮'),
                 ('ΠΑΡΌΝΤΕΣ ΒΟΥΛΕΥΤΈΣ', '🌮'),      # pandoc
                 ('(Ώρα λήξης: 6.15 μ.μ.)', '🌮'),  # 2015-03-19
                 ('Παρόντες αντιπρόσωποι θρησκευτικών ομάδων', '🌯'),
                 ('Αντιπρόσωποι θρησκευτικών ομάδων', '🌯'),  # 2014-10-23
                 ('Περιεχόμενα', '🌯'),
                 ('ΠΕΡΙΕΧΟΜΕΝΑ', '🌯'),)


def extract_attendees(url, text, heading, date):
    # Split at page breaks 'cause the columns will have likely shifted
    # and strip off leading whitespace
    _, _, attendee_table = apply_subs(text, ATTENDEE_SUBS).rpartition('🌮')
    attendee_table, _, _ = attendee_table.partition('🌯')
    attendee_table = ('\n'.join(l.lstrip() for l in s.splitlines())
                      for s in attendee_table.split('\x0c'))

    attendees = set(filter(lambda i: bool(i) and not i.isdigit(),
                           (clean_spaces(a, medial_newlines=True)
                            for t in attendee_table
                            for a in TableParser(t).values)))
    # The President's not listed among the attendees
    if 'ΠΡΟΕΔΡΟΣ:' in heading:
        attendees = attendees | _select_president(date)
    return sorted(attendees)


RE_SITTING_NUMBER = re.compile(r'Αρ\. (\d+)')


def extract_sitting(url, text):
    match = RE_SITTING_NUMBER.search(text)
    return int(match.group(1)) if match else logger.warning(
        'Unable to extract sitting number of {!r}'.format(url))


_Cap2Item = namedtuple('Cap2Item', 'number title sponsors committees')


def extract_cap2_item(url, item):
    if not item:
        return

    try:
        title, sponsors, committees = item
    except ValueError:
        logger.warning('Unable to parse Chapter 2 item {} in {!r}'
                       .format(item, url))
        return

    id_ = RE_ID.search(title)
    if not id_:
        logger.info('Unable to extract document type of {} in {!r}'
                    .format(item, url))
        return
    return _Cap2Item(id_.group(1), RE_TITLE_OTHER.sub('', title).rstrip('. '),
                     sponsors, committees)


RE_CAP2 = re.compile(r'\(Η κατάθεση νομοσχεδίων και εγγράφων\)(.*?)'
                     r'(?:ΠΡΟΕΔΡΟΣ|ΠΡΟΕΔΡΕΥΩΝ|ΠΡΟΕΔΡΕΥΟΥΣΑ)', re.DOTALL)


def _item_groupper():
    counter, prev_length = 0, 0

    def inner(item):
        nonlocal counter, prev_length
        new_length = sum(1 for v in item if v)  # Count 'truthy' values in list
        # The assumption here's that every new list item will contain all
        # three of title, sponsor and committee on the first line, and that
        # fewer than three items will wrap onto the next line.  Ya, it's pretty
        # wonky.  Given how tight the columns are, it's extremely unlikely that
        # the title's gonna fit inside the first line, but it _is_ probable
        # that all three are gonna be of equal length (> 1) - especially if
        # the bill's got multiple sponsors or is referred to several
        # committees.  We're gonna have to resort to using a probabilistic
        # parser (eventually), but regexes and my shitty logic do the job for
        # now (well, sort of)
        if new_length > prev_length:
            counter += 1
        prev_length = new_length
        return counter
    return inner


def extract_cap2(url, text):
    try:
        item_table = RE_CAP2.search(text).group(1)
    except AttributeError:
        logger.warning('Unable to extract Chapter 2 table in {!r}'.format(url))
        return

    item_table = RE_PAGE_NO.sub('', item_table).replace('|', ' ').split('\x0c')

    items = it.chain.from_iterable(TableParser(t, columns=4).rows
                                   for t in item_table)
    # ((<title>, <sponsor>, <committee>), ...)
    items = (tuple(clean_spaces(' '.join(x), medial_newlines=True)
                   for x in it.islice(zip(*v), 1, None))
             for _, v in it.groupby(items, key=_item_groupper()))
    items = (extract_cap2_item(url, item) for item in items)
    # (Bill(<number>, <title>, <sponsor>, <committee>), ...)
    items = tuple(filter(None, items))
    if items:
        # ((<number>, <number>, ...), (Bill, Bill, ...))
        return next(zip(*items)), AgendaItems(url, items).bills_and_regs


def _walk_pandoc_ast(v):
    return pandocfilters.walk(v, _stringify_pandoc_dicts, None, None)


pandoc_ListNumberMarker = object()

RE_LIST_NUMBER = re.compile(r'\d{1,2}\.$')

PANDOC_TRANSFORMS =  {
    'AlignDefault': lambda _: None,
    'OrderedList': lambda _: [pandoc_ListNumberMarker],
    'Para': lambda v: _walk_pandoc_ast(v),
    'Period': lambda _: '.',
    'Plain': lambda v: _walk_pandoc_ast(v),
    'Space': lambda _: ' ',
    'Str': lambda v: pandoc_ListNumberMarker if RE_LIST_NUMBER.match(v) else v,
    'Strong': lambda v: _walk_pandoc_ast(v),
    'Table': lambda v: v,}


def _stringify_pandoc_dicts(key, value, format_, meta):
    return_value = PANDOC_TRANSFORMS[key](value)
    if isinstance(return_value, list) and \
            all(isinstance(i, str) for i in return_value):
        return_value = [''.join(return_value)]
    return return_value


def _locate_cap2_table(node):
    if node['t'] != 'Table':
        return
    if pandocfilters.stringify(node).startswith('Νομοσχέδια'):
        return True


def extract_pandoc_items(url, list_):
    # Extract agenda items from the pandoc AST
    for x in list_:
        if not x:
            continue
        if isinstance(x[0], list):
            if x[0] == [pandoc_ListNumberMarker]:
                *title, number = x[1]
                title, sponsors, committees = map(' '.join, (title, *x[2:]))
                yield _Cap2Item(RE_ID.search(number).group(1),
                                RE_TITLE_OTHER.sub('', title).rstrip('. '),
                                sponsors, committees)
            else:
                yield from extract_pandoc_items(url, x)


def extract_pandoc_cap2(url, content):
    tables = filter(_locate_cap2_table, json.loads(content)[1])
    try:
        table = next(tables)
    except StopIteration:
        logger.warning('Unable to extract Chapter 2 table in {!r}'.format(url))
        return

    items = tuple(extract_pandoc_items(url, _walk_pandoc_ast([table])))
    if items:
        return next(zip(*items)), AgendaItems(url, items).bills_and_regs


def parse_transcript(url, func, content):
    if func == 'docx_to_json':
        text = pandoc_json_to(content, 'plain')
    else:
        text = content

    heading, _, _ = text.partition('(')
    heading = clean_spaces(heading, medial_newlines=True)
    try:
        date = parse_long_date(heading)
    except ValueError as e:
        logger.error('{}; skipping {!r}'.format(e, url))
        return

    if func == 'docx_to_json':
        cap2, bills_and_regs = (extract_pandoc_cap2(url, content) or
                                ((), ()))
    else:
        cap2, bills_and_regs = extract_cap2(url, text) or ((), ())
    return url, text, heading, date, cap2, bills_and_regs
