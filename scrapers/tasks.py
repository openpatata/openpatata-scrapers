
"""Scraping tasks."""

import asyncio
from collections import OrderedDict as od
from functools import reduce
import logging
import re

import pymongo

from scrapers import db, records
from scrapers.text_utils import *

logger = logging.getLogger(__name__)


def _select_plenary_date(date):
    if date[0] != date[1] and db.plenary_sittings.find_one(
                filter={'_filename': '{}.yaml'.format(date[1])}):
        return date[1]
    else:
        return date[0]


def parse_agenda(url, html):
    """Create plenary records and bills from agendas."""
    body_text = clean_spaces(html.xpath('string(//div[@class="articleBox"])'))

    def _extract_parliament():
        try:
            return re.search(r'(\w+)[\'΄] ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ',
                             body_text).group(1)
        except AttributeError:
            logger.error("Unable to extract parliamentary period"
                         " of '{}'".format(url))

    def _extract_session():
        try:
            return re.search(r'ΣΥΝΟΔΟΣ (\w+)[\'΄]', body_text).group(1)
        except AttributeError:
            logger.error("Unable to extract session"
                         " of '{}'".format(url))

    def _extract_sitting():
        try:
            return int(re.search(r'(\d+)[ηή] ?συνεδρίαση',
                                 body_text).group(1))
        except AttributeError:
            logger.error("Unable to extract sitting number"
                         " of '{}'".format(url))

    bills = []  # [records.Bill(), ...]
    plenary = records.PlenarySitting({
        'date': parse_long_date(clean_spaces(html.xpath('string(//h1)')),
                                plenary=True),
        'links': [od([('type', 'agenda'), ('url', url)])],
        'parliament': _extract_parliament(),
        'session': _extract_session(),
        'sitting': _extract_sitting()})

    for e in html.xpath('//div[@class="articleBox"]//tr/td[last()]'):
        try:
            title, *ext, id_ = (clean_spaces(e.text_content())
                                for e in e.xpath('*[self::div or self::p]'))
        except ValueError:
            # Presumably a faux header; skip it
            continue
        else:
            id_ = [id_] + ext

        title = title.rstrip('.')
        id_ = (re.sub(r'[^0-9\.\-]', '', i).strip('.') for i in id_)
        for i in id_:
            try:
                doc_type = re.match(r'23\.(\d{2})', i).group(1)
            except AttributeError:
                continue
            if doc_type in {'04', '05'}:
                plenary['agenda']['debate'].append(i)
            else:
                plenary['agenda']['legislative_work'].append(i)

                bill = records.Bill({'_filename': '{}.yaml'.format(i),
                                     'identifier': i, 'title': title})
                bills.append(bill)
            break
        else:
            logger.error("Unable to extract document type"
                         " of '{}' in '{}'".format(title, url))

    # Version same-day sitting filenames from oldest to newest; extraordinary
    # sittings come last. We're doing this bit of filename trickery 'cause
    # (a) it's probably a good idea if the filenames were to persist; and
    # (b) Parliament similarly version the transcript filenames, meaning
    # that we can bypass downloading and parsing the PDFs (for now, anyway)
    sittings = \
        {(records.PlenarySitting(p)['sitting'] or None) for p in
         db.plenary_sittings.find(filter={'date': plenary['date']})} | \
        {plenary['sitting'] or None}
    sittings = sorted(sittings, key=lambda v: float('inf') if v is None else v)
    for i, sitting in enumerate(sittings):
        if i:
            _filename = '{}_{}.yaml'.format(plenary['date'], i+1)
        else:
            _filename = '{}.yaml'.format(plenary['date'])
        if plenary['sitting'] == sitting:
            plenary['_filename'] = _filename
        db.plenary_sittings.find_one_and_update(
            filter={'date': plenary['date'], 'sitting': sitting},
            update={'$set': {'_filename': _filename}})

    result = db.plenary_sittings.find_one_and_update(
        filter={'_filename': plenary['_filename']},
        update=plenary.prepare(),
        upsert=True,
        return_document=pymongo.ReturnDocument.AFTER)
    if not result:
        logger.warning("Unable to insert or update plenary on '{}'"
                       " in '{}'".format(plenary['date'], url))

    for bill in bills:
        result = db.bills.find_one_and_update(
            filter={'_filename': bill['_filename']},
            update={'$set': bill},
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER)
        if not result:
            logger.warning(
                "Unable to insert or update bill with id '{}'"
                " and title '{}' in '{}'".format(bill['identifier'],
                                                 bill['title'], url))


async def process_agenda(crawler, url):
    try:
        html = await crawler.get_html(url)
    except UnicodeDecodeError:
        # Probably a PDF; we might have to insert those manually
        logger.error("Unable to decode '{}'".format(url))
        return
    crawler.exec_blocking(parse_agenda, url, html)


async def process_agenda_listing(crawler, url, form_data=None, lpass=1):
    html = await crawler.get_html(url,
                                  form_data=form_data, request_method='post')

    if lpass == 1:
        pagination = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
        if pagination:
            await asyncio.gather(*{
                process_agenda_listing(
                    crawler,
                    url,
                    form_data={'page': ''.join(filter(str.isdigit, s))},
                    lpass=2)
                for s in pagination})
        else:
            await process_agenda_listing(crawler, url, lpass=2)
    elif lpass == 2:
        await asyncio.gather(*{
            process_agenda(crawler, href)
            for href in html.xpath('//a[@class="h3Style"]/@href')})


async def process_agenda_index(crawler, url):
    html = await crawler.get_html(url)
    await asyncio.gather(*{
        process_agenda_listing(crawler, href)
        for href in html.xpath('//a[@class="h3Style"]/@href')})


def parse_committee(url, html):
    """Create bare-bones committee records."""
    SUBS = [("'Εσχες", 'Έσχες')]

    title = clean_spaces(html.xpath('string(//h1)'))
    title = reduce(lambda t, sub: t.replace(*sub), SUBS, title)
    if title.startswith('ΥΠΟΕΠΙΤΡΟΠΕΣ'):
        logger.debug("Skipping subcommittee listing in '{}'".format(url))
        return

    committee = records.Committee({
        '_filename': '{}.yaml'.format(Translit.slugify(title)),
        'name': {'el': title, 'en': None}})

    result = db.committees.find_one_and_update(
        filter={'_filename': committee['_filename']},
        update={'$set': committee},
        upsert=True,
        return_document=pymongo.ReturnDocument.AFTER)
    if not result:
        logger.warning("Unable to insert or update committee '{}'".format(url))


async def process_committee(crawler, url):
    html = await crawler.get_html(url)
    crawler.exec_blocking(parse_committee, url, html)


async def process_committee_index(crawler, url):
    html = await crawler.get_html(url)
    await asyncio.gather(*{
        process_committee(crawler, href)
        for href in html.xpath('//a[@class="h3Style"]/@href')})


async def process_committee_report_index(crawler, url):
    raise NotImplementedError


async def process_mp_index(*crawler, urls):
    raise NotImplementedError


def parse_question_listing(url, html):
    """Create individual question records from a question listing."""
    SUBS = [
        ('-Ερώτηση',                     'Ερώτηση'),
        ('φΕρώτηση',                     'Ερώτηση'),
        ('Περδίκη Ερώτηση',              'Ερώτηση'),
        ('Λευκωσίας Χρήστου Στυλιανίδη', 'Λευκωσίας κ. Χρήστου Στυλιανίδη')]

    def _extract_qs(html):
        """Pin down question boundaries."""
        heading = ()  # (<Element>, '')
        body = []     # [(<Element>, ''), ...]
        footer = []

        stream = ((e, clean_spaces(e.text_content()))
                  for e in html.xpath('//tr//p'))
        while True:
            e = next(stream, type(
                'Sentinel', (tuple,),
                {'__bool__': lambda _: False})([..., 'Ερώτηση με αρ.']))

            ungarbled_text = ungarble_qh(e[1])
            ungarbled_text = reduce(lambda s, sub: s.replace(*sub),
                                    SUBS, ungarbled_text)
            if ungarbled_text.startswith('Ερώτηση με αρ.'):
                if heading and body:
                    yield heading, body, footer
                else:
                    logger.warning(
                        "Heading and/or body empty in question"
                        " '{}' in '{}'".format((heading, body, footer), url))

                if not e:
                    break
                heading = (e[0], ungarbled_text)
                body.clear()
                footer.clear()
            elif ungarbled_text.startswith('Απάντηση'):
                footer.append(e)
            else:
                body.append(e)

    def _insert(heading, body, footer, _seen=set()):
        # `id` and `date` are required
        m = re.match(r'Ερώτηση με αρ\. (?P<id>[\d\.]+),? ημερομηνίας'
                     r' (?P<date>[\w ]+)', heading[1])
        # Format before 2002 or thereabouts
        m = m or re.match(r'Ερώτηση με αρ\. (?P<id>[\d\.]+) που .*'
                          r' (?:την|στις) (?P<date>[\w ]+)', heading[1])
        if not m:
            logger.error("Unable to parse heading '{}' in '{}'".format(
                heading[1], url))
            return

        def _extract_names():
            for name in re.findall(r'((?:[ -][ΆΈ-ΊΌΎΏΑ-ΡΣ-Ϋ][ΐά-ώ]*\.?){2,3})',
                                   heading[1]):
                can_name = NameConverter.find_match(name)
                if not can_name:
                    logger.warning("No match found for name '{}' in heading"
                                   " '{}' in '{}'".format(name, heading[1],
                                                          url))
                    continue
                yield can_name

        def _extract_answers():
            for a, _ in footer:
                try:
                    a = a.xpath('.//a/@href')[0]
                except IndexError:
                    logger.warning(
                        "Unable to extract URL of answer to question with"
                        " id '{}' in '{}'".format(m.group('id'), url))
                else:
                    yield a

        question = records.Question({
            '_filename': '{}.yaml'.format(m.group('id')),
            'answers': list(_extract_answers()),
            'by': list(_extract_names()),
            'date': parse_long_date(m.group('date')),
            'heading': heading[1],
            'identifier': m.group('id'),
            'text': '\n\n'.join(p_text for _, p_text in body).strip()})

        if question['identifier'] in _seen:
            question = question.compact()
            logger.warning("Question with id '{}' in '{}' parsed"
                           " repeatedly".format(question['identifier'], url))
        else:
            _seen.add(question['identifier'])

        result = db.questions.find_one_and_update(
            filter={'_filename': question['_filename']},
            update={'$set': question},
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER)
        if not result:
            logger.warning("Unable to insert or update question '{}' from"
                           " '{}'".format(question, url))

    for heading, body, footer in _extract_qs(html):
        _insert(heading, body, footer)


async def process_question_listing(crawler, url):
    html = await crawler.get_html(url, clean=True)
    crawler.exec_blocking(parse_question_listing, url, html)

    await asyncio.gather(process_question_index(crawler, url))


async def process_question_index(crawler, url):
    html = await crawler.get_html(url)
    await asyncio.gather(*{
        process_question_listing(crawler, href)
        for href in html.xpath('//a[contains(@href, "chronological")]/@href')})


def parse_transcript(url, payload):
    """Extract stuff from transcripts."""
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
            logger.warning("Unable to pair name '{}' with MP on record while"
                           " processing '{}'".format(name, url))
            return
        if name != new_name:
            logger.info("Name '{}' converted to '{}' while"
                        " processing '{}'".format(name, new_name, url))
        return new_name

    def _extract_attendees(attendee_table, date, text):
        # Split at page breaks 'cause the columns will have likely shifted
        attendee_table = attendee_table.split('\x0c')

        attendees = itertools.chain(*[TableParser(subtable).values
                                      for subtable in attendee_table])
        attendees = list(filter(bool, map(_parse_attendee_name, attendees)))
        # The President isn't listed among the attendees, for whatever reason
        if '(Γ. ΟΜΗΡΟΥ)' in text or date \
                in {'2015-04-02_1', '2015-04-02_2'}:
            attendees.append('Ομήρου Γιαννάκης')
        return attendees

    text = pdf2text(payload)
    text = reduce(lambda s, sub: s.replace(*sub), SUBS, text)

    try:
        attendee_table = re.search(r'[\n\x0c] *🌮(.*?)🌯',
                                   text, re.DOTALL).group(1)
    except AttributeError:
        logger.error("Unable to extract attendees from transcript at"
                     " '{}'".format(url))
        return

    date = _select_plenary_date(parse_transcript_date(url)[0])
    attendees = _extract_attendees(attendee_table, date, text)

    result = db.plenary_sittings.find_one_and_update(
        filter={'_filename': '{}.yaml'.format(date)},
        update={'$set': {'attendees': attendees}})
    if not result:
        logger.warning("Unable to locate or update plenary for date '{}'"
                       " of transcript".format(date))


async def process_transcript(crawler, url):
    payload = await crawler.get_payload(url)
    crawler.exec_blocking(parse_transcript, url, payload)


async def process_transcripts(crawler, *urls):
    for url in urls:
        html = await crawler.get_html(url)
        await asyncio.gather(*{
            process_transcript(crawler, href)
            for href in html.xpath('//a[contains(@href, "praktiko")]/@href')})


def parse_transcript_listing(url, html):
    """Add links to transcript PDFs to corresponding plenaries."""
    for href, date, date_success in (
            (href, *parse_transcript_date(href))
            for href in html.xpath('//a[contains(@href, "praktiko")]/@href')):
        if not date_success:
            logger.error("Unable to extract date '{}' from transcript"
                         " listing at '{}'".format(date, url))
            continue
        date = _select_plenary_date(date)
        # BSON objs need to be arranged in the same (alphabetical)
        # order to be evaluated as identical—apparently
        transcript = od([('type', 'transcript'), ('url', href)])

        result = db.plenary_sittings.find_one_and_update(
            filter={'_filename': '{}.yaml'.format(date)},
            update={'$addToSet': {'links': transcript}})
        if not result:
            logger.warning("Unable to locate or update plenary for date '{}'"
                           " of transcript".format(date))


async def process_transcript_listing(crawler, url):
    html = await crawler.get_html(url)
    crawler.exec_blocking(parse_transcript_listing, url, html)


async def process_transcript_index(crawler, url):
    html = await crawler.get_html(url)
    await asyncio.gather(*{
        process_transcript_listing(crawler, href)
        for href in html.xpath('//a[@class="h3Style"]/@href')})


TASKS = {
    'agendas': (
        process_agenda_index,
        'http://www.parliament.cy/easyconsole.cfm/id/290'),
    'committee_reports': (
        process_committee_report_index,
        'http://www.parliament.cy/easyconsole.cfm/id/220'),
    'committees': (
        process_committee_index,
        'http://www.parliament.cy/easyconsole.cfm/id/183'),
    'mps': (
        process_mp_index,
        'http://www.parliament.cy/easyconsole.cfm/id/186',
        'http://www.parliament.cy/easyconsole.cfm/id/904'),
    'questions': (
        process_question_index,
        'http://www2.parliament.cy/parliamentgr/008_02.htm'),
    'transcript_urls': (
        process_transcript_index,
        'http://www.parliament.cy/easyconsole.cfm/id/159'),
    'transcripts': (
        process_transcripts,
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IC.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IDS.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_ID.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IE.htm')}
