
"""Scraping tasks."""

from collections import deque, namedtuple
import itertools
import logging
import re

from scrapers import records
from scrapers.text_utils import (apply_subs,
                                 clean_spaces,
                                 decipher_name,
                                 NameConverter,
                                 parse_long_date,
                                 parse_transcript_date,
                                 pdf2text,
                                 TableParser,
                                 ungarble_qh)

logger = logging.getLogger(__name__)


def parse_agenda(url, html):
    """Create plenary records from agendas."""
    def _extract_parliamentary_period(text):
        try:
            return re.search(r'(\w+)[\'΄] ΒΟΥΛΕΥΤΙΚΗ ΠΕΡΙΟΔΟΣ', text).group(1)
        except AttributeError:
            logger.error(
                'Unable to extract parliamentary period of {!r}'.format(url))

    def _extract_session(text):
        try:
            return re.search(r'ΣΥΝΟΔΟΣ (\w+)[\'΄]', text).group(1)
        except AttributeError:
            logger.error('Unable to extract session of {!r}'.format(url))

    def _extract_sitting(text):
        try:
            return int(re.search(r'(\d+)[ηή] ?συνεδρίαση', text).group(1))
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
            uid = ''.join([uid] + ex)
            try:
                uid = re.search(r'([12]3\.[0-9.-]+)', uid).group(1)
            except AttributeError:
                logger.error('Unable to extract document type'
                             ' of {!r} in {!r}'.format(title, url))
            else:
                yield uid, title

    body_text = clean_spaces(html.xpath('string(//div[@class="articleBox"])'))

    all_items = dict(_extract_items(html))
    bills = dict(filter(lambda i: i[0].startswith(('23.01', '23.02', '23.03')),
                        all_items.items()))
    debate_topics = dict(filter(lambda i: i[0].startswith('23.05'),
                                all_items.items()))
    unparsed = all_items.keys() - bills.keys() - debate_topics.keys()
    if unparsed:
        logger.warning("Unparsed items {} in {!r}".format(unparsed, url))

    plenary_sitting = records.PlenarySitting.from_template({
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
    bill = records.Bill.from_template({'_filename': '{}.yaml'.format(uid),
                                       'identifier': uid,
                                       'title': title})
    if not bill.insert():
        logger.warning(
            'Unable to insert or update bill with id {!r} and title {!r}'
            ' from {!r}'.format(bill['identifier'], bill['title'], url))


async def _process_agenda(crawler, url):
    try:
        html = await crawler.get_html(url)
    except UnicodeDecodeError:
        # Probably a PDF; we might have to insert those manually
        logger.error("Unable to decode {!r}".format(url))
        return
    await crawler.exec_blocking(parse_agenda, url, html)


async def _process_agenda_listing(crawler, url, form_data=None, lpass=1):
    html = await crawler.get_html(url,
                                  form_data=form_data, request_method='post')

    if lpass == 1:
        pagination = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
        if pagination:
            await crawler.enqueue({
                _process_agenda_listing(
                    crawler, url,
                    form_data={'page': ''.join(filter(str.isdigit, s))},
                    lpass=2)
                for s in pagination})
        else:
            await _process_agenda_listing(crawler, url, lpass=2)
    elif lpass == 2:
        await crawler.enqueue({
            _process_agenda(crawler, href)
            for href in html.xpath('//a[@class="h3Style"]/@href')})


async def _process_agenda_index(crawler, url):
    html = await crawler.get_html(url)
    await crawler.enqueue({
        _process_agenda_listing(crawler, href)
        for href in html.xpath('//a[@class="h3Style"]/@href')})


async def _process_committee_index(crawler, url):
    raise NotImplementedError


async def _process_committee_report_index(crawler, url):
    raise NotImplementedError


async def _process_mp_index(crawler, *urls):
    raise NotImplementedError


def parse_question_listing(url, html):
    """Create individual question records from a question listing."""
    SUBS = [
        ('-Ερώτηση', 'Ερώτηση'),
        ('Λευκωσίας Χρήστου Στυλιανίδη', 'Λευκωσίας κ. Χρήστου Στυλιανίδη'),
        ('Περδίκη Ερώτηση', 'Ερώτηση'),
        ('φΕρώτηση', 'Ερώτηση')]

    def _extract():
        """Pin down question boundaries."""
        Element = namedtuple('Element', 'element, text')

        heading = ()      # (<Element>, '')
        body = deque()    # [(<Element>, ''), ...]
        footer = deque()
        for e in itertools.chain((Element(e, clean_spaces(e.text_content()))
                                  for e in html.xpath('//tr//p')),
                                 (Element(..., 'Ερώτηση με αρ.'),)):
            norm_text = apply_subs(ungarble_qh(e.text), SUBS)
            if norm_text.startswith('Ερώτηση με αρ.'):
                if heading and body:
                    yield heading, body, footer
                else:
                    logger.warning(
                        'Heading and/or body empty in question'
                        ' {!r} in {!r}'.format((heading, body, footer), url))

                heading = Element(e.element, norm_text)
                body.clear()
                footer.clear()
            elif norm_text.startswith('Απάντηση'):
                footer.append(e)
            else:
                body.append(e)

    def _parse(heading, body, footer, _seen=set()):
        def _extract_names():
            def _inner():
                for name in re.findall(
                        r'((?:[ -][ΆΈ-ΊΌΎΏΑ-ΡΣ-Ϋ][ΐά-ώ]*\.?){2,3})',
                        heading.text):
                    can_name = NameConverter.find_match(name)
                    if not can_name:
                        logger.warning(
                            'No match found for name {!r} in heading'
                            ' {!r} in {!r}'.format(name, heading.text, url))
                        continue
                    yield can_name
            return list(_inner())

        def _extract_answers():
            def _inner():
                for e, _ in footer:
                    try:
                        a = e.xpath('.//a/@href')[0]
                    except IndexError:
                        logger.info(
                            'Unable to extract URL of answer to question with'
                            ' id {!r} in {!r}'.format(m.group('id'), url))
                    else:
                        yield a
            return list(_inner())

        m = (re.match(r'Ερώτηση με αρ\. (?P<id>[\d\.]+),? ημερομηνίας'
                      r' (?P<date>[\w ]+)', heading.text) or
             # Format before 2002 or thereabouts
             re.match(r'Ερώτηση με αρ\. (?P<id>[\d\.]+) που .*'
                      r' (?:την|στις) (?P<date>[\w ]+)', heading.text))
        if not m:
            logger.error('Unable to parse heading {!r} in {!r}'.format(
                heading.text, url))
            return

        question = records.Question({
            '_filename': '{}.yaml'.format(m.group('id')),
            'answers': _extract_answers(),
            'by': _extract_names(),
            'date': parse_long_date(m.group('date')),
            'heading': heading.text,
            'identifier': m.group('id'),
            'text': '\n\n'.join(i.text for i in body).strip()})

        if question['identifier'] in _seen:
            result = question.merge()
            logger.warning('Question with id {!r} in {!r} parsed'
                           ' repeatedly'.format(question['identifier'], url))
        else:
            result = question.insert()
            _seen.add(question['identifier'])
        if not result:
            logger.warning('Unable to insert or update question {!r} from'
                           ' {!r}'.format(question, url))

    for heading, body, footer in _extract():
        _parse(heading, body, footer)


async def _process_question_listing(crawler, url):
    html = await crawler.get_html(url, clean=True)
    await crawler.exec_blocking(parse_question_listing, url, html)
    await crawler.enqueue({_process_question_index(crawler, url)})


async def _process_question_index(crawler, url):
    html = await crawler.get_html(url)
    await crawler.enqueue({
        _process_question_listing(crawler, href)
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
        # Crude, but it saves us time
        logger.warning('We are only able to parse PDF transcripts;'
                       ' skipping {!r}'.format(url))
        return

    date, date_success = parse_transcript_date(url)
    if not date_success:
        logger.error('Unable to extract date from filename of transcript'
                     ' at {!r}'.format(url))
        return

    text = apply_subs(pdf2text(payload), SUBS)

    try:
        attendee_table = re.search(r'[\n\x0c] *🌮(.*?)🌯',
                                   text, re.DOTALL).group(1)
    except AttributeError:
        logger.error('Unable to extract attendee table from transcript at'
                     ' {!r}'.format(url))
        return

    plenary_sitting = records.PlenarySitting.from_template(
        {'_filename': '{}.yaml'.format(records.PlenarySitting.select_date(
            date)),
         'mps_present': _extract_attendees(attendee_table, date, text)})
    if not plenary_sitting.merge():
        logger.warning('Unable to locate or update plenary with'
                       ' filename {!r}'.format(plenary_sitting['_filename']))


async def _process_transcript(crawler, url):
    payload = await crawler.get_payload(url)
    await crawler.exec_blocking(parse_transcript, url, payload)


async def _process_transcript_listings(crawler, *urls):
    for url in urls:
        html = await crawler.get_html(url)
        await crawler.enqueue({
            _process_transcript(crawler, href)
            for href in html.xpath('//a[contains(@href, "praktiko")]/@href')})


def parse_transcript_listing(url, html):
    """Add links to transcript PDFs to corresponding plenaries."""
    for href, date, date_success in (
            (href, *parse_transcript_date(href))
            for href in html.xpath('//a[contains(@href, "praktiko")]/@href')):
        if not date_success:
            logger.error('Unable to extract date {!r} from transcript'
                         ' listing at {!r}'.format(date, url))
            continue

        plenary_sitting = records.PlenarySitting(
            {'_filename': '{}.yaml'.format(records.PlenarySitting.select_date(
                date)),
             'links': [{'type': 'transcript', 'url': href}]})
        if not plenary_sitting.merge():
            logger.warning(
                'Unable to insert transcript with URL {!r} in plenary with'
                ' filename {!r}'.format(url, plenary_sitting['_filename']))


async def _process_transcript_listing(crawler, url):
    html = await crawler.get_html(url)
    await crawler.exec_blocking(parse_transcript_listing, url, html)


async def _process_transcript_index(crawler, url):
    html = await crawler.get_html(url)
    await crawler.enqueue({
        _process_transcript_listing(crawler, href)
        for href in html.xpath('//a[@class="h3Style"]/@href')})


TASKS = {
    'committee_reports': (
        _process_committee_report_index,
        'http://www.parliament.cy/easyconsole.cfm/id/220'),
    'committees': (
        _process_committee_index,
        'http://www.parliament.cy/easyconsole.cfm/id/183'),
    'mps': (
        _process_mp_index,
        'http://www.parliament.cy/easyconsole.cfm/id/186',
        'http://www.parliament.cy/easyconsole.cfm/id/904'),
    'plenary_agendas': (
        _process_agenda_index,
        'http://www.parliament.cy/easyconsole.cfm/id/290'),
    'plenary_attendance': (
        _process_transcript_listings,
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IA.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IB.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IES.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IC.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IDS.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_ID.htm',
        'http://www2.parliament.cy/parliamentgr/008_01_01/008_01_IE.htm'),
    'plenary_transcript_urls': (
        _process_transcript_index,
        'http://www.parliament.cy/easyconsole.cfm/id/159'),
    'questions': (
        _process_question_index,
        'http://www2.parliament.cy/parliamentgr/008_02.htm')}
