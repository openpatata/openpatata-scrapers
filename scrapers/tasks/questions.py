
"""Create individual question records from question listings."""

from collections import deque, namedtuple
import itertools
import logging
import re

from scrapers.records import Question
from scrapers.text_utils import (apply_subs,
                                 clean_spaces,
                                 NameConverter,
                                 parse_long_date,
                                 ungarble_qh)

logger = logging.getLogger(__name__)

RE_HF1 = re.compile(r'Ερώτηση με αρ\. (?P<id>[\d\.]+),? ημερομηνίας'
                    r' (?P<date>[\w ]+)')
# Heading format before 2002 or thereabouts
RE_HF2 = re.compile(r'Ερώτηση με αρ\. (?P<id>[\d\.]+) που .*'
                    r' (?:την|στις) (?P<date>[\w ]+)')

RE_HNAMES = re.compile(r'((?:[ -][ΆΈ-ΊΌΎΏΑ-ΡΣ-Ϋ][ΐά-ώ]*\.?){2,3})')


async def process_question_index(crawler):
    url = 'http://www2.parliament.cy/parliamentgr/008_02.htm'
    html = await crawler.get_html(url)
    await crawler.enqueue({
        process_question_listing(crawler, href)
        for href in html.xpath('//a[contains(@href, "chronological")]/@href')})

HEAD = process_question_index


async def process_question_listing(crawler, url):
    html = await crawler.get_html(url, clean=True)
    await crawler.enqueue({process_question_index(crawler, url)})
    await crawler.exec_blocking(parse_question_listing, url, html)


def parse_question_listing(url, html):
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

    def _parse(heading, body, footer):
        def _extract_names():
            def inner():
                for name in RE_HNAMES.findall(heading.text):
                    can_name = NameConverter.find_match(name)
                    if not can_name:
                        logger.warning(
                            'No match found for name {!r} in heading'
                            ' {!r} in {!r}'.format(name, heading.text, url))
                        continue
                    yield can_name
            return list(inner())

        def _extract_answers():
            def inner():
                for e, _ in footer:
                    try:
                        a = e.xpath('.//a/@href')[0]
                    except IndexError:
                        logger.info(
                            'Unable to extract URL of answer to question with'
                            ' id {!r} in {!r}'.format(m.group('id'), url))
                    else:
                        yield a
            return list(inner())

        m = RE_HF1.match(heading.text) or RE_HF2.match(heading.text)
        if not m:
            return logger.error('Unable to parse heading'
                                ' {!r} in {!r}'.format(heading.text, url))

        question = Question({
            '_filename': '{}.yaml'.format(m.group('id')),
            'answers': _extract_answers(),
            'by': _extract_names(),
            'date': parse_long_date(m.group('date')),
            'heading': heading.text.rstrip('.'),
            'identifier': m.group('id'),
            'text': '\n\n'.join(i.text for i in body).strip()})
        if question['identifier'] in Question.seen:
            result = question.merge()
            logger.warning('Question with id {!r} in {!r} parsed'
                           ' repeatedly'.format(question['identifier'], url))
        else:
            result = question.insert()
            Question.seen.add(question['identifier'])
        if not result:
            logger.warning('Unable to insert or update question {!r} from'
                           ' {!r}'.format(question, url))

    for heading, body, footer in _extract():
        _parse(heading, body, footer)
