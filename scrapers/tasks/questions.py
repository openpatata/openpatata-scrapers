
from collections import namedtuple
import logging
import itertools
import re

from scrapers.crawling import Task
from scrapers.records import Question
from scrapers.text_utils import (apply_subs,
                                 clean_spaces,
                                 NameConverter,
                                 parse_long_date,
                                 ungarble_qh)

logger = logging.getLogger(__name__)


class Questions(Task):
    """Create individual question records from question listings."""

    name = 'questions'

    async def process_question_index(self, url=None):
        url = url or 'http://www2.parliament.cy/parliamentgr/008_02.htm'
        html = await self.crawler.get_html(url)
        await self.crawler.gather(
            {self.process_question_listing(href) for href in
             html.xpath('//a[contains(@href, "chronological")]/@href')})

    __call__ = process_question_index

    async def process_question_listing(self, url):
        html = await self.crawler.get_html(url, clean=True)
        await self.process_question_index(url)
        await self.crawler.gather(
           {self.crawler.exec_blocking(parse_question, url, *question)
            for question in _extract_questions_from_listing(url, html)})


RE_HF1 = re.compile(
    r'Ερώτηση με αρ\. (?P<id>[\d\.]+),? ημερομηνίας (?P<date>[\w ]+)')
# Heading format before 2002 or thereabouts
RE_HF2 = re.compile(
    r'Ερώτηση με αρ\. (?P<id>[\d\.]+) που .* (?:την|στις) (?P<date>[\w ]+)')
RE_HNAMES = re.compile(r'((?:[ -][ΆΈ-ΊΌΎΏΑ-ΡΣ-Ϋ][ΐά-ώ]*\.?){2,3})')

SUBS = [('-Ερώτηση', 'Ερώτηση'),
        ('Λευκωσίας Χρήστου Στυλιανίδη', 'Λευκωσίας κ. Χρήστου Στυλιανίδη'),
        ('Περδίκη Ερώτηση', 'Ερώτηση'),
        ('φΕρώτηση', 'Ερώτηση')]


def _extract_questions_from_listing(url, html):
    """Pin down question boundaries."""
    Element = namedtuple('Element', 'element, text')

    heading = ()  # (<Element>, '')
    body = []     # [(<Element>, ''), ...]
    footer = []

    for e in itertools.chain((Element(e, clean_spaces(e.text_content()))
                              for e in html.xpath('//tr//p')),
                             (Element(..., 'Ερώτηση με αρ.'),)):
        norm_text = apply_subs(ungarble_qh(e.text), SUBS)
        if norm_text.startswith('Ερώτηση με αρ.'):
            if heading and body:
                yield heading, body, footer
            else:
                logger.warning('Heading and/or body empty in question {!r} in'
                               ' {!r}'.format((heading, body, footer), url))

            heading = Element(e.element, norm_text)
            body = []
            footer = []
        elif norm_text.startswith('Απάντηση'):
            footer.append(e)
        else:
            body.append(e)


def _parse_names(url, heading):
    names = RE_HNAMES.findall(heading.text)
    names = \
        (NameConverter.find_match(name) or logger.warning(
             'No match found for name {!r} in heading {!r}'
             ' in {!r}'.format(name, heading.text, url)) for name in names)
    return list(filter(None, names))


def _parse_answers(url, footer, id_):
    def inner():
        for e, _ in footer:
            try:
                a = e.xpath('.//a/@href')[0]
            except IndexError:
                logger.info('Unable to extract URL of answer to question with'
                            ' id {!r} in {!r}'.format(id_, url))
            else:
                yield a
    return list(inner())


def _construct_filename(url, id_):
    counter = 1
    filename = id_
    while filename in Question.seen:
        counter += 1
        filename = '{}_{}'.format(id_, counter)
        logger.warning('Question with filename {!r} in {!r} parsed'
                       ' repeatedly'.format(id_, url))
    Question.seen.add(filename)
    return filename


def parse_question(url, heading, body, footer):
    match = RE_HF1.match(heading.text) or RE_HF2.match(heading.text)
    if not match:
        logger.error('Unable to parse heading {!r}'
                     ' in {!r}'.format(heading.text, url))
        return

    question = Question.from_template(
        _construct_filename(url, match.group('id')),
        {'answers': _parse_answers(url, footer, match.group('id')),
         'by': _parse_names(url, heading),
         'date': parse_long_date(match.group('date')),
         'heading': heading.text.rstrip('.'),
         'identifier': match.group('id'),
         'text': '\n\n'.join(i.text for i in body).strip()})
    if not question.insert():
        logger.warning('Unable to insert or update question {!r}'
                       ' in {!r}'.format(question, url))
