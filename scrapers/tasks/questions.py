
from collections import namedtuple
import itertools
import logging
import re

from scrapers.crawling import Task
from scrapers import records
from scrapers.text_utils import (apply_subs, clean_spaces, match_declined_name,
                                 parse_long_date, ungarble_qh)

logger = logging.getLogger(__name__)


class Questions(Task):
    """Create individual question records from question listings."""

    name = 'questions'

    async def process_question_index(self, url=None):
        url = url or 'http://www2.parliament.cy/parliamentgr/008_02.htm'
        html = await self.c.get_html(url)
        await self.c.gather({self.process_question_listing(href) for href in
                             html.xpath('//a[contains(@href,'
                                        ' "chronological")]/@href')})

    __call__ = process_question_index

    async def process_question_listing(self, url):
        html = await self.c.get_html(url, clean=True)
        await self.process_question_index(url)
        await self.c.gather({self.c.exec_blocking(parse_question,
                                                  url, *question) for question
                             in extract_questions_from_listing(url, html)})


SUBS = [('-Ερώτηση', 'Ερώτηση'),
        ('Λευκωσίας Χρήστου Στυλιανίδη', 'Λευκωσίας κ. Χρήστου Στυλιανίδη'),
        ('Περδίκη Ερώτηση', 'Ερώτηση'),
        ('φΕρώτηση', 'Ερώτηση')]


def extract_questions_from_listing(url, html):
    """Pin down question boundaries."""
    Element = namedtuple('Element', 'element, text')

    heading = ()  # (<Element>, '')
    body = []     # [(<Element>, ''), ...]
    footer = []

    for e in itertools.chain((Element(e, clean_spaces(e.text_content()))
                              for e in html.xpath('//tr//p')),
                             (Element(..., 'Ερώτηση με αρ.'),)):   # Sentinel
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


RE_HF1 = re.compile(
    r'Ερώτηση με αρ\. (?P<id>[\d\.]+),? ημερομηνίας (?P<date>[\w ]+)')
# Heading format before 2002 or thereabouts
RE_HF2 = re.compile(
    r'Ερώτηση με αρ\. (?P<id>[\d\.]+) που .* (?:την|στις) (?P<date>[\w ]+)')
RE_HNAMES = re.compile(r'((?:[ -][ΆΈ-ΊΌΎΏΑ-ΡΣ-Ϋ][ΐά-ώ]*\.?){2,3})')


def parse_question(url, heading, body, footer):
    try:
        match = (RE_HF1.match(heading.text) or
                 RE_HF2.match(heading.text)).groupdict()
    except AttributeError:
        logger.error('Unable to parse heading {!r}'
                     ' in {!r}'.format(heading.text, url))
        return

    names = RE_HNAMES.findall(heading.text)
    names = (match_declined_name(name) or logger.warning(
         'No match found for name {!r} in heading {!r}'
         ' in {!r}'.format(name, heading.text, url)) for name in names)
    names = list(filter(None, names))

    answer_links = (e.xpath('.//a/@href') or logger.info(
        'Unable to extract URL of answer to question with'
        ' id {!r} in {!r}'.format(match['id'], url)) for e, _ in footer)
    answer_links = itertools.chain.from_iterable(filter(None, answer_links))
    answer_links = sorted(set(answer_links))

    question = records.Question.from_template(
        filename=None, sources=(url,),
        update={'answers': answer_links,
                'by': names,
                'date': parse_long_date(match['date']),
                'heading': heading.text.rstrip('.'),
                'identifier': match['id'],
                'text': '\n\n'.join(p.text for p in body).strip()})
    try:
        question.insert()
    except records.InsertError as e:
        logger.error(e)
