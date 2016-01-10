
import itertools
import logging
import re

from lxml.html import HtmlElement

from scrapers.crawling import Task
from scrapers import records
from scrapers.text_utils import (apply_subs, clean_spaces, CanonicaliseName,
                                 parse_long_date, ungarble_qh)

logger = logging.getLogger(__name__)


class Questions(Task):
    """Create individual question records from question listings."""

    async def process(self):
        url = 'http://www2.parliament.cy/parliamentgr/008_02.htm'
        return itertools.chain.from_iterable(
            await self.process_question_index(url))

    __call__ = process

    async def process_question_index(self, url):
        html = await self.c.get_html(url)
        return itertools.chain.from_iterable(await self.c.gather(
            {self.process_question_listing(href)
             for href in html.xpath('//a[contains(@href, '
                                    '"chronological")]/@href')}))

    async def process_question_listing(self, url):
        html = await self.c.get_html(url, clean=True)
        return \
            itertools.chain.from_iterable(
                await self.process_question_index(url)), \
            ((url, *question) for question in extract_questions(url, html))

    @staticmethod
    def after(output):
        for question in output:
            _parse_question(*question)


SUBS = (('-Ερώτηση', 'Ερώτηση'), ('Περδίκη Ερώτηση', 'Ερώτηση'),
        ('φΕρώτηση', 'Ερώτηση'))


def extract_questions(url, html):
    """Pin down question boundaries."""
    heading = None  # <Element>
    body = []       # [<Element>, ...]
    footer = []

    for e in itertools.chain(html.xpath('//tr//p'),
                             (HtmlElement('Ερώτηση με αρ.'),)):    # Sentinel
        e.text = clean_spaces(e.text_content())
        norm_text = apply_subs(ungarble_qh(e.text), SUBS)
        if norm_text.startswith('Ερώτηση με αρ.'):
            if heading is not None and body:
                yield heading, body, footer
            else:
                logger.warning('Skipping question {} in {!r}'
                               ''.format((getattr(heading, 'text', ''),) +
                                         tuple(i.text for i in body), url))

            e.text = norm_text
            heading = e
            body = []
            footer = []
        elif norm_text.startswith('Απάντηση'):
            footer.append(e)
        else:
            body.append(e)


RE_HEADING = re.compile(r'Ερώτηση με αρ\. (?P<id>[\d\.]+)'
                        r'(?:,? ημερομηνίας| που .* (?:την|στις)) '
                        r'(?P<date>[\w ]+)')

# Chop off the district 'cause it could pass off as a name
RE_NAMES_PREPARE = re.compile(r'.* περιφέρειας \w+ (?:κ\. )')

#             name = first_name ' ' last_name
#       first_name = name_part
#        last_name = name_part [(' ' | '-') name_part]
#                  | uppercase_letter '. ' name_part
#        name_part = uppercase_letter {lowercase_letter}
# uppercase_letter = 'Α' | 'Β' | ...
# lowercase_letter = 'α' | 'β' | ...
RE_NAMES = re.compile(r'''({uc}{lc}+
                            \ (?:{uc}{lc}+(?:[\ -]{uc}{lc}+)?|
                                 {uc}\.\ {uc}{lc}+))
                       '''.format(uc=r'[ΆΈ-ΊΌΎΏΑ-ΡΣ-Ϋ]', lc=r'[ΐά-ώ]'),
                      re.VERBOSE)


def _parse_question(url, heading, body, footer):
    try:
        match = RE_HEADING.match(heading.text).groupdict()
    except AttributeError:
        logger.error('Unable to parse heading {!r}'
                     ' in {!r}'.format(heading.text, url))
        return

    names = RE_NAMES.findall(RE_NAMES_PREPARE.sub('', heading.text))
    names = (CanonicaliseName.from_declined(name) or logger.warning(
         'No match found for name {!r} in heading {!r}'
         ' in {!r}'.format(name, heading.text, url)) for name in names)
    names = list(filter(None, names))

    answer_links = (e.xpath('.//a/@href') or logger.info(
        'Unable to extract URL of answer to question with number'
        ' {!r} in {!r}'.format(match['id'], url)) for e in footer)
    answer_links = itertools.chain.from_iterable(filter(None, answer_links))
    answer_links = sorted(set(answer_links))

    question = records.Question.from_template(
        {'_sources': [url],
         'answers': answer_links,
         'by': names,
         'date': parse_long_date(match['date']),
         'heading': heading.text.rstrip('.'),
         'identifier': match['id'],
         'text': '\n\n'.join(p.text for p in body).strip()})
    try:
        question.insert()
    except records.InsertError as e:
        logger.error(e)
