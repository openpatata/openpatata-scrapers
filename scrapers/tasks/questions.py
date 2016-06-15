
import csv
from io import StringIO
import itertools as it
import logging
from pathlib import Path
import re

import jellyfish
from lxml.html import HtmlElement

from ._models import MP, Question
from ..crawling import Task
from ..text_utils import clean_spaces, parse_long_date, ungarble_qh

logger = logging.getLogger(__name__)

with (Path(__file__).parent.parent/'data'/'declined_names.csv').open() as file:
    NAMES = dict(it.islice(csv.reader(file), 1, None))


class Questions(Task):
    """Create individual question records from question listings."""

    async def process(self):
        url = 'http://www2.parliament.cy/parliamentgr/008_02.htm'
        return it.chain.from_iterable(await self.process_question_index(url))

    __call__ = process

    async def process_question_index(self, url):
        html = await self.c.get_html(url)

        question_listing_urls = \
            html.xpath('//a[contains(@href, "chronological")]/@href')
        return it.chain.from_iterable(await self.c.gather(
            {self.process_question_listing(href)
             for href in question_listing_urls}))

    async def process_question_listing(self, url):
        html = await self.c.get_html(url, clean=True)
        return (it.chain.from_iterable(await self.process_question_index(url)),
                ((url, *question)
                 for question in extract_questions(url, html)))

    def after(output):
        for question in output:
            _parse_question(*question)


class ReconcileDeclinedNames(Questions):

    def after(output):
        names_and_ids = {mp['_id']: ' '.join(mp['name']['el'].split()[::-1])
                         for mp in MP.collection.find()}
        names = sorted(set(it.chain.from_iterable(
            RE_NAMES.findall(RE_NAMES_PREPARE.sub('', h.text))
            for _, h, *_ in output)))
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(('declined_name', 'id'))
        csv_writer.writerows(pair_name(n, names_and_ids) for n in names)
        print(output.getvalue())


def pair_name(name, names_and_ids):
    if name in NAMES:
        return name, NAMES[name]
    options = tuple(enumerate(
        sorted(((jellyfish.jaro_distance(name, new_name), id_)
                for id_, new_name in names_and_ids.items()), reverse=True)[:5]))
    _, (_, selection) = options[int(input('''\
Please choose one of the following for {!r}:
  {}
'''.format(name, '\n  '.join(map(repr, options)))) or 0)]
    return name, selection


def extract_questions(url, html):
    """Pin down question boundaries."""
    heading = None  # <Element>
    body = []       # [<Element>, ...]
    footer = []

    counter = 0
    for e in it.chain(html.xpath('//tr//p'),
                      (HtmlElement('Ερώτηση με αρ.'),)):    # Sentinel
        e.text = clean_spaces(e.text_content())
        norm_text = ungarble_qh(e.text)
        if norm_text.startswith('Ερώτηση με αρ.'):
            if heading is not None and body:
                counter += 1
                yield heading, body, footer, counter
            else:
                logger.warning('Skipping question {} in {!r}'
                               .format((getattr(heading, 'text', ''),) +
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


def _extract_answers(url, match, footer):
    answer_links = (e.xpath('.//a/@href') for e in footer)
    answer_links = it.chain.from_iterable(filter(None, answer_links))
    answer_links = sorted(set(answer_links))
    if not answer_links:
        logger.info('Unable to extract URL of answer to question with number'
                    ' {!r} in {!r}'.format(match['id'], url))
    return answer_links


def _extract_names(url, heading):
    return [{'mp_id': NAMES[n]}
             for n in RE_NAMES.findall(RE_NAMES_PREPARE.sub('', heading.text))]


def _parse_question(url, heading, body, footer, counter):
    match = RE_HEADING.match(heading.text).groupdict()
    question = Question(_position_on_page=counter,
                        _sources=[url],
                        answers=_extract_answers(url, match, footer),
                        by=_extract_names(url, heading),
                        date=parse_long_date(match['date']),
                        heading=heading.text.rstrip('.'),
                        identifier=match['id'],
                        text='\n\n'.join(p.text for p in body).strip())
    if question.exists:
        logger.info('Merging question ' + repr(question))
        question.merge()
    else:
        question.insert()
