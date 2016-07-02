
import csv
from io import StringIO
import itertools as it
from pathlib import Path
import re

from lxml.html import HtmlElement

from ..crawling import Task
from ..models import MP, Question
from ..reconciliation import pair_name
from ..text_utils import clean_spaces, parse_long_date, ungarble_qh


class Questions(Task):
    """Create individual question records from question listings."""

    with (Path(__file__).parent.parent
          /'data'/'reconciliation'/'question_names.csv').open() as file:
        NAMES = dict(it.islice(csv.reader(file), 1, None))

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
                 for question in demarcate_questions(url, html)))

    def after(output):
        for url, heading, body, footer, counter in output:
            match = RE_HEADING.search(heading.text).groupdict()
            question = Question(_position_on_page=counter,
                                _sources=[url],
                                answers=extract_answers(url, match, footer),
                                by=extract_names(url, heading),
                                date=parse_long_date(match['date']),
                                heading=heading.text.rstrip('.'),
                                identifier=match['id'],
                                text='\n\n'.join(p.text for p in body).strip())
            if question.exists:
                logger.info('Merging question ' + repr(question))
                question.insert(merge=True)
            else:
                question.insert()


class ReconcileQuestionNames(Questions):

    def after(output):
        names_and_ids = {mp['_id']: ' '.join(mp['name']['el'].split()[::-1])
                         for mp in MP.collection.find()}
        names = sorted(set(it.chain.from_iterable(
            RE_NAMES.findall(RE_NAMES_PREPARE.sub('', h.text))
            for _, h, *_ in output)))
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(('name', 'id'))
        csv_writer.writerows(pair_name(n, names_and_ids, Questions.NAMES)
                             for n in names)
        print(output.getvalue())


QUESTION_PREFIXES = ('Ερώτηση με αρ.', 'φΕρώτηση με αρ.', '-Ερώτηση με αρ.',
                     'Περδίκη Ερώτηση με αρ.', 'Ερώτηση με 23.06.007.04.013')


def demarcate_questions(url, html):
    """Pin down question boundaries."""
    heading = None  # <Element>
    body = []       # [<Element>, ...]
    footer = []

    counter = 0
    for e in it.chain(html.xpath('//tr//*[self::hr or self::p]'),
                      (HtmlElement('Ερώτηση με αρ.'),)):    # Sentinel
        e.text = clean_spaces(e.text_content())
        norm_text = ungarble_qh(e.text)
        if norm_text.startswith(QUESTION_PREFIXES):
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


RE_HEADING = re.compile(r'Ερώτηση με(?: αρ\.)? (?P<id>[\d\.]+)'
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


def extract_answers(url, match, footer):
    answer_links = (e.xpath('.//a/@href') for e in footer)
    answer_links = it.chain.from_iterable(filter(None, answer_links))
    answer_links = sorted(set(answer_links))
    if not answer_links:
        logger.info('Unable to extract URL of answer to question with number'
                    ' {!r} in {!r}'.format(match['id'], url))
    return answer_links


def extract_names(url, heading):
    names = (Questions.NAMES.get(n) or
              logger.error('No match found for {!r} in {!r}'.format(n, url))
             for n in RE_NAMES.findall(RE_NAMES_PREPARE.sub('', heading.text)))
    return [{'mp_id': n} for n in filter(None, names)]
