
import csv
from io import StringIO
import itertools as it
import json
import logging
from pathlib import Path

from ._models import (ContactDetails, Identifier, Link,
                      MP, MultilingualField, OtherName)
from .questions import pair_name
from ..crawling import Task
from ..text_utils import (clean_spaces, parse_long_date,
                          translit_elGrek2Latn, translit_el2tr)

logger = logging.getLogger(__name__)


class MPs(Task):

    with (Path(__file__).parent.parent
          /'data'/'reconciliation'/'profile_names.csv').open() as file:
        NAMES = dict(it.islice(csv.reader(file), 1, None))

    async def __call__(self):
        listing_urls = ('http://www.parliament.cy/easyconsole.cfm/id/186',
                        'http://www.parliament.cy/easyconsole.cfm/id/2004')

        mp_urls = await self.c.gather(map(self.process_multi_page_listing,
                                          listing_urls))
        mp_urls = tuple(it.chain.from_iterable(mp_urls))
        return zip(
            mp_urls,
            await self.c.gather(self.c.get_html(u + '/lang/el') for u in mp_urls),
            await self.c.gather(self.c.get_html(u + '/lang/en') for u in mp_urls))

    async def process_multi_page_listing(self, url):
        html = await self.c.get_html(url)
        pages = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
        if pages:
            pages = {self.process_multi_page_page(
                      url, form_data={'page': ''.join(filter(str.isdigit, p))})
                     for p in pages}
            return it.chain.from_iterable(await self.c.gather(pages))
        else:
            return await self.process_multi_page_page(url)

    async def process_multi_page_page(self, url, form_data=None):
        html = await self.c.get_html(url, form_data=form_data,
                                     request_method='post')
        return html.xpath('//a[@class="h3Style"]/@href')

    def after(output):
        for url, html_el, html_en in output:
            extractor = extract_contact_details(url, 'el', html_el)
            contact_details = [ContactDetails(type=t, value=f(i))
                                for l, t, d, f in CONTACT_DETAIL_SUBS
                                if extractor.get(l)
                                for i in extractor[l].split(d)]
            email = next((i['value'] for i in contact_details if i['type'] == 'email'),
                         None)
            images = extract_images(html_el)
            links = [Link(note=MultilingualField(**n), url=f(extractor[l], html_el))
                                for l, n, d, f in LINK_SUBS if extractor.get(l)
                                for i in extractor[l].split(d)]
            name = clean_spaces(html_el.xpath('string(//h1)'))
            name = MultilingualField(el=name, en=translit_elGrek2Latn(name),
                                     tr=translit_el2tr(name))
            other_names = [OtherName(name=clean_spaces(html_en.xpath('string(//h1)')),
                                     note="Official spelling in the 'en' locale;"
                                          " possibly anglicised")]

            mp = MP(_id=MPs.NAMES.get(name['el']),
                    _sources=[url],
                    birth_date=extract_birth_date_place(url, html_el)[0],
                    contact_details=contact_details,
                    email=email,
                    image=[*images, None][0],
                    images=images,
                    links=links,
                    name=name,
                    other_names=other_names)
            mp.insert(merge=mp.exists)


class ReconcileProfileNames(MPs):

    def after(output):
        names_and_ids = {i['_id']: i['name']['el'] for i in MP.collection.find()}
        names = tuple(clean_spaces(h.xpath('string(//h1)'))
                      for _, h, _ in output)
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(('name', 'id'))
        csv_writer.writerows(pair_name(n, names_and_ids, MPs.NAMES)
                             for n in names)
        print(output.getvalue())


class WikidataIds(Task):

    async def __call__(self):
        url = ('https://cdn.rawgit.com/everypolitician/everypolitician-data'
               '/master/data/Cyprus/House_of_Representatives/ep-popolo-v1.0.json')
        return await self.c.get_payload(url)

    def after(ep_data):
        ep_persons = (p for p in json.loads(ep_data.decode())['persons']
                      if extract_id(p, 'wikidata'))
        for p in ep_persons:
            wd = Identifier(identifier=extract_id(p, 'wikidata'),
                            scheme='http://www.wikidata.org/entity/')
            mp = MP(_id=extract_id(p, 'openpatata'), identifiers=[wd])
            logger.info('Updating identifiers of {!r} with {!r}'
                        .format(mp._id, mp.data['identifiers']))
            mp.insert(merge=mp.exists)


def extract_id(person, scheme):
    return next((i['identifier']
                 for i in person['identifiers'] if i['scheme'] == scheme),
                None)


class ContactInfo(Task):

    with (Path(__file__).parent.parent
          /'data'/'reconciliation'/'contact_names.csv').open() as file:
        NAMES = dict(it.islice(csv.reader(file), 1, None))

    async def __call__(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/185'
        return await self.c.get_html(url)

    def after(output):
        for mp_name, _, voice, email in extract_contact_rows(output):
            if not ContactInfo.NAMES.get(mp_name):
                logger.warning('{!r} not found in `NAMES`; skipping'
                               .format(mp_name))
                continue
            voice = ''.join(voice.split())
            voice = '(+357) {} {}'.format(voice[:2], voice[2:])
            mp = MP(_id=ContactInfo.NAMES[mp_name],
                    _sources=['http://www.parliament.cy/easyconsole.cfm/id/185'],
                    contact_details=[ContactDetails(type='email', value=email),
                                     ContactDetails(type='voice', value=voice)],
                    email=email)
            try:
                mp.insert(mp.exists)
            except mp.InsertError:
                logger.warning('Unable to insert ' + repr(mp))


class ReconcileContactNames(ContactInfo):

    def after(output):
        names_and_ids = {i['_id']: i['name']['el'] for i in MP.collection.find()}
        names = tuple(r[0] for r in extract_contact_rows(output))
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(('name', 'id'))
        csv_writer.writerows(pair_name(n, names_and_ids, ContactInfo.NAMES)
                             for n in names)
        print(output.getvalue())


def extract_contact_rows(rows):
    rows = rows.xpath('//div[@class = "articleBox"]/div[2]/table[1]//tr')
    rows = (tuple(clean_spaces(cell.text_content()) for cell in row)
            for row in rows)
    rows = (row[1:] for row in rows if row[0].isnumeric())
    return rows


LABELS = {
    'el': {
        'foreign languages': 'Ξένες γλώσσες',
        'pob dob': 'Τόπος και ημερομηνία γέννησης',
        'poo dob': 'Τόπος καταγωγής και ημερομηνία γέννησης',
        'poo': 'Τόπος καταγωγής',
        'profession': 'Επάγγελμα',
        'studies': 'Σπουδές',},
    'en': {
        'foreign languages': 'Foreign languages',
        'pob dob': 'Place of origin and date of birth',
        'poo dob': 'Place and date of birth',
        'poo': 'Place of origin',
        'profession': 'Profession',
        'studies': 'Studies',}}


def extract_item(el, en):
    html = locals()

    def inner(lang, *items):
        for item in items:
            item = html[lang].xpath('//p[contains(strong/text(), "{}")]/text()'
                                    .format(LABELS[lang][item]))
            if item:
                yield item
    return inner


def extract_birth_date_place(url, html):
    extractor = extract_item(html, None)
    birth_date, birth_place = None, None
    try:
        birth_things = next(extractor('el', 'poo dob', 'pob dob', 'poo'))
    except StopIteration:
        logger.error('No date and place of birth found in ' + repr(url))
    else:
        birth_things = clean_spaces(''.join(birth_things).strip(':.'),
                                    medial_newlines=True)
        try:
            birth_place, birth_date = birth_things.split(',')
        except ValueError:
            logger.error('No date of birth found in ' + repr(url))
            birth_place = birth_things
        else:
            birth_date = parse_long_date(birth_date)
    return birth_date, birth_place


def extract_contact_details(url, lang, html):
    heading = {'el': 'Στοιχεία επικοινωνίας', 'en': 'Contact info'}[lang]
    try:
        contact_details = next(iter(html.xpath(
            '//p[contains(strong/text(), "{}")]/following-sibling::p'
            .format(heading))))
    except StopIteration:
        logger.error("Could not extract contact details in '{}/lang/{}'"
                     .format(url, lang))
        return {}
    else:
        contact_details = dict(zip(
            (clean_spaces(i.strip(': '), True)
             for i in contact_details.xpath('./strong/text()')),
            (clean_spaces(i.strip(': '), True)
             for i in ''.join(i.xpath('string()') if hasattr(i, 'xpath') else i
                              for i in contact_details.xpath('./node()[not(self::strong)]')
                              ).splitlines()
             )))
        return contact_details


def extract_images(html):
    images = tuple(it.chain(html.xpath('//a[@class = "lightview"]/@href'),
                            html.xpath('//a[contains(@href, "/assets/image/imageoriginal")]'
                                       '/@href')))
    images = sorted(set(images), key=images.index)
    return images


def extract_homepage(url, html):
    try:
        url, = html.xpath('//a[contains(string(.), "{}")]/@href'.format(url))
        url = url.rstrip('/') + '/'
        return url
    except ValueError:
        if url.startswith('http'):
            return url
        return 'http://{}/'.format(url.rstrip('/'))


CONTACT_DETAIL_SUBS = (
    ('Διεύθυνση', 'address', ' / ', lambda v: v),
    ('Τηλέφωνο', 'voice', ', ', lambda v: '(+357) ' + v.replace('(+357) ', '')),
    ('Τηλεομοιότυπο', 'fax', ', ', lambda v: '(+357) ' + v.replace('(+357) ', '')),
    ('Ηλεκτρονικό ταχυδρομείο', 'email', ', ', lambda v: v),)

LINK_SUBS = (
    ('Διαδικτυακός τόπος',
     {'el': 'Προσωπική ιστοσελίδα', 'en': 'Personal website', 'tr': 'Kişisel websitesi'},
     ', ',
     extract_homepage),
    ('Λογαριασμός στο Twitter',
     {'el': 'twitter', 'en': 'twitter', 'tr': 'twitter'},
     ', ',
     lambda v, _: 'https://twitter.com/' + v.lstrip('@')),)
