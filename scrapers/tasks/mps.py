
import csv
from io import StringIO
import itertools as it
import json

from ..crawling import Task
from ..models import (ContactDetails, Identifier, Link,
                      MP, MultilingualField, OtherName)
from ..reconciliation import pair_name, load_pairings
from ..text_utils import (clean_spaces, parse_long_date,
                          translit_elGrek2Latn, translit_el2tr)


class MpProfiles(Task):

    DISTRICTS = load_pairings('district_names.csv')
    NAMES = load_pairings('profile_names.csv')
    PARTIES = load_pairings('party_names.csv')

    async def __call__(self):
        listing_urls = ('http://www.parliament.cy/easyconsole.cfm/id/2004',
                        'http://www.parliament.cy/easyconsole.cfm/id/2033',
                        'http://www.parliament.cy/easyconsole.cfm/id/186',
                        'http://www.parliament.cy/easyconsole.cfm/id/182')

        mp_urls = await self.c.gather(self.process_multi_page_listing(u)
                                      for u in listing_urls)
        return await self.c.gather(self.process_mp(u, t)
                                   for i in mp_urls for u, t in i)

    async def process_multi_page_listing(self, url):
        html = await self.c.get_html(url)
        pages = html.xpath('//a[contains(@class, "pagingStyle")]/@href')
        if pages:
            mps = await self.c.gather(self.process_multi_page_page(
                url, form_data={'page': ''.join(c for c in p
                                                if c.isdigit())}) for p in pages)
            mps = it.chain.from_iterable(mps)
        else:
            mps = await self.process_multi_page_page(url)
        return zip(mps,
                   it.repeat({'182': '11', '186': '11', '2004': '10', '2033': '10'
                              }[url.rpartition('/')[-1]]))

    async def process_multi_page_page(self, url, form_data=None):
        html = await self.c.get_html(url,
                                     form_data=form_data, request_method='post')
        return html.xpath('//a[@class="h3Style"]/@href')

    async def process_mp(self, url, term):
        return url, await self.c.get_html(f'{url}/lang/el'), term

    def parse_item(url, html, term):
        name = clean_spaces(html.xpath('string(//h1)'))
        name = MultilingualField(el=name,
                                 en=translit_elGrek2Latn(name),
                                 tr=translit_el2tr(name))

        extractor = extract_contact_details(url, html, 'el')

        birth_date, place_of_origin = extract_birth_details(url, html, 'el')

        district, party = html.xpath('string(//div[@class = "articleBox"]/p[1])').splitlines()
        district = district.rpartition(' ')[-1]

        contact_details = [ContactDetails(type=t, value=clean_spaces(f(i), True))
                           for l, t, d, f in CONTACT_DETAIL_SUBS if extractor.get(l)
                           for i in split_contact_details(extractor[l], d)]
        for i in contact_details:
            if (((i['type'] == 'fax' or i['type'] == 'voice') and
                 '22 407' in i['value']) or
                (i['type'] == 'email' and
                 i['value'].endswith('@parliament.cy')) or
                (i['type'] == 'address' and
                 'Βουλή των Αντιπροσώπων' in i['value'])):
                i['note'] = 'parliament'
        links = [Link(note=MultilingualField(**n), url=f(extractor[l], html))
                 for l, n, d, f in LINK_SUBS if extractor.get(l)
                 for i in extractor[l].split(d)]

        mp = MP(_id=MpProfiles.NAMES.get(name['el']),
                _sources=[url],
                birth_date=birth_date,
                email=email,
                images=extract_images(html),
                # name=name,
                # other_names=[OtherName(name=clean_spaces(html_en.xpath('string(//h1)')),
                #                        note="Official spelling in the 'en' locale")],
                memberships=[MP.TermOfOffice(electoral_district_id=...,
                                             parliamentary_period_id=term,
                                             party_id=...,
                                             contact_details=contact_details,
                                             links=links)],
                place_of_origin=MultilingualField(el=place_of_origin))
        mp.insert(merge=mp.exists)


class ReconcileProfileNames(MpProfiles):

    def after(output):
        names_and_ids = {i['_id']: i['name']['el'] for i in MP.collection.find()}
        names = tuple(clean_spaces(h.xpath('string(//h1)'))
                      for _, h, _ in output)
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(('name', 'id'))
        csv_writer.writerows(pair_name(n, names_and_ids, MpProfiles.NAMES)
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
            logger.info(f'Updating identifiers of {mp._id!r} with {mp.data["identifiers"]!r}')
            mp.insert(merge=mp.exists)


def extract_id(person, scheme):
    return next((i['identifier']
                 for i in person['identifiers'] if i['scheme'] == scheme),
                None)


class ContactInfo(Task):

    NAMES = load_pairings('contact_names.csv')

    async def __call__(self):
        url = 'http://www.parliament.cy/easyconsole.cfm/id/185'
        return await self.c.get_html(url)

    def after(output):
        for mp_name, _, voice, email in extract_contact_rows(output):
            if not ContactInfo.NAMES.get(mp_name):
                logger.warning(f'{mp_name!r} not found in `NAMES`; skipping')
                continue
            voice = ''.join(voice.split())
            voice = f'(+357) {voice[:2]} {voice[2:]}'
            mp = MP(_id=ContactInfo.NAMES[mp_name],
                    _sources=['http://www.parliament.cy/easyconsole.cfm/id/185'],
                    contact_details=[ContactDetails(type='email', value=email,
                                                    note='parliament'),
                                     ContactDetails(type='voice', value=voice,
                                                    note='parliament')],
                    email=email)
            try:
                mp.insert(mp.exists)
            except mp.InsertError:
                logger.warning(f'Unable to insert {mp!r}')


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


def extract_items(html, lang, *items):
    return html.xpath(f'''//p[{' or '.join(f'contains(strong/text(), "{LABELS[lang][i]}")'
                                           for i in items)}]/text()''')


def extract_birth_details(url, html, lang):
    birth_date, place_of_origin = None, None
    data = ' '.join(extract_items(html, lang, 'poo dob', 'poo'))
    data = clean_spaces(''.join(data).strip(':.'), medial_newlines=True)
    if data:
        try:
            place_of_origin, birth_date = data.split(',')
        except ValueError:
            logger.error(f'No birth date found in {url!r}')
            place_of_origin = data
        else:
            birth_date = parse_long_date(birth_date)
    else:
        logger.error(f'No birth date or place of origin found in {url!r}')
    return birth_date, place_of_origin


def extract_contact_details(url, html, lang):
    heading = {'el': 'Στοιχεία επικοινωνίας', 'en': 'Contact info'}[lang]
    try:
        contact_details, = html.xpath(f'//p[contains(strong/text(), "{heading}")]'
                                       '/following-sibling::p[1]')
    except ValueError:
        logger.error(f"Could not extract contact details in '{url}/lang/{lang}'")
        return {}
    else:
        contact_details = dict(zip(
            (clean_spaces(i.strip(': '), True)
             for i in contact_details.xpath('./strong/text()')),
            filter(None,
                   (clean_spaces(i.strip(': '), True)
                    for i in ''.join(i.xpath('string()') if hasattr(i, 'xpath') else i
                                     for i in contact_details.xpath('./node()[not(self::strong)]')
                                     ).splitlines()
             ))))
        return contact_details


def extract_images(html):
    images = html.xpath('//a[@class = "lightview"]/@href | '
                        '//a[contains(@href, "/assets/image/imageoriginal")]/@href')
    images = sorted(set(images), key=images.index)
    return images


def extract_homepage(url, html):
    try:
        url, = html.xpath(f'//a[contains(string(.), "{url}")]/@href')
        url = url.rstrip('/') + '/'
        return url
    except ValueError:
        if url.startswith('http'):
            return url
        return f'http://{url.rstrip("/")}/'


def split_contact_details(s, delimiters):
    for d in delimiters:
        if d in s:
            return s.split(d)
    return [s]


CONTACT_DETAIL_SUBS = (
    ('Διεύθυνση', 'address', '/', lambda v: v),
    ('Τηλέφωνο', 'voice', ',', lambda v: '(+357) ' + v.replace('(+357) ', '')),
    ('Τηλεομοιότυπο', 'fax', ',', lambda v: '(+357) ' + v.replace('(+357) ', '')),
    ('Ηλεκτρονικό ταχυδρομείο', 'email', ';,', lambda v: v),)

LINK_SUBS = (
    ('Διαδικτυακός τόπος',
     {'el': 'Προσωπική ιστοσελίδα', 'en': 'Personal website', 'tr': 'Kişisel websitesi'},
     ', ',
     extract_homepage),
    ('Λογαριασμός στο Twitter',
     {'el': 'twitter', 'en': 'twitter', 'tr': 'twitter'},
     ', ',
     lambda v, _: 'https://twitter.com/' + v.lstrip('@')),)
