
import itertools as it
import json
import logging

from ._models import Identifier, MP, MultilingualField, OtherName
from ..crawling import Task
from ..text_utils import (clean_spaces, parse_long_date,
                          translit_elGrek2Latn, translit_el2tr)

logger = logging.getLogger(__name__)


class MPs(Task):

    async def __call__(self):
        listing_urls = ('http://www.parliament.cy/easyconsole.cfm/id/181',
                        'http://www.parliament.cy/easyconsole.cfm/id/2004')

        mp_urls = await self.c.gather(map(self.process_multi_page_listing,
                                          listing_urls))
        mp_urls = tuple(it.chain.from_iterable(mp_urls))
        return zip(mp_urls,
                   await self.c.gather(self.c.get_html(url + '/lang/el')
                                       for url in mp_urls),
                   await self.c.gather(self.c.get_html(url + '/lang/en')
                                       for url in mp_urls))

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
            _parse_mp(url, html_el, html_en)


class WikidataIds(Task):

    async def __call__(self):
        url = ('https://cdn.rawgit.com/everypolitician/everypolitician-data'
               '/master/data/Cyprus/House_of_Representatives/ep-popolo-v1.0.json')
        return await self.c.get_payload(url)

    def after(ep_data):
        ep_persons = (p for p in json.loads(ep_data.decode())['persons']
                      if _extract_id(p, 'wikidata'))
        for p in ep_persons:
            wd = Identifier(identifier=_extract_id(p, 'wikidata'),
                             scheme='http://www.wikidata.org/entity/')
            mp = MP(_id=_extract_id(p, 'openpatata'), identifiers=[wd])
            logger.info('Updating identifiers of {!r} with {!r}'
                        .format(mp._id, mp.data['identifiers']))
            mp.insert(merge=mp.exists)


def _extract_id(person, scheme):
    return next((i for i in person['identifiers'] if i['scheme'] == scheme),
                {}).get('identifier')


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


def _extract_item(el, en):
    html = locals()

    def inner(lang, *items):
        for item in items:
            item = html[lang].xpath('//p[contains(strong/text(), "{}")]/text()'
                                    .format(LABELS[lang][item]))
            if item:
                yield item
    return inner


def _extract_birth_date_place(url, html):
    extract_item = _extract_item(html, None)
    birth_date, birth_place = None, None
    try:
        birth_things = next(extract_item('el', 'poo dob', 'pob dob', 'poo'))
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


def _extract_contact_details(url, lang, html):
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


def _extract_images(html):
    images = html.xpath('//a[contains(@href, "/assets/image/imageoriginal")]'
                        '/@href')
    images = sorted(set(images), key=images.index)
    return images


def _parse_mp(url, html_el, html_en):
    name = clean_spaces(html_el.xpath('string(//h1)'))
    mp = MP(
        _sources=[url],
        birth_date=_extract_birth_date_place(url, html_el)[0],
        images=_extract_images(html_el),
        name=MultilingualField(el=name,
                               en=translit_elGrek2Latn(name),
                               tr=translit_el2tr(name)),
        other_names=[OtherName(name=clean_spaces(html_en.xpath('string(//h1)')),
                               note="Official spelling in the 'en' locale;"
                                    " possibly anglicised")])
    try:
        mp.insert(merge=mp.exists)
    except mp.InsertError as e:
        logger.error(e)
