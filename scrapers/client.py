
import asyncio
import builtins
from collections import namedtuple
import datetime as dt
from functools import partial
import itertools as it
import logging

from aiohttp import ClientSession, TCPConnector, ClientResponseError
import gridfs
import magic

from . import config, get_db
from .text_utils import doc_to_text, docx_to_json, parse_html, pdf_to_text


_CACHE = get_db(config.CACHE_DB)
_CACHE = namedtuple('_CACHE', 'file text')(gridfs.GridFS(_CACHE),
                                           _CACHE['text'])


class Client:

    ClientResponseError = ClientResponseError

    def __init__(self, debug=False):
        self._loop = asyncio.get_event_loop()
        self._loop.set_debug(enabled=debug)

    def __call__(self, task):
        with ClientSession(connector=TCPConnector(use_dns_cache=True,
                                                  limit_per_host=10,
                                                  loop=self._loop),
                           raise_for_status=True,
                           loop=self._loop) \
                as self._session:
            output = self._loop.run_until_complete(task(self)())
        return task.after(output)

    def exec_blocking(self, func):
        return partial(self._loop.run_in_executor, None, func)

    async def gather(self, tasks):
        return await asyncio.gather(*tasks, loop=self._loop)

    async def get_text(self, url, *,
                        form_data=None, request_method='get', params=None):
        exists = _CACHE.text.find_one(dict(url=url,
                                           form_data=form_data,
                                           request_method=request_method))
        if exists:
            return exists['text']

        async with self._session.request(request_method, url,
                                         data=form_data, params=params) \
                as response:
            text = await response.text()
        _CACHE.text.insert_one(dict(url=url,
                                    form_data=form_data,
                                    request_method=request_method,
                                    text=text))
        return text

    async def get_html(self, url, *, clean=False, **kwargs):
        return parse_html(url, (await self.get_text(url, **kwargs)), clean)

    async def get_payload(self, url, *, decode=False, params=None):
        if decode is True:
            return await self._decode_payload(url, await self.get_payload(url))

        exists = _CACHE.file.find_one(dict(url=url))
        if exists:
            return exists.read()

        async with self._session.get(url, params=params) as response:
            payload = await response.read()
        _CACHE.file.put(payload, url=url)
        return payload

    async def _decode_payload(self, url, payload):
        DECODE_FUNCS = {'application/msword': doc_to_text,
                        'application/pdf': pdf_to_text,
                        'application/vnd.openxmlformats-officedocument.'
                        'wordprocessingml.document': docx_to_json}

        try:
            decode_func = DECODE_FUNCS[magic.from_buffer(payload, mime=True)]
        except KeyError:
            raise ValueError(f'Unable to decode {url!r}; unknown mime type')
        else:
            return decode_func.__name__, \
                await self.exec_blocking(decode_func)(payload)

    @classmethod
    def clear_text_cache(cls):
        _CACHE.text.drop()


def dump_cache(cache_path=None):
    from pathlib import Path
    from urllib.parse import urlencode, urlparse, urlunparse

    cache_dir = Path(cache_path or 'cache-dump')
    cache_dir.mkdir(exist_ok=True)
    for file in _CACHE.file.find():
        path = Path(cache_dir, file.url.replace('://', '%3A%2F%2F'))
        path.parent.mkdir(exist_ok=True, parents=True)
        with path.open('wb') as file_handle:
            file_handle.write(file.read())
    for file in _CACHE.text.find():
        url = urlparse(file['url'])._asdict()
        if file['form_data']:
            url['query'] = urlencode(file['form_data'])
        path = Path(cache_dir, urlunparse(url.values()).replace('://', '%3A%2F%2F'))
        path.parent.mkdir(exist_ok=True, parents=True)
        with path.open('w') as file_handle:
            file_handle.write(file['text'])
    with (cache_dir/'VERSION').open('w') as file_handle:
        file_handle.write(dt.datetime.now().isoformat())


def _camel_to_snake(s):
    name = ''.join(('_' if c is True else '') + ''.join(t)
                   for c, t in it.groupby(s, key=lambda i: i.isupper()))
    name = name.lower().strip('_')
    return name


class Task:

    __tasks__ = {}

    def __init__(self, client):
        self.client = self.c = client

    def __init_subclass__(cls):
        cls.__tasks__[_camel_to_snake(cls.__name__)] = cls

    @classmethod
    def after(cls, output):
        for item in output:
            cls.parse_item(*item)

    def parse_item(*args):
        raise NotImplementedError
