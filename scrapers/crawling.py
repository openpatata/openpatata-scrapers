
"""Crawling classes."""

import asyncio
import builtins
from collections import namedtuple
import datetime as dt
import functools
import logging
from pathlib import Path
from urllib.parse import urlencode, urlparse, urlunparse

from aiohttp import ClientSession, TCPConnector
import gridfs
import magic

from . import config, Db
from .text_utils import doc_to_text, docx_to_json, html_to_lxml, pdf_to_text

_CACHE = Db.get(config.CACHE_DB)
_CACHE = namedtuple('_CACHE', 'file text')(gridfs.GridFS(_CACHE),
                                           _CACHE['text'])


class Crawler:
    """An async request pool and a rudimentary persisent cache."""

    def __init__(self, debug=False, max_reqs=15):
        self._loop = asyncio.get_event_loop()
        self._loop.set_debug(enabled=debug)

        # Limit concurrent requests to `max_reqs` to avoid flooding the
        # server.  `aiohttp.BaseConnector` has also got a `limit` option,
        # but that limits the number of open _sockets_
        self._semaphore = asyncio.Semaphore(max_reqs)

    def __call__(self, task):
        """Set off the crawler."""
        with ClientSession(connector=TCPConnector(use_dns_cache=True),
                           loop=self._loop) \
                as self._session:
            output = self._loop.run_until_complete(task(self)())
        return task.after(output)

    def exec_blocking(self, func):
        """Execute blocking operations independently of the async loop.

        `exec_blocking` wraps `func` inside a coroutine; call it as you
        normally would a coroutine.  Use `exec_blocking` with long-running
        blocking operations.
        """
        return functools.partial(self._loop.run_in_executor, None, func)

    async def gather(self, tasks):
        """Execute the supplied sub-tasks, aggregating their return values."""
        return await asyncio.gather(*tasks, loop=self._loop)

    async def get_text(self, url, *,
                        form_data=None, request_method='get', params=None):
        """Retrieve the decoded content of `url`."""
        exists = _CACHE.text.find_one(dict(url=url,
                                           form_data=form_data,
                                           request_method=request_method))
        if exists:
            return exists['text']

        # Postpone the req until a slot has become available
        async with self._semaphore, \
                self._session.request(request_method, url,
                                      data=form_data, params=params) as response:
            text = await response.text()
            _CACHE.text.insert_one(dict(url=url,
                                        form_data=form_data,
                                        request_method=request_method,
                                        text=text))
            return text

    async def get_html(self, url, *, clean=False, **kwargs):
        """Retrieve the lxml'ed text content of `url`."""
        text = await self.get_text(url, **kwargs)
        return await self.exec_blocking(html_to_lxml)(url, text, clean)

    async def get_payload(self, url, *, decode=False, params=None):
        """Retrieve the encoded content of `url`."""
        if decode is True:
            return await self._decode_payload(url, await self.get_payload(url))

        exists = _CACHE.file.find_one(dict(url=url))
        if exists:
            return exists.read()

        async with self._semaphore, \
                self._session.request('get', url, params=params) as response:
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
            raise ValueError(f'Unable to decode {url!r}; unknown mime type') \
                from None
        else:
            return decode_func.__name__, \
                await self.exec_blocking(decode_func)(payload)

    @classmethod
    def clear_text_cache(cls):
        """Clear the text cache.

        Manually clear the text cache every so often, in the absence of
        a mechanism to check whether a document is stale.
        """
        _CACHE.text.drop()

    @classmethod
    def dump_cache(cls, cache_path=None):
        """Save the cache on disk."""
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


class Task:
    """A scraping task primitive."""

    def __init__(self, crawler):
        builtins.logger = logging.getLogger(self.__class__.__name__)
        self.crawler = self.c = crawler

    @classmethod
    def after(cls, output):
        for item in output:
            cls.parse_item(*item)

    def parse_item(*args):
        raise NotImplementedError
