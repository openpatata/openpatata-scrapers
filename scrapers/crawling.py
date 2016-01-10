
"""Crawling classes."""

import asyncio
from collections import namedtuple
import functools

import aiohttp
import gridfs
import magic
from pymongo import MongoClient

from scrapers import config
from scrapers.text_utils import (doc_to_text, docx_to_json, html_to_lxml,
                                 pdf_to_text)

_CACHE = MongoClient()[config.CACHE_DB_NAME]
_CACHE = namedtuple('_CACHE', 'file, text')(gridfs.GridFS(_CACHE),
                                            _CACHE['text'])


class Crawler:
    """An async request pool and a rudimentary persisent cache."""

    def __init__(self, debug=False, max_reqs=15):
        self._debug = debug
        self._max_reqs = max_reqs

    def __call__(self, task):
        """Set off the crawler."""
        self._loop = asyncio.get_event_loop()
        self._loop.set_debug(enabled=self._debug)

        # Limit concurrent requests to `max_reqs` to avoid flooding the
        # server.  `aiohttp.BaseConnector` has also got a `limit` option,
        # but that limits the number of open _sockets_
        self._semaphore = asyncio.Semaphore(self._max_reqs)

        with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(use_dns_cache=True),
                loop=self._loop) as self._session:
            output = self._loop.run_until_complete(task(self)())
        task.after(output)

    def exec_blocking(self, func):
        """Execute blocking operations independently of the async loop.

        `exec_blocking` wraps `func` inside a coroutine; call it as you
        normally would a coroutine.  Use `exec_blocking` with long-running
        blocking operations.
        """
        return functools.partial(self._loop.run_in_executor, None, func)

    async def gather(self, tasks):
        """Execute the supplied sub-tasks, aggregating ther return values."""
        return await asyncio.gather(*tasks, loop=self._loop)

    async def get_text(self, url,
                       form_data=None, request_method='get'):
        """Retrieve the decoded content of `url`."""
        exists = _CACHE.text.find_one(dict(url=url,
                                           form_data=form_data,
                                           request_method=request_method))
        if exists:
            return exists['text']

        # Postpone the req until a slot has become available
        async with self._semaphore, \
                self._session.request(request_method, url,
                                      data=form_data) as response:
            text = await response.text()
            _CACHE.text.insert_one(dict(url=url,
                                        form_data=form_data,
                                        request_method=request_method,
                                        text=text))
            return text

    async def get_html(self, url,
                       clean=False, **kwargs):
        """Retrieve the lxml'ed text content of `url`."""
        text = await self.get_text(url, **kwargs)
        return await self.exec_blocking(html_to_lxml)(url, text, clean)

    async def get_payload(self, url,
                          decode=False):
        """Retrieve the encoded content of `url`."""
        if decode is True:
            return await self._decode_payload(url, await self.get_payload(url))

        exists = _CACHE.file.find_one(dict(url=url))
        if exists:
            return exists.read()

        async with self._semaphore, \
                self._session.request('get', url) as response:
            payload = await response.read()
            _CACHE.file.put(payload, url=url)
            return payload

    async def _decode_payload(self, url, payload):
        DECODE_FUNCS = {b'application/msword': doc_to_text,
                        b'application/pdf': pdf_to_text,
                        b'application/vnd.openxmlformats-officedocument.'
                        b'wordprocessingml.document': docx_to_json}

        try:
            decode_func = DECODE_FUNCS[magic.from_buffer(payload, mime=True)]
        except KeyError:
            raise ValueError('Unable to decode {!r};'
                             ' unknown mime type'.format(url)) from None
        else:
            return decode_func.__name__, \
                await self.exec_blocking(decode_func)(payload)

    @classmethod
    def clear_cache(cls):
        """Clear the cache.

        Manually clear the text cache every so often, in the absence of
        a mechanism to check whether a document is stale.
        """
        _CACHE.text.drop()


class Task:
    """A scraping task primitive."""

    def __init__(self, crawler):
        self.crawler = self.c = crawler

    @staticmethod
    def after(output):
        """Overload to handle the output of the `Task`."""
        raise NotImplementedError
