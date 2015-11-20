
"""Crawling classes."""

import asyncio
from collections import namedtuple

import aiohttp
import gridfs
from pymongo import MongoClient

from scrapers import config
from scrapers.text_utils import parse_html

_CACHE = MongoClient()[config.CACHE_DB_NAME]
_CACHE = namedtuple('_CACHE', 'file, text')(gridfs.GridFS(_CACHE),
                                            _CACHE['text'])


class Crawler:
    """An async request pool whatever and a rudimentary persisent cache.

    An instance of `Crawler` is passed down the task execution pipeline
    as each coroutine's first argument.
    """

    def __init__(self, debug=False, max_reqs=15):
        self._debug = debug

        # Limit concurrent requests to `max_reqs` to avoid flooding the
        # server. `aiohttp.BaseConnector` has also got a `limit` option,
        # but that limits the number of open _sockets_
        self._semaphore = asyncio.Semaphore(max_reqs)

    def __call__(self, task, *task_args):
        """Set off the crawler."""
        self._loop = asyncio.get_event_loop()
        self._loop.set_debug(enabled=self._debug)

        with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(use_dns_cache=True),
                loop=self._loop) as self._session:
            self._loop.run_until_complete(task(self))

    async def enqueue(self, tasks):
        """Execute the supplied sub-tasks, aggregating ther return values."""
        return await asyncio.gather(*tasks, loop=self._loop)

    async def exec_blocking(self, func, *args):
        """Execute blocking operations independently of the async loop."""
        return await self._loop.run_in_executor(None, func, *args)

    async def get_text(self, url, form_data=None, request_method='get'):
        """Retrieve the decoded content of `url`."""
        exists = _CACHE.text.find_one(dict(
            url=url, form_data=form_data, request_method=request_method))
        if exists:
            return exists['text']

        # Postpone the req until a slot has become available
        async with self._semaphore:
            async with self._session.request(request_method, url,
                                             data=form_data) as response:
                text = await response.text()
                _CACHE.text.insert_one(dict(
                    url=url, form_data=form_data,
                    request_method=request_method, text=text))
                return text

    async def get_html(self, url, clean=False,
                       **kwargs):
        """Retrieve the lxml'ed text content of `url`."""
        text = await self.get_text(url, **kwargs)
        return await self.exec_blocking(parse_html, url, text, clean)

    async def get_payload(self, url):
        """Retrieve the encoded content of `url`."""
        exists = _CACHE.file.find_one(dict(url=url))
        if exists:
            return exists.read()

        async with self._semaphore:
            async with self._session.request('get', url) as response:
                payload = await response.read()
                _CACHE.file.put(payload, url=url)
                return payload

    @classmethod
    def clear_cache(cls):
        """Clear the cache.

        Manually clear the text cache every so often, in the absence of
        a mechanism to check whether a document is stale.
        """
        _CACHE.text.drop()
