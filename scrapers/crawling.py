
"""Crawling classes."""

import asyncio
import logging
from urllib.parse import urldefrag

import aiohttp
import gridfs
import lxml.html
import pypandoc

from . import db

logger = logging.getLogger(__name__)


def _parse_html(url, text, clean=False):
    """Parse HTML into an lxml tree."""
    if clean:
        text = pypandoc.convert(text, 'html5', format='html')
    html = lxml.html.document_fromstring(text)
    # Infinite loops ahoy
    html.rewrite_links(lambda s: None if urldefrag(s).url == url else s,
                       base_href=url)
    return html


class Crawler:
    """An HTTP request pool whatever and a rudimentary persisent cache."""

    def __init__(self, max_reqs=15):
        self._cache = db['_cache']
        self._gfs = gridfs.GridFS(db)

        # Limit concurrent connections to `max_reqs` to avoid flooding the
        # server. `aiohttp.BaseConnector` has also got a `limit` option,
        # but I've not managed to get it to work the way it should
        self._semaphore = asyncio.Semaphore(max_reqs)

        self._loop = asyncio.get_event_loop()
        self._session = aiohttp.ClientSession(loop=self._loop)

    def clear_cache(self):
        """Clear the cache."""
        self._cache.drop()

    def close(self):
        """Done crawling."""
        self._session.close()

    def exec_blocking(self, func, *args):
        """Execute blocking operations independently of the async loop."""
        return self._loop.run_in_executor(None, func, *args)

    async def get_text(self, url, form_data=None, request_method='get'):
        """Retrieve the decoded content of `url`."""
        exists = self._cache.find_one(dict(
            url=url, form_data=form_data, request_method=request_method))
        if exists:
            return exists['text']

        # Postpone the request until a slot has become available
        async with self._semaphore:
            response = await self._session.request(request_method, url,
                                                   data=form_data)
            text = await response.text()
            self._cache.insert_one(dict(
                url=url, form_data=form_data, request_method=request_method,
                text=text))
            return text

    async def get_html(self, url, clean=False,
                       **kwargs):
        """Retrieve the lxml'ed text content of `url`."""
        text = await self.get_text(url, **kwargs)
        return _parse_html(url, text, clean)

    async def get_payload(self, url):
        """Retrieve the encoded content of `url`."""
        exists = self._cache.find_one(dict(url=url))
        if exists:
            return exists.read()

        async with self._semaphore:
            response = await self._session.request('get', url)
            payload = await response.read()
            self._gfs.put(payload, url=url)
            return payload
