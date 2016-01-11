*openpatata-scrapers* scrapes the Cypriot parliament.  This program visits pages
on the parliament website; parses information that is of interest to us; and
places it in a database, a copy of which is maintained over at
[*openpatata-data*](http://github.com/openpatata/openpatata-data).  Presently,
*openpatata-scrapers* collects and structures information on plenary sittings,
bills and regulations, and written questions.  The proximate goal of the project
is to allow for the multiform presentation and analysis of the data.  The
ultimate goal is for the Cypriot public to gain a better understanding of the
activities of their elected representatives.

*openpatata-scrapers* is written in Python (3.5+).  It requires antiword, icu4c,
libmagic, mongodb (2.6+), pandoc, pdftotext (Poppler), and a \*nix environment.

## Usage instructions

### Getting set up

Install the external dependencies listed above using your package manager of
choice.  (Do note, on OS X you've got to `brew link --force icu4c` for the
installation of PyICU to be succesful.)  Afterwards:

```bash
git clone https://github.com/openpatata/openpatata-scrapers
cd openpatata-scrapers
git clone https://github.com/openpatata/openpatata-data data

python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```

And to populate the database:

```bash
python3 -m scrapers init
```

### Running the scraper tasks

To crawl the parliament website:

```bash
python3 -m scrapers run plenary_agendas 2> error.log
python3 -m scrapers run plenary_transcripts 2>> error.log
python3 -m scrapers run questions 2>> error.log
```

To dump a mongo collection:

```bash
python3 -m scrapers dump plenary_sittings
```

The `dump` command will export each individual record in the 'plenary_sittings'
collection to YAML—a both human- and machine-readable data format—within
`data-new/plenary_sittings`.  (This operation is intended to be fully
reversible: all that can be exported can also be imported.)  To review the
changes between the old and new dataset at a glance, run
`diff -uarN data/plenary_sittings data-new/plenary_sittings`.

Execute `python3 -m scrapers -h` to view a list of all available options.

## Contributing

There's lots still to be done: setting up unit tests; parsing the transcripts
into a machine-readable data format (Akoma Ntoso or similar); collecting
information on former MPs; and much more.  If you're interested in contributing
to the cause, simply open a new ticket here on GitHub.

## Architecture

The scraper is split into thematic tasks.  A task is a class that subclasses
`crawling.Task` to define a calling method (`__call__`) and a post-processor,
`after`.

When a task's run from the command line, the `Task` class is initialised, its
`crawler` attribute is given an instance of `crawling.Crawler`, and the
coroutine that had been assigned to `__call__` is awaited. `Crawler` is a thin
wrapper around the `aiohttp` library that defines a number of methods to assist
with crawling; namely:

- `get_text(url)`: the raw text of the page at `url`.
- `get_html(url, clean=False)`: the `lxml` tree of the *HTML* page at `url`.
  When `clean` is `True`, the text content is sanitised by pandoc.
- `get_payload(url, decode=False)`: same as `get_text`, but for use with
  archives and other binary files.  When `decode` is `True`, it will attempt
  to convert PDFs and Word documents (`doc`) to text, and `docx` documents to
  JSON; and, in addition to the output, it will return the unqualified name of
  the function that performed the conversion.
- `gather(coroutines)`: same as `asyncio.gather`, except that `coroutines`
  must be a sequence or a set.

When the crawler is done, the return value of the calling method is fed to
the function `Task.after` to be operated on and inserted into the database.

```python
from scrapers.crawling import Crawler, Task


class TextOfIndexPage(Task):

    async def __call__(self):
        html = await self.crawler.get_html('http://www.parliament.cy/')
        return html

    def after(output):
        html = output
        print(html.text_content())

if __name__ == '__main__':
    Crawler(debug=True)(TextOfIndexPage)
```

## License

*openpatata-scrapers* is licensed under the AGPLv3.

*openpatata-scrapers* belongs to the commons.
