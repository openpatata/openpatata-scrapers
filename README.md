*openpatata-scrapers* scrapes the Cypriot parliament website.  It collects and
structures information on MPs, plenary sittings, bills and regulations, and
written questions, placing it in a database, a copy of which is maintained over
at [*openpatata-data*](http://github.com/openpatata/openpatata-data).
The proximate goal of this project is to allow for the multiform presentation
and analysis of parliament data.  The ultimate goal is for the Cypriot public
to gain a better understanding of the activities of their elected
representatives.

*openpatata-scrapers* is written in Python (3.5+).  It requires antiword, icu4c,
libmagic, mongodb (2.6+), pandoc, pdftotext (Poppler) and a \*nix environment.

## Usage instructions

### Getting set up

Install the external dependencies listed above using your package manager of
choice.  (Do note, on OS X you've got to `brew link --force icu4c` for the
installation of PyICU below to work.)  Afterwards:

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

To use a custom database URI, export the env var `OPENPATATA_SCRAPERS_DB`.
Its default value is `mongodb://localhost:27017/openpatata-data`, where
`openpatata-data` is the name of the database.

### Running the scraper tasks

To crawl the parliament website:

```bash
python3 -m scrapers run plenary_sittings:plenary_agendas 2> error.log
python3 -m scrapers run plenary_sittings:plenary_transcripts 2>> error.log
python3 -m scrapers run questions:questions 2>> error.log
```

To dump a mongo collection:

```bash
python3 -m scrapers dump plenary_sittings
```

The `dump` command will export each individual record in the 'plenary_sittings'
collection to YAML—a both human- and machine-readable data format—on the disk at
`data-new/plenary_sittings`.  (This operation is intended to be fully
reversible: all that can be exported can also be imported. :)  To view the
changes between the old and new dataset at a glance, try
`diff -uarN data/plenary_sittings data-new/plenary_sittings`.

Run `python3 -m scrapers -h` to view all available options.

## Contributing

Please check the issues here on GitHub.

## Architecture

The scraper is split into thematic tasks.  A task is a class that subclasses
`crawling.Task` to define a calling method (`__call__`) and a post-processor,
`after`.

When a task's run from the command line, the `Task` class is initialised with
an instance of `crawling.Crawler` and the coroutine at `__call__` is awaited.
`Crawler` is a thin wrapper around the `aiohttp` library that adds a number of
methods to assist with crawling; namely:

- `get_text(url)`: the raw text of the page at `url`.
- `get_html(url, clean=False)`: the `lxml` tree of the *HTML* page at `url`.
  When `clean` is `True`, the text content is sanitised by pandoc.
- `get_payload(url, decode=False)`: same as `get_text` but for use with
  archives and other binary files.  When `decode` is `True`, it will attempt
  to convert PDFs and Word documents (`doc`) to text, and `docx` documents to
  JSON.  In addition to the regular output, it will return the unqualified name
  of the function that performed the conversion.
- `gather(coroutines)`: same as `asyncio.gather`, except that `coroutines`
  must be a sequence or a set.

The output of `__call__` is fed to `Task.after` to be synchronously parsed and
inserted into the database.

Below is a simplified example of a task.

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
