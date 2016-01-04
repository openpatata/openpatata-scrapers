*openpatata-scrapers* scrapes the Cypriot parliament. This program visits pages
on the parliament website; parses all the relevant information; and places it in
a database, a copy of which is maintained at
[openpatata-data](http://github.com/openpatata/openpatata-data). Presently,
*openpatata-scrapers* collects and structures information on plenary sittings,
bills and regulations, and written questions. The proximate goal of the project
is to allow for the multiform presentation and analysis of the data; the
ultimate goal is for the Cypriot public to gain a better understanding of the
activities of their elected representatives. As of October 2015, and to the best
of my knowledge, the only user of the data is the
[openpatata](http://github.com/openpatata/openpatata) website.

*openpatata-scrapers* is written in Python (3.5+). It requires antiword, icu4c,
libmagic, mongodb (2.6+), pandoc, pdftotext (Poppler), and a \*nix environment.

## Usage instructions

### Getting set up

Install all dependencies:

```bash
git clone https://github.com/openpatata/openpatata-scrapers
cd openpatata-scrapers
git clone https://github.com/openpatata/openpatata-data data

brew install antiword icu4c libmagic mongodb pandoc python3   # Or apt-get or dnf or whatever
brew link --force icu4c

python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```

And to populate the database:

```bash
python3 -m scrapers init
```

On Linux distros, the installation of lxml and PyICU is... nerve-racking. You'd
probably be better off using the system packages: look for `python3-lxml` and
`python3-pyicu`.

### Running the scrapers

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

This command will export each individual record in the "plenary_sittings"
collection to YAML—a both human- and machine-readable data format—within
`data-new/plenary_sittings`. (This operation is intended to be fully reversible;
which is to say that the data on disk should be identical to the data in the
database.) To review the changes between the old and new dataset at a glance,
run `diff -uarN data/plenary_sittings data-new/plenary_sittings`.

Execute `python3 -m scrapers -h` to view a list of all available options.

## Contributing

There's lots still to be done: setting up unit tests and caching; parsing
transcripts into a machine-readable format (Akoma Ntoso or similar); extracting
MP attendance statistics from transcripts; collecting information on former MPs;
and much more. If you're interested in contributing to the cause, simply open a
new ticket here on GitHub.

## License

*openpatata-scrapers* is licensed under the AGPLv3. This means that the software
can be modified and reused on the following provisions: (a) attribution is
maintained; and (b) derived works are made publicly available under the same
conditions.

*openpatata-scrapers* belongs to the commons.
