*openpatata-scrapers* scrapes the Cypriot parliament. This program visits pages
on the parliament website; parses all the relevant information; and places it in
a database, a copy of which is maintained at
[openpatata-data](http://github.com/openpatata/openpatata-data). Presently,
*openpatata-scrapers* collects and structures information on plenary sittings,
bills and regulations, and written questions. The proximate goal of the project
is to allow for the multiform presentation and analysis of the data; the
ultimate goal is for the Cypriot public to gain a better understanding of the
activities of their elected representatives. As of October 2015, and to the best
of my knowledge, the only user of the data is the
[openpatata](http://github.com/openpatata/openpatata) website.

*openpatata-scrapers* is written in Python (3.5+). It requires icu4c,
mongodb (2.6+), pandoc, and a \*nix environment.

## Usage instructions

### Getting set up

```bash
git clone https://github.com/openpatata/openpatata-scrapers
cd openpatata-scrapers
git clone https://github.com/openpatata/openpatata-data data

brew install icu4c mongodb pandoc python3   # Or apt-get or dnf or whatever
brew link --force icu4c

pyvenv venv
source venv/bin/activate
pip3 install -r requirements.txt            # Or python3-pip ...
```

And to populate the database:

```bash
python3 scrape.py init
```

### Running the scrapers

To crawl the parliament website:

```bash
python3 scrape.py run agendas 2> error.log
python3 scrape.py run transcripts 2>> error.log
python3 scrape.py run qas 2>> error.log
```

To dump a mongo collection:

```bash
python3 scrape.py dump plenary_sittings data-new
```

Execute `python3 scrape.py -h` to view a list of all available options.

## Contributing

There's lots still to be done: setting up unit tests; parsing transcripts into
a machine-readable format (Akoma Ntoso or similar); extracting MP attendance
statistics from transcripts; collecting information on former MPs; and much
more. If you're interested in contributing to the cause, simply open a new
ticket here on GitHub.

## License

*openpatata-scrapers* is licensed under the AGPLv3. This means that the software
can be modified and reused on the following provisions: (a) attribution is
maintained; and (b) derived works are made publicly available under the same
conditions.

*openpatata-scrapers* belongs to the commons.
