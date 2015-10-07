*openatata-scapers* scrapes the Cypriot parliament. It collects information on
plenary sittings, bills and regulations, and questions. Currently, scrapers
exist for the following: the agendas; written questions; and transcripts (no
parsing).

*openatata-scapers* requires icu4c, mongodb (2.6+), pandoc, Python (3.5+), and
a \*nix environment.

## Getting set up

```bash
git clone https://github.com/openpatata/openpatata-scrapers
cd openpatata-scrapers
git clone https://github.com/openpatata/openpatata-data data

brew install icu4c mongodb pandoc python3   # Or apt-get, or whatever
brew link --force icu4c

pyvenv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

To populate the database:

```bash
python3 scrape.py init
```

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

## License

*openatata-scapers* is licensed under the AGPLv3.
