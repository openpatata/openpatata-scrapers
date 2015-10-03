*openatata-scapers* scrapes the Cypriot parliament. It collects information on
plenary sittings, bills and regulations, and questions.

## Getting set up

```bash
git clone https://github.com/openpatata/openpatata-scrapers
cd openpatata-scrapers
git clone https://github.com/openpatata/openpatata-data data

brew install icu4c mongodb python3
brew link --force icu4c

pyvenv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

To populate the database:

```bash
python3 insert.py
```

To crawl the parliament website:

```bash
python3 scrape.py run agendas 2> error.log
python3 scrape.py run transcripts 2>> error.log
python3 scrape.py run qas 2>> error.log
```

To dump a mongo collection:

```bash
python3 scrape.py dump plenary_sittings
```

## License

*openatata-scapers* is licensed under the AGPLv3.
