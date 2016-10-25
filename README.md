*openpatata-scrapers* scrapes the Cypriot parliament website.  It collects and
structures information on MPs, plenary sittings, bills and regulations, and
written questions, placing it in a database, a copy of which is maintained over
at [*openpatata-data*](http://github.com/openpatata/openpatata-data).

The proximate goal of this project is to allow for the multiform presentation
and analysis of parliament data.  The ultimate goal is for the Cypriot public
to gain a better understanding of the activities of their elected
representatives.

*openpatata-scrapers* is written in Python (3.5+).  It requires antiword, icu4c,
libmagic, MongoDB (2.6+), pandoc, pdftotext (Poppler) and a \*nix environment.

## Usage instructions

### Getting set up

Install the external dependencies listed above using your package manager of
choice.  Afterwards:

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
python3 -m scrapers data load
```

### Grabbing the data

To scrape the parliament website:

```bash
python3 -m scrapers tasks run plenary_sittings:plenary_agendas 2> error.log
python3 -m scrapers tasks run plenary_sittings:plenary_transcripts 2>> error.log
python3 -m scrapers tasks run questions:questions 2>> error.log
```

To (over)write the data on disk:

```bash
python3 -m scrapers data unload --location=data plenary_sittings
```

Run `python3 -m scrapers -h` to view all available options.

## Contributing

Additions and improvements are welcome.  You can start by peering at the issues
here on GitHub.

## License

*openpatata-scrapers* is licensed under the AGPLv3.
