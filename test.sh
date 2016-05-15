#!/usr/bin/env bash

if [[ -n $TRAVIS ]]
then
    svn co --non-interactive --trust-server-cert \
           https://github.com/openpatata/openpatata-data/trunk/mps \
           travis-data/mps
    python3 -m scrapers init --drop-db travis-data mps
fi

nosetests --ignore-files='^\\.' --ignore-files='^setup\\.py$' \
          --verbosity=3 --with-doctest \
          scrapers
