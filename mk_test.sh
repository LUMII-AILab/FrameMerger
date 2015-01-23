#!/bin/bash

mk_folder="../MKtest/SemanticAnalyzer/"
# Izanalizējam ievaddatus ar norādīto Melnās Kastes versiju
rm testdata_mk_parsed/*
${mk_folder}/pipe/pipe.py testdata_mk testdata_mk_parsed
find ./testdata_mk_parsed/ -type f -name \*.json >> ./testdata_mk_parsed/jsonlist.txt
find ./testdata_mk_parsed/ -type f -name \*.json.gz >> ./testdata_mk_parsed/jsonlist.txt

# Ielādējam šos MK datus accept_test datubāzē
./entity_maintenance.py --cleardb --database=accept_test
./uploadJSON.py --database=accept_test < ./testdata_mk_parsed/jsonlist.txt
./consolidate_frames.py --database=accept_test --dirty

# Vērtējam šos datus
./evaluate_mk.py --database=accept_test testdata_mk_parsed > mktestdati.$(date +"%Y%m%d")