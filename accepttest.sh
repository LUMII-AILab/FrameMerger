#/bin/sh

./entity_maintenance.py --cleardb --database=accept_test

cat /dev/null > ./testdata/jsonlist.txt
find ./testdata/ -type f -name \*.json >> ./testdata/jsonlist.txt
find ./testdata/ -type f -name \*.json.gz >> ./testdata/jsonlist.txt

./uploadJSON.py --database=accept_test < ./testdata/jsonlist.txt

./consolidate_frames.py --database=accept_test --dirty