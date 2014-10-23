#/bin/sh

cat /dev/null > ./testdata/jsonlist.txt
find ./testdata/ -type f -name \*.json.gz >> ./testdata/jsonlist.txt
find ./testdata/ -type f -name \*.json >> ./testdata/jsonlist.txt

./uploadJSON.py --database=accept_test < ./testdata/jsonlist.txt
