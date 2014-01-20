#/bin/sh

cat /dev/null > ./testdata/jsonlist.txt
find ./testdata/ -type f -name \*.json.gz >> ./testdata/jsonlist.txt
find ./testdata/ -type f -name \*.json >> ./testdata/jsonlist.txt

./uploadJSON.py < ./testdata/jsonlist.txt
