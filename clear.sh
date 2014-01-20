#/bin/sh

rm -rf ./.git
rm .gitignore
rm -rf ./output
rm -rf ./log
rm -rf ./utils
rm lastlog
rm db_config.py
rm src/*.pyc
find ./testdata/zipped -maxdepth 1 -name '*.gz' -delete
rm *.log
rm testoutput.txt
rm package_list
rm *.pyc
rm src/RdfOutput.py
rm src/scrap_test.py
rm src/README-RDF_generation