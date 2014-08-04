#/bin/sh

rm -rf ./.git
rm .gitignore
rm -rf ./output
rm -rf ./log
rm -rf ./utils
rm -rf ./bug
rm lastlog
rm db_config.py
rm src/*.pyc
find ./testdata/ -maxdepth 1 -name '*.gz' -delete
rm package_list
rm *.pyc
rm src/RdfOutput.py
rm src/README-RDF_generation
rm smalltest.txt
rm constest.txt