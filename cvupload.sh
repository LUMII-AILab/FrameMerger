#/bin/sh

find ../SemanticAnalyzer/CVFactData/CVjson/ -type f -name \*.json | sort > ./cv_jsonlist.txt

./uploadJSON.py < ./cv_jsonlist.txt

./consolidate_frames.py --dirty
