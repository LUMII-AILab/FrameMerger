#/bin/sh

cat /dev/null > ./cv_jsonlist.txt
find ../SemanticAnalyzer/CVFactData/CV_json/ -type f -name \*.json >> ./cv_jsonlist.txt

./uploadJSON.py < ./cv_jsonlist.txt
