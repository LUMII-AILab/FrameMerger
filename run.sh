#/bin/sh

cat /dev/null > ./testdata/jsonlist.txt
for f in ./testdata/zipped/*.gz
do
	STEM=$(basename "${f}" .gz)
	gunzip -c "${f}" > ./testdata/"${STEM}"
	echo "./testdata/${STEM}" >> ./testdata/jsonlist.txt
done

./uploadJSON.py < ./testdata/jsonlist.txt
