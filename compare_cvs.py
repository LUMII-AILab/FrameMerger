#!/usr/bin/env python
# coding=utf-8
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

import os, json, sys

sys.path.append("./src")
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection
from db_config import api_conn_info

def main():
	new_cv_folder = '../SemanticAnalyzer/CVFactData/CVjson/'
	old_cv_folder = '../SemanticAnalyzer/CVFactData/CVjson_20141024/'
	# old_cv_folder = './testdata/'

	conn = PostgresConnection(api_conn_info)
	api = SemanticApiPostgres(conn)

	frameupdates = []
	summaryframeupdates = []

	counter = 0
	for file in os.listdir(new_cv_folder):
		if file.endswith('.json') and not file.startswith('entities'):
			if os.path.isfile(old_cv_folder + file):
				with open(old_cv_folder + file) as f:
					old_doc = json.load(f, object_hook=Dict)
				with open(new_cv_folder + file) as f:
					new_doc = json.load(f, object_hook=Dict)

				new_data = {}
				for nr, sentence in enumerate(new_doc.sentences):
					new_data[sentence.text] = nr

				for nr, sentence in enumerate(old_doc.sentences):
					new_nr = new_data.get(sentence.text)
					if new_nr is None:
						raise Exception('Pazudis teikums %s :(' % sentence.text)

					if new_nr != nr:
						# print('%d -> %d\t%s\n\t\t%s' % (nr+1, new_nr+1, sentence.text, new_doc.sentences[nr].text))
						frameids = api.api.query("SELECT f.frameid FROM frames f WHERE documentid = %s AND sentenceid = %s", (file[:-5], str(nr+1)))						
						for frameid in frameids:
							frameupdates.append((new_nr+1, frameid.frameid))

						frameids = api.api.query("SELECT f.frameid FROM summaryframes f WHERE documentid = %s AND sentenceid = %s", (file[:-5], str(nr+1)))						
						for frameid in frameids:
							summaryframeupdates.append((new_nr+1, frameid.frameid))

				counter += 1
				if counter % 1000 == 0:
					print(counter)

	for params in frameupdates:
		stuff = api.api.insert('update frames set sentenceid = %s where frameid = %s', params)

	for params in summaryframeupdates:
		stuff = api.api.insert('update summaryframes set sentenceid = %s where frameid = %s', params)

	conn.commit


# JavaScript like dictionary: d.key <=> d[key]
# Elegants risinājums:
# http://stackoverflow.com/a/14620633
class Dict(dict):
    def __init__(self, *args, **kwargs):
        super(Dict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def __getattribute__(self, key):
        try:
            return super(Dict, self).__getattribute__(key)
        except:
            return

    def __delattr__(self, name):
        if name in self:
            del self[name]


# ---------------------------------------- 

if __name__ == "__main__":
    main()