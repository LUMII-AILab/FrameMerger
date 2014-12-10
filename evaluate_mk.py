#!/usr/bin/env python
# coding=utf-8
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.
from __future__ import print_function

import sys, os, json, gzip, getopt
from db_config import api_conn_info

sys.path.append("./src")
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection

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

def main():
    # komandrindas apstrāde
    options, remainder = getopt.getopt(sys.argv[1:], '', ['help', 'database='])
    for opt, arg in options:
        if opt == '--help':
            print('Semantic json document evaluation')
            print('')
            print('Usage: pass the folder name as a command line argument, the files from this folder will be evaluated (default "testdata_mk_parsed"')
            print('--database=<dbname>   overrides the database name from the one set in db_config.py')
            quit()
        elif opt == '--database':
            api_conn_info["dbname"] = arg

    if remainder:
        inputdir = remainder[0]
    else:
        inputdir = 'testdata_mk_parsed'

    conn = PostgresConnection(api_conn_info)
    api = SemanticApiPostgres(conn)

    for filename in os.listdir(inputdir):
        if not os.path.isabs(filename):
            filename = os.path.join(inputdir, filename)
        basename = os.path.basename(filename)
        docid = os.path.splitext(basename)[0]
        docid = os.path.splitext(docid)[0] # .json.gz gadījumam

        document = None
        if filename.endswith('.json'):
            with open(filename) as f:
                document = json.load(f, object_hook=Dict)
        if filename.endswith('.json.gz'):
            with gzip.open(filename, 'rb') as f:
                document = json.load(f, object_hook=Dict)

        if document:
            print("Processing document %s" % docid)
            # sql = "SELECT sentenceid, frametext FROM summaryframes WHERE documentid = %s"
            sql = """
SELECT f.sentenceid, s.frametext FROM summaryframes s
JOIN summaryframedata d ON s.frameid = d.summaryframeid
JOIN frames f ON f.frameid = d.frameid
WHERE s.documentid = %s"""
            frames = api.api.query(sql, (docid,))
            print(frames)
            result = []
            for counter, sentence in enumerate(document.sentences):
                # new_sentence = Dict({'text':sentence.text, 'frames':[]})
                sentence_frames = []
                for frame in frames:
                    if frame.sentenceid == str(counter+1):
                        sentence_frames.append(frame.frametext)
                print('%s: %s\n\t' % (counter+1, sentence.text), end="")
                if sentence_frames:
                    print('\n\t'.join(sentence_frames))
                else:
                    print('Freimu nav')




# ---------------------------------------- 

if __name__ == "__main__":
    main()