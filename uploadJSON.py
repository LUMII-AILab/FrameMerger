#!/usr/bin/env python3
# coding=utf-8
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

import sys, os, glob, fnmatch, json, codecs, traceback, gzip
from datetime import date, datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
from DocumentUpload import upload2db
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection
from db_config import api_conn_info, instance_name, log_dir
import logging as log
import getopt

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

def start_logging(log_level = log.ERROR):
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    filename = "uploadJSON-%s.log" % (datetime.now().strftime("%Y_%m_%d-%H_%M"))
    filename = os.path.join(log_dir, filename)

    #log.basicConfig(
    #    level = log_level,
    #    datefmt= "%Y-%m-%d %H:%M:%S",
    #    format = "%(asctime)s: %(name)s: %(levelname)s: %(message)s",
    #)
    # Windows + Python 3: failam, kurā tiek log-ots, vajag norādīt encoding.
    log.getLogger().setLevel(log_level)
    handler = log.FileHandler(filename, encoding='utf-8')
    handler.setFormatter(log.Formatter(datefmt= "%Y-%m-%d %H:%M:%S", fmt = "%(asctime)s: %(name)s: %(levelname)s: %(message)s"))
    log.getLogger().addHandler(handler)
	
    log.getLogger("SemanticApiPostgres").level = log.INFO

def main():
    # komandrindas apstrāde
    options, remainder = getopt.getopt(sys.argv[1:], '', ['help', 'database='])
    for opt, arg in options:
        if opt == '--help':
            print('JSON document upload to semantic DB')
            print('')
            print('Usage: pass filenames to be processed through stdin, one filename per line')
            print('--database=<dbname>   overrides the database name from the one set in db_config.py')
            quit()
        elif opt == '--database':
            api_conn_info["dbname"] = arg

    start_logging(log.INFO)
    log.info("Starting %s", sys.argv[0])

    sys.stderr.write( 'Starting...\n')
    sys.stderr.write( 'Pass filenames to be processed through stdin, one filename per line\n')

    processID = instance_name + ' ' + str(os.getpid())
    basedir = os.path.dirname(os.path.realpath(__file__))

    conn = PostgresConnection(api_conn_info)
    api = SemanticApiPostgres(conn)

    try:
        for filename in sys.stdin:
            filename = filename.strip()
            if filename.endswith('.DS_Store'): # ar Mac menedžējot testadatus, vislaik ievazājas :(
                continue
            if filename.endswith('.json') and filename.startswith('entities_'): # CV pārveidošanas ģenerētie saraksti ar "core" entītijām
                log.info('Skipping file %s' % filename)
                continue                

            # Tiek kropļots īstais ceļš uz failu:
            # if not os.path.isabs(filename):
            #     filename = os.path.join(basedir, filename)
            basename = os.path.basename(filename)
            docid = os.path.splitext(basename)[0]
            if docid.endswith('.json'):
                docid = os.path.splitext(docid)[0] # .json.gz gadījumam
            
            log.info("Looking at filename %s", filename)
            #api.reprocessDoc([docid]) # temporary for testing - uztaisam ierakstu ka šo dokumentu vispār vajag apstrādāt
            api.setDocProcessingStatus(docid, processID, 202)

            try:
                document = None
                if filename.endswith('.json'):
                    with open(filename, encoding='utf-8') as f:
                        document = json.load(f, object_hook=Dict)
                if filename.endswith('.json.gz'):
                    with gzip.open(filename, 'rt', encoding='utf-8') as f:
                        document = json.load(f, object_hook=Dict)

                if document:
                    log.info("Processing document %s", docid)
                    document.id = docid
                    document.date = datetime.strptime(document.date.split(' ')[0], '%Y-%m-%d').date() # atpakaļ no serializētā stringa

                    upload2db(document, api)
                    sys.stdout.write(filename + "\tOK\n") # Feedback par veiksmīgi apstrādātajiem dokumentiem
                    api.setDocProcessingStatus(docid, processID, 201)
                else:
                    api.setDocProcessingStatus(docid, processID, 404)
                    sys.stdout.write(filename + "\tNot processed, unknown file type\n")
            except Exception as e:
                sys.stderr.write('Problem on file: '+filename+' ... \n')
                log.error('Problem on file: '+filename+' ... \n')
                traceback.print_exc()
                print(filename, '\tFail:\t', e)
                api.setDocProcessingStatus(docid, processID, 406)

        log.info("Done.")
        sys.stderr.flush()

    except KeyboardInterrupt:
        print('Interrupted!')
        log.info("Interrupted by user")

# ---------------------------------------- 

if __name__ == "__main__":
    main()
