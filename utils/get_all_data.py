#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
sys.path.append("./src")

from collections import Counter
from pprint import pprint
import logging as log
import cPickle as pickle
import time

from SemanticApi import SemanticApi, EntityFrames, EntityDb


def get_all_frames():

    api = SemanticApi(timeout = 600)

    FR_PICKLE = "./all_frames.pickle"
    FR_TEXT = "./all_frames.txt"

    print "Retrieving frames from API"
    frames = api.get_frames([])

    pickle_to_file(FR_PICKLE, frames)
    #log.info("Data saved to file: %s", FR_PICKLE) 
    print "Pickle file [%s] saved.\n" % (FR_PICKLE,)

    print "Saving text version to [%s]" % (FR_TEXT,)
    with open(FR_TEXT, "wb") as outf:
        pprint(frames, stream=outf)
    print "Text file [%s] saved.\n" % (FR_TEXT,)

def get_all_entities():

    api = SemanticApi(timeout = 600)

    ENT_PICKLE = "./all_entities.pickle"
    ENT_TEXT = "./all_entities.txt"

    print "Retrieving entities from API"
    entities = api.entities_by_id([])

    pickle_to_file(ENT_PICKLE, entities)
    #log.info("Data saved to file: %s", ENT_PICKLE) 
    print "Pickle file [%s] saved.\n" % (ENT_PICKLE,)

    print "Saving text version to [%s]" % (ENT_TEXT,)
    with open(ENT_TEXT, "wb") as outf:
        pprint(entities, stream=outf)
    print "Text file [%s] saved.\n" % (ENT_TEXT,)

def main():
    start_logging(log.INFO)
    log.info("")
    log.info("Starting %s" % (sys.argv[0]))

    get_all_entities()
    get_all_frames()

def pickle_to_file(fname, obj):
    with open(fname, "wb") as outf:
        pickle.dump(obj, outf, -1)

def start_logging(log_level = log.ERROR):
    log.basicConfig(
        filename = "get_all_data.log",
        level = log_level,
        datefmt= "%Y-%m-%d %H:%M:%S",
        format = "%(asctime)s: %(name)s: %(levelname)s: %(message)s",
    )

    log.getLogger("requests.packages.urllib3").level = log.INFO

# ---------------------------------------- 

if __name__ == "__main__":
    main()

