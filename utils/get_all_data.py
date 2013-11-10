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

import itertools

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


def collect_and_serialise_referenced_entity_ids():
    print("load frames from pickle")
    frame_list = pickle.load(open('./all_frames.pickle', 'rb'))
    print("frames loaded")

    entity_id_set = set()

    print("collecting referenced entity ids")
    for frame in frame_list[u'Answers'][0][u'FrameData']:
        for role_link in frame[u'FrameData']:
            entity_id = role_link[u'Value'][u'Entity']
            entity_id_set.add(entity_id)

    print("collected entities", len(entity_id_set))
    print("serializing")

    ENT_PICKLE = "./entity_id_list.pickle"
    with open(ENT_PICKLE, "wb") as outf:
        pickle.dump(list(entity_id_set), outf, -1)
    print("serialization done")


def grouper(n, iterable):
    args = [iter(iterable)] * n
    return ([e for e in t if e != None] for t in itertools.izip_longest(*args))

def get_referenced_entities():

    api = SemanticApi(timeout = 600)

    REF_ENT_PICKLE = "./ref_entities_data.pickle"
    REF_ENT_TEXT = "./ref_entities_data.txt"

    entity_id_list = pickle.load(open('./entity_id_list.pickle', 'rb'))

    print "Retrieving entities from API"
    batch_size = 250

    batch_counter = 0

    answer = None

    for entity_id_batch in grouper(batch_size, entity_id_list):
        batch_counter = batch_counter + 1
        print("precessing batch", batch_counter)
        print("sending request")
        current_answer = api.entities_by_id(entity_id_batch)

        print("request returned")
        if not answer:
            answer = current_answer
        else:
            answer[u'Answers'].extend(current_answer[u'Answers'])

        print("saving answers so far")
        pickle_to_file(REF_ENT_PICKLE, answer)
        #log.info("Data saved to file: %s", REF_ENT_PICKLE)
        print "Pickle file [%s] saved.\n" % (REF_ENT_PICKLE,)


def main():
    start_logging(log.INFO)
    log.info("")
    log.info("Starting %s" % (sys.argv[0]))

    get_all_frames()

    # get_all_entities()

    # while there is no way to get all entities
    # we will collect referenced entity ids from frames
    # and get them in batches
    collect_and_serialise_referenced_entity_ids()
    get_referenced_entities()



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

