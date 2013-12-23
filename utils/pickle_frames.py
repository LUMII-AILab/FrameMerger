#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
sys.path.append("./src")
sys.path.append(".")

from pprint import pprint
from itertools import chain
import logging as log
import os.path
import csv
import cPickle as pickle
from datetime import datetime
import os

import EntityFrames as EF

from db_config import api_conn_info
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection


def print_entity_frames(entity_list):
    for entity in entity_list:

        log.info("-"*60)
        log.info("Entity name: %s\tId: %s", entity.entity["Name"], entity.entity["EntityId"])

        log.info("Frames (Total/Consolidated): %s / %s", len(entity.frames), len(entity.cons_frames))

        for frame in entity.cons_frames:
            frametext = frame.get("FrameText")
            frame_type = frame["FrameType"]
        

def pickle_entity_frame(e_id, out_dir, api):

    data = EF.EntitySummaryFrames(api, e_id)

    #print_entity_frames(frames)

    fname = "output/%s-new.pickle" % (e_id,)
    log.debug("Dumping resulting frame list to: %s", fname)

    with open(fname, "wb") as outf:
        # frames = a list of [ ... EF.EntityFrames(api, e_id) ... ]
        pickle.dump(data, outf)   # consolidated frames

        # reading the pickle file:

        # import sys
        # sys.path.append("src")
        # import cPickle as pickle
        # import EntityFrames as EF
        # data = pickle.load(open(fname, "rb")) 

        # e_info = data[0]    # entity that was consolidated
        # [i for i in e_info.cons_frames if i["FrameId"] == 1264893]

    #save_entity_frames(out_dir, frames)

def entity_ids_from_stdin():
    """
    Generator. Returns Entity_IDs (int) read from stdin.
    """

    for e_line in sys.stdin:
        if len(e_line.strip()) > 0:
            e_id = int(e_line)
            yield e_id

def main():
    start_logging(log.DEBUG) #log.INFO)
    log.info("Starting %s" % (sys.argv[0]))

    out_dir = "./output"
    log.info("Output directory: %s\n", out_dir)

    conn = PostgresConnection(api_conn_info)
    api = SemanticApiPostgres(conn)
    
    entity_list = entity_ids_from_stdin()

    for e_id in entity_list:
        pickle_entity_frame(e_id, out_dir, api=api)

    log.info('Darbs pabeigts.')

def start_logging(log_level = log.ERROR):
    log_dir = "log"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    filename = "pickle_frames-%s.log" % (datetime.now().strftime("%Y_%m_%d-%H_%M"))
    filename = os.path.join(log_dir, filename)

    log.basicConfig(
        filename = filename,
        level = log_level,
        datefmt= "%Y-%m-%d %H:%M:%S",
        format = "%(asctime)s: %(name)s: %(levelname)s: %(message)s",
    )

    log.getLogger("SemanticApiPostgres").level = log.INFO

# ---------------------------------------- 

if __name__ == "__main__":
    main()

