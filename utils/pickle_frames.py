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

import requests

import EntityFrames as EF

from db_config import api_conn_info
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection

from ConsolidateFrames import Consolidator, BaseConsolidator
from FrameInfo import FrameInfo

from TextGenerator import get_mentioned_entities, get_frame_text


def save_entity_frames(out_dir, entity_list):

    for entity in entity_list:

        fname = os.path.join(out_dir, str(entity.entity_id) + ".csv")
        log.info("Saving frame data to file: %s", fname)

        with open(fname, "wb") as outf:

            out = csv.writer(outf)

            out.writerow((entity.entity["Name"].encode("utf-8"), entity.entity["EntityId"]))
            out.writerow([])

            out.writerow(("Total frames:", len(entity.frames)))
            out.writerow(("Consolidated frames:", len(entity.cons_frames)))
            out.writerow([])

            out.writerow(("> Consolidated frames: <",))
            out.writerow([])

            # output consolidated frames

            out.writerow(format_header_for_output())
            data = sorted(entity.cons_frames, key = lambda x: x["FrameType"])

            for frame in data:
                if frame["FrameData"] is not None:
                    out.writerow(format_frame_for_output(frame))

            out.writerow([])

            out.writerow(("> Original frames: <",))
            out.writerow([])

            # output original frames

            out.writerow(format_header_for_output())
            data = sorted(entity.frames, key = lambda x: x["FrameType"])
            for frame in data:
                if frame["FrameData"] is not None:
                    out.writerow(format_frame_for_output(frame))

            out.writerow(("-"*80,))

        # print "Frames for e_id = [%s] written to: %s" % (entity.entity_id, fname)

def format_header_for_output():
    return (
        "Status",
        "Type",
        "Count",
        "SourceId",
#       // FrameId -> not using for now, need to taker FrameIdList into account
    )

def format_frame_for_output(frame):
    buf = [
        frame.get("FrameStatus", ""),

        frame["FrameType"],
        frame.get("FrameCnt", ""),
        frame["SourceId"],
#       // FrameId -> not using for now, need to taker FrameIdList into account
    ]

    buf.extend(chain((item["Key"], item["Value"]["Entity"]) for item in frame["FrameData"])) 

    return buf


def print_entity_frames(entity_list):
    for entity in entity_list:

        log.info("-"*60)
        log.info("Entity name: %s\tId: %s", entity.entity["Name"], entity.entity["EntityId"])

        log.info("Frames (Total/Consolidated): %s / %s", len(entity.frames), len(entity.cons_frames))

        for frame in entity.cons_frames:
            frametext = frame.get("FrameText")
            frame_type = frame["FrameType"]

            # if not frametext is None:
            #     print frametext.encode("utf8")
            #     None
            # else: 
            #     print "None - ", f_info.type_name_from_id(frame_type).encode("utf8"), frame                

#        print "> Original frames: <"
#        print
#        pprint(entity.frames)
        

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

    log.getLogger("requests.packages.urllib3").level = log.ERROR
    log.getLogger("SemanticApiPostgres").level = log.INFO

# ---------------------------------------- 

if __name__ == "__main__":
    main()

