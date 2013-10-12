#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
sys.path.append("./src")

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

from SemanticApi import SemanticApi
from ConsolidateFrames import Consolidator, BaseConsolidator

ENT_Bondars = 10 #274319
ENT_Ziedonis = 274418
ENT_Ziedonis2 = 250423
ENT_Lembergs = 260970

def get_entity_frames(e_id_list):
    
    api = SemanticApi()

    try:
        for e_id in e_id_list:
            yield EF.EntityFrames(api, e_id)

    except Exception:
        log.exception("Error getting entity frames from the API:")
        raise
    
def consolidate_frames(entity_list):
    
    c = BaseConsolidator()
    #c = Consolidator()

    for entity in entity_list:
        if entity.entity is not None and entity.frames is not None:

            try:
                frames = filter(lambda x: x["FrameData"] is not None, entity.frames)
                log.info("Found %s frames for entity %s", len(frames), entity.entity)

                frames = c.apply(frames)
                log.info("Finished consolidating frames. Result frame count: %s\n", len(frames))
            except TypeError:
                log_data = "\n".join([repr(log_item) for log_item in entity.frames])
                log.exception("Error consolidating frames:\n%s", log_data)
                raise

            entity.set_consolidated_frames(frames)

            yield entity

def save_entity_frames_to_api(api, entity_list):

    for entity in entity_list:

        # insert all entity frames [as summary frames]

        summary_frame_ids = []
        error_frames = []

        to_save = entity.cons_frames
        log.info("Save_entity_frames_to_api - summary frames to save for entity %s: %s", entity.entity_id, len(to_save))

        ## XXX: trying inserting 1 frame for starters
        #log.warning("Trying inserting 1 frame FOR NOW (!!!).")
        #to_save = to_save[:1]

        for frame in to_save:
            log.debug("Saving summary frame data to API: calling api.insert_summary_frame:\n%s", repr(frame))
            res = api.insert_summary_frame(frame)

            log.debug("... return value from saving summary frame:\n%s", repr(res))

            is_ok = res["Answers"][0]

            if is_ok["Answer"] == 0:
                frame_id = is_ok["FrameId"]
                log.debug("Frame saved OK with id: %s\n", frame_id)

                summary_frame_ids.append(frame_id)

            else:
                log.debug("Error saving frame to the API: (%s, %s)\n", is_ok["Answer"], is_ok["AnswerTypeString"])
                error_frames.append(frame)

        #for frame in data:
        #    if frame["FrameData"] is not None:
        #        out.writerow(format_frame_for_output(frame))
        log.info("")

        log.info("Save_entity_frames_to_api - completed.")
        print "\nSave_entity_frames_to_api - completed."

        log.info(" - list of frame IDs (for %s frames saved):\n%s", len(summary_frame_ids), repr(summary_frame_ids))
        print " - list of frame IDs (for %s frames saved):\n%s\n" % (len(summary_frame_ids), repr(summary_frame_ids))

        if len(error_frames)>0:
            log.info(" - %s frames could not be saved, returned errors.", len(error_frames))
            print " - %s frames could not be saved, returned errors." % (len(error_frames))

            log.debug("list of frames that could not be saved:")
            print "list of frames that could not be saved:"

            for fr in error_frames:
                log.debug("%s", repr(fr))
                print repr(fr)

        print


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

        print "Frames for e_id = [%s] written to: %s" % (entity.entity_id, fname)

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

        print "-"*80
        pprint((entity.entity["Name"], entity.entity["EntityId"]))
        print

        print "Total frames:", len(entity.frames)
        print "Consolidated frames:", len(entity.cons_frames)
        print

#        print "> Consolidated frames: <"
#        print
#        pprint(entity.cons_frames)
#        print
#
#        print "> Original frames: <"
#        print
#        pprint(entity.frames)
        
    

def main():
    start_logging(log.DEBUG) #log.INFO)
    log.info("Starting %s" % (sys.argv[0]))

    out_dir = "./output"
    log.info("Output directory: %s\n", out_dir)

    #entity_list = [ENT_Bondars, ENT_Lembergs, ENT_Ziedonis2] #, ENT_Ziedonis]
    #entity_list = [ENT_Ziedonis]
    entity_list = [42]
    # entity_list = [75362]
    # ENT_Bondars]
    #entity_list = [ENT_Lembergs]

    data = list(get_entity_frames(entity_list))
    gen_frames = consolidate_frames(data)

    frames = list(gen_frames)

    print_entity_frames(frames)

    if len(entity_list) == 1:       
        # in case of 1 entity in the list
        # name the dump file according to the entity ID

        fname = "output/%s.pickle" % (entity_list[0],)
        log.debug("Dumping resulting frame list to: %s", fname)

        with open(fname, "wb") as outf:
            # frames = a list of [ ... EF.EntityFrames(api, e_id) ... ]
            pickle.dump(frames, outf)   # consolidated frames

            # reading the pickle file:

            # import sys
            # sys.path.append("src")
            # import cPickle as pickle
            # import EntityFrames as EF
            # data = pickle.load(open(fname, "rb")) 

            # e_info = data[0]    # entity that was consolidated
            # [i for i in e_info.cons_frames if i["FrameId"] == 1264893]

    save_entity_frames(out_dir, frames)

    api = SemanticApi()     # TODO: refactor, change to all SemanticApi() just once (!)
    save_entity_frames_to_api(api, frames)

def start_logging(log_level = log.ERROR):
    log_dir = "log"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    filename = "demo_consolidate_frames-%s.log" % (datetime.now().strftime("%Y_%m_%d-%H_%M"))
    filename = os.path.join(log_dir, filename)

    log.basicConfig(
        filename = filename,
        level = log_level,
        datefmt= "%Y-%m-%d %H:%M:%S",
        format = "%(asctime)s: %(name)s: %(levelname)s: %(message)s",
    )

    log.getLogger("requests.packages.urllib3").level = log.ERROR
    log.getLogger("SemanticApi").level = log.INFO

# ---------------------------------------- 

if __name__ == "__main__":
    main()

