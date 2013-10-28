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
from FrameInfo import FrameInfo

from TextGenerator import get_mentioned_entities, get_frame_text

ENT_Bondars = 10 #274319
ENT_Ziedonis = 274418
ENT_Ziedonis2 = 250423
ENT_Lembergs = 260970

Test50 = [131426,131427,131428,131429,131430,131431,131432,131433,131434,131435,131436,131437,131438,131439,131440,131441,131442,131443,131444,131445,131446,131447,131448,131449,131450,131451,131452,131453,131454,131455,131456,131457,131458,131459,131460,131461,131462,131463,131464,131465,131466,131467,131468,131469,131470,131471,131472,131473,131474,131475]

f_info = FrameInfo("input/frames-new-modified.xlsx")

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

    mentioned_entities = get_mentioned_entities(entity_list) # Ielasam visu freimos pieminēto (ne tikai galveno) entītiju vārdus un locījumus

    for entity in entity_list:
        if entity.entity is not None and entity.frames is not None:

            try:
                frames = filter(lambda x: x["FrameData"] is not None, entity.frames)
                log.info("Found %s frames for entity %s", len(frames), entity.entity)

                frames = c.apply(frames)
                log.info("Finished consolidating frames. Result frame count: %s\n", len(frames))
                frames = filter(valid_frame, frames) # Izmetam tos kam arī pēc apvienošanas par maz datu
                log.info("Frames after filtering for sparsity: %s\n", len(frames))

                # Building frame descriptions
                for frame in frames:
                    frametext = get_frame_text(mentioned_entities, frame)
                    if frametext is not None:
                        frametext = frametext.strip()
                    frame["FrameText"] = frametext

            except TypeError:
                log_data = "\n".join([repr(log_item) for log_item in entity.frames])
                log.exception("Error consolidating frames:\n%s", log_data)
                raise

            entity.set_consolidated_frames(frames)

            yield entity

def invalid_frame(frame):
    return not valid_frame(frame)

def valid_frame(frame):
    if len(frame["FrameData"]) < 2:
        return False  # Ja tikai 1 elements, tad fakta reāli nav

    frame_type = frame["FrameType"]
    roles = set()
    for element in frame["FrameData"]:
        roles.add(f_info.elem_name_from_id(frame_type,element["Key"]-1))

    if frame_type == 0: # Dzimšana
        if u'Bērns' not in roles: return False
    if frame_type == 1: # Vecums
        if u'Persona' not in roles: return False
        if u'Vecums' not in roles: return False
    if frame_type == 2: # Miršana
        if u'Mirušais' not in roles: return False
    if frame_type == 3: # Attiecības
        if (u'Partneris_1' not in roles or u'Partneris_2' not in roles) and u'Partneri' not in roles: return False
        if u'Attiecības' not in roles: return False
    if frame_type == 4: # Vārds alternatīvais
        if u'Vārds' not in roles: return False
        if u'Entītija' not in roles: return False
    if frame_type == 5: # Dzīvesvieta
        if u'Rezidents' not in roles: return False  
        if u'Vieta' not in roles: return False
    if frame_type == 6: # Izglītība    
        if u'Students' not in roles: return False  
    if frame_type == 7: # Nodarbošanās
        if u'Persona' not in roles: return False  
        if u'Nodarbošanās' not in roles: return False  
    if frame_type == 8: # Izcelsme
        if u'Persona' not in roles: return False  
    if frame_type in (9,10,11): # Amats, Darba sākums, Darba Beigas
        if u'Darbinieks' not in roles: return False
    if frame_type == 12: # Dalība
        if u'Biedrs' not in roles: return False  
        if u'Organizācija' not in roles: return False  
    if frame_type == 13: # Vēlēšanas
        if u'Dalībnieks' not in roles: return False  
    if frame_type == 14: # Atbalsts
        if u'Atbalstītājs' not in roles: return False  
        if u'Saņēmējs' not in roles: return False  
    if frame_type == 15: # Dibināšana
        if u'Organizācija' not in roles: return False  
    if frame_type == 16: # Piedalīšanās
        if u'Notikums' not in roles: return False  
    if frame_type == 17: # Finanses
        if u'Organizācija' not in roles: return False  
    if frame_type == 18: # Īpašums
        if u'Īpašums' not in roles: return False  
        if u'Īpašnieks' not in roles: return False  
    if frame_type == 19: # Parāds
        if u'Parādnieks' not in roles and u'Aizdevējs' not in roles: return False  
    if frame_type == 22: # Sasniegums
        if u'Sasniegums' not in roles: return False  
    if frame_type == 23: # Ziņošana
        if u'Ziņa' not in roles: return False  

    return True 

def save_entity_frames_to_api(api, entity_list):    
    for entity in entity_list:

        # insert all entity frames [as summary frames]

        summary_frame_ids = []
        error_frames = []

        to_save = entity.cons_frames
        log.info("Save_entity_frames_to_api - summary frames to save for entity %s: %s", entity.entity_id, len(to_save))

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
        # print "\nSave_entity_frames_to_api - completed."

        log.info(" - list of frame IDs (for %s frames saved):\n%s", len(summary_frame_ids), repr(summary_frame_ids))
        # print " - list of frame IDs (for %s frames saved):\n\n%s" % (len(summary_frame_ids), repr(summary_frame_ids))

        if len(error_frames)>0:
            log.info(" - %s frames could not be saved, returned errors.", len(error_frames))
            print " - %s frames could not be saved, returned errors." % (len(error_frames))

            log.debug("list of frames that could not be saved:")
            print "list of frames that could not be saved:"

            for fr in error_frames:
                log.debug("%s", repr(fr))
                print repr(fr)


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

        print "-"*80
        pprint((entity.entity["Name"], entity.entity["EntityId"]))

        print "Frames (Total/Consolidated):", len(entity.frames), " / ", len(entity.cons_frames)
        print

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
        
    

def main():
    start_logging(log.DEBUG) #log.INFO)
    log.info("Starting %s" % (sys.argv[0]))

    out_dir = "./output"
    log.info("Output directory: %s\n", out_dir)

    #entity_list = [ENT_Bondars, ENT_Lembergs, ENT_Ziedonis2] #, ENT_Ziedonis]
    #entity_list = [ENT_Ziedonis]
    #entity_list = [10,42,120272]
    entity_list = range(131426,131475)
    # entity_list = [131426]
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

