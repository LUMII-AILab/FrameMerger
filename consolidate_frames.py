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

from db_config import api_conn_info
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection

from ConsolidateFrames import Consolidator, BaseConsolidator
from FrameInfo import FrameInfo

from TextGenerator import get_mentioned_entities, get_frame_text

f_info = FrameInfo("input/frames-new-modified.xlsx")

def get_entity_frames(e_id_list, api):

    try:
        for e_id in e_id_list:
            # FIXME - replace w. EntityFrames for Postgres API
            #  - remove the need for self.api in EntityFrames (at least when pickling)
            yield EF.EntityFrames(api, e_id)

    except Exception:
        log.exception("Error getting entity frames from the API:")
        raise
    
def consolidate_frames(entity_list, api):
    
    c = BaseConsolidator()
    #c = Consolidator()

    mentioned_entities = get_mentioned_entities(entity_list, api) # Ielasam visu freimos pieminēto (ne tikai galveno) entītiju vārdus un locījumus

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

                    try:
                        frametext = get_frame_text(mentioned_entities, frame)
                    except KeyError, e:
                        log.exception("Key error in get_frame_text:\n%s", e)
                        frametext = None

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
        try:
            roles.add(f_info.elem_name_from_id(frame_type,element["Key"]-1))
        except KeyError, e:
            log.error("Entity ID %s (used in a frame element) not found! Location: valid_frame() - Data:\n%r\n", element["Value"]["Entity"], frame)
            print "Entity ID %s (used in a frame element) not found! Location: valid_frame() - Data:\n%r\n" % (element["Value"]["Entity"], frame)
            continue

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
    if frame_type == 25: # Zīmols
        if u'Organizācija' not in roles: return False  

    return True 

def save_entity_frames_to_api(api, entity_list):    
    for entity in entity_list:

        # delete previous summary frames
        api.delete_entity_summary_frames(entity.entity_id)

        # XXX, FIXME:
        #  - the deletion logic must be changed to preserve blessed frames
        #    (if blessed frames are saved as summary frames)

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

        if len(error_frames)>0:
            status = "ERROR"
        else:
            status = "OK"

        print "%s\t%s" % (entity.entity_id, status)
        sys.stdout.flush()

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
        

def process_entities(entity_list, out_dir, api):
    data = list(get_entity_frames(entity_list, api))
    gen_frames = consolidate_frames(data, api)

    frames = list(gen_frames)

    print_entity_frames(frames)

    if False:       # skip saving to pickle file (connection info can't be pickled) - FIXME if needed for debug reasons
        """
Traceback (most recent call last):
  File "./demo_consolidate_frames.py", line 393, in <module>
    main()
  File "./demo_consolidate_frames.py", line 368, in main
    process_entities(chunk, out_dir)
  File "./demo_consolidate_frames.py", line 328, in process_entities
    pickle.dump(frames, outf)   # consolidated frames
  File "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/copy_reg.py", line 70, in _reduce_ex
    raise TypeError, "can't pickle %s objects" % base.__name__
TypeError: can't pickle connection objects
"""
        #if len(entity_list) == 1:       
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

    #save_entity_frames(out_dir, frames)

    save_entity_frames_to_api(api, frames)

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
        process_entities((e_id,), out_dir, api=api)

    log.info('Darbs pabeigts.')

def start_logging(log_level = log.ERROR):
    log_dir = "log"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    filename = "consolidate_frames-%s.log" % (datetime.now().strftime("%Y_%m_%d-%H_%M"))
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

