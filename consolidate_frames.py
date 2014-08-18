#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
sys.path.append("./src")

from pprint import pprint
import logging as log
import os.path
from datetime import datetime
import os, itertools

import EntityFrames as EF

from db_config import api_conn_info, instance_name, log_dir
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection

from ConsolidateFrames import BaseConsolidator
from FrameInfo import FrameInfo

from TextGenerator import get_mentioned_entities, get_frame_text

f_info = FrameInfo("input/frames-new-modified.xlsx")
processID = instance_name + ' ' + str(os.getpid())

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

            blessed_summary_frames = api.blessed_summary_frame_data_by_entity_id(entity.entity_id)

            try:
                frames = filter(lambda x: x["FrameData"] is not None, entity.frames)
                log.info("Found %s frames for entity %s", len(frames), entity.entity)

                frames = c.apply(frames, blessed_summary_frames)
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
                api.setEntityProcessingStatus([entity.entity_id for entity in entity_list], processID, 406) # nevalīdi dati
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
            # api.setEntityProcessingStatus(entity_list, processID, 406) # nevalīdi dati - trūkst entītes
            api.setEntityProcessingStatus([int(element["Value"]["Entity"])], processID, 406) # nevalīdi dati - trūkst entītes
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
        api.delete_entity_summary_frames_except_blessed(entity.entity_id, commit=False)

        # insert all entity frames [as summary frames]

        summary_frame_ids = []
        error_frames = []

        to_save = entity.cons_frames
        log.info("Save_entity_frames_to_api - summary frames to save for entity %s: %s", entity.entity_id, len(to_save))

        for frame in to_save:     
            summary_frame_id = frame.get("SummaryFrameID")
            if summary_frame_id:
                api.updateSummaryFrameRawFrames(summary_frame_id, frame["SummarizedFrames"], commit=False)
                summary_frame_ids.append(summary_frame_id)
            else:
                frame_id = api.insert_summary_frame(frame, commit=False)
                summary_frame_ids.append(frame_id)

        # commit changes (delete + insert in one transaction)
        api.api.commit()

        log.info("")

        log.info("Save_entity_frames_to_api - completed.")
        log.info(" - list of frame IDs (for %s frames saved):\n%s", len(summary_frame_ids), repr(summary_frame_ids))

        if len(error_frames)>0:
            log.info(" - %s frames could not be saved, returned errors.", len(error_frames))
            print " - %s frames could not be saved, returned errors." % (len(error_frames))

            log.debug("list of frames that could not be saved:")
            print "list of frames that could not be saved:"

            for fr in error_frames:
                log.debug("%s", repr(fr))
                print repr(fr)
            api.setEntityProcessingStatus([entity.entity_id], processID, 410) # kaut kas nepatika

        if len(error_frames)>0:
            status = "ERROR"
        else:
            api.setEntityProcessingStatus([entity.entity_id], processID, 201) # šai entītijai viss ok
            status = "OK"

        print "%s\t%s" % (entity.entity_id, status)
        sys.stdout.flush()


def process_entities(entity_list, out_dir, api):
    api.setEntityProcessingStatus(entity_list, processID, 202) # sākam apstrādi

    data = list(get_entity_frames(entity_list, api))
    api.setEntityProcessingStatus(entity_list, processID, 203) # jēlie freimi izvilkti
    gen_frames = consolidate_frames(data, api)

    frames = list(gen_frames)

    for fr in frames:
        log.info("-"*60)
        log.info("Entity name: %s\tId: %s", fr.entity["Name"], fr.entity["EntityId"])
        log.info("Frames (Total/Consolidated): %s / %s", len(fr.frames), len(fr.cons_frames))

    api.setEntityProcessingStatus(entity_list, processID, 205) # sāku saglabāt freimus
    save_entity_frames_to_api(api, frames)


def entity_ids_from_stdin():
    """
    Generator. Returns Entity_IDs (int) read from stdin.
    """
    for e_line in sys.stdin:
        if len(e_line.strip()) > 0:
            e_id = int(e_line)
            yield e_id

# NB! tas nozīmētu ka pirmo entītiju nesāks procesēt, kamēr nebūs padots pilns čunks vai arī EOF.
def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))

def main():
    start_logging(log.DEBUG) #log.INFO)
    log.info("Starting %s", sys.argv[0])

    out_dir = "./output"
    log.info("Output directory: %s\n", out_dir)

    conn = PostgresConnection(api_conn_info)
    api = SemanticApiPostgres(conn)
    
    entity_list = entity_ids_from_stdin()

    if '-single' in sys.argv:
        # reālā laika apstrāde - pēc katra ID uzreiz apstrādāt
        while 1:
            try:
                line = sys.stdin.readline()
            except KeyboardInterrupt:
                break
            if not line or line == "\n":
                break
            process_entities([int(line)], out_dir, api=api)
    else: # batch processing - dalam visu porcijās
        for chunk in split_seq(entity_list, 30): # TODO - čunka izmērs 30 var nebūt optimāls, cits cipars varbūt dod labāku ātrdarbību
            process_entities(chunk, out_dir, api=api)

    log.info('Darbs pabeigts.')

def start_logging(log_level = log.ERROR):
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

    log.getLogger("SemanticApiPostgres").level = log.INFO

# ---------------------------------------- 

if __name__ == "__main__":
    main()

