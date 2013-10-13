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

import requests, json

import EntityFrames as EF

from SemanticApi import SemanticApi
from ConsolidateFrames import Consolidator, BaseConsolidator
from FrameInfo import FrameInfo

ENT_Bondars = 10 #274319
ENT_Ziedonis = 274418
ENT_Ziedonis2 = 250423
ENT_Lembergs = 260970

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

                # Building frame descriptions
                for frame in frames:
                    frame["FrameText"] = get_frame_text(mentioned_entities, frame)
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

# Izvelkam sarakstu ar entīšu ID, kas šajā entīšu un to freimu kopā ir pieminēti
def get_mentioned_entities(entity_list):
    mentioned_entities = set()    
    for entity in entity_list:
        for frame in entity.frames: # FIXME - ja sauc pēc konsolidācijas, tad var ņemt cons_frames kuru ir mazāk un tas ir ātrāk
            for element in frame["FrameData"]:
                mentioned_entities.add( element["Value"]["Entity"])
    answers = SemanticApi().entities_by_id( list(mentioned_entities))
    entity_data = {}
    for answer in answers["Answers"]:
        if answer["Answer"] == 0:
            entity = answer["Entity"]
            entity_data[entity["EntityId"]] = entity
        else:
            log.warning("Entity %s not found. Error code [%s], message: %s" % (e_id, data[0]["Answer"], data[0]["AnswerTypeString"]))
    return entity_data # Dict no entītiju id uz entītijas pilnajiem datiem

# Izveidot smuku aprakstu freimam
def get_frame_text(mentioned_entities, frame):
    frame_type = frame["FrameType"]
    roles = {}
    for element in frame["FrameData"]:
        role = f_info.elem_name_from_id(frame_type,element["Key"]-1)
        entity = mentioned_entities[element["Value"]["Entity"]]
        if entity["NameInflections"] == u'':
            print 'Entītija bez locījumiem', entity
        else:
            roles[role] = json.loads(entity["NameInflections"])

    def elem(role, case=u'Nominatīvs'):
        if not role in roles or roles[role] is None:
            return None
        return roles[role][case]

    # Tipiskās vispārīgās lomas
    # FIXME - šausmīga atkārtošanās sanāk...
    laiks = u''
    if not elem(u'Laiks') is None:
        laiks = u' ' + elem(u'Laiks',u'Lokatīvs') # ' 2002. gadā'

    vieta = u''
    if not elem(u'Vieta') is None:
        vieta = u' ' + elem(u'Vieta',u'Lokatīvs') # ' Ķemeros'

    amats = u''
    if not elem(u'Amats') is None:
        amats = u' par ' + elem(u'Amats',u'Akuzatīvs')

    if frame_type == 0: # Dzimšana
        radinieki = u''    
        if not elem(u'Radinieki') is None:
            darbavieta = u' ' + elem(u'Radinieki',u'Lokatīvs')
        return elem(u'Bērns') + " ir dzimis" + vieta + laiks + radinieki
        # TODO - radinieki var būt datīvā vai lokatīvā atkarībā no konteksta, jāapdomā

    if frame_type == 3: # Attiecības
        return elem(u'Partneris_1', u'Ģenitīvs') + u' ' + elem(u'Attiecības') + u' ir ' + elem(u'Partneris_2')

    if frame_type == 6: # Izglītība    
        iestaade = u''    
        if not elem(u'Iestāde') is None:
            iestaade = u' ' + elem(u'Iestāde', u'Lokatīvs')    
        return elem(u'Students') + laiks + u' ir mācījies' + iestaade

    if frame_type == 7: # Nodarbošanās
        if elem(u'Persona') is None or elem(u'Nodarbošanās') is None:
            print "Nodarbošanās bez pašas nodarbošanās vai dalībnieka :( ", frame
            return None
        return elem(u'Persona') + " ir " + elem(u'Nodarbošanās')

    if frame_type == 9: # Amats
        if not elem(u'Sākums') is None:
            laiks = laiks + u' no ' + elem(u'Sākums', u'Ģenitīvs') # ' 2002. gadā no janvāra'
        if not elem(u'Beigas') is None:
            laiks = laiks + u' līdz ' + elem(u'Beigas', u'Datīvs') # ' 2002. gadā no janvāra līdz maijam'
        darbavieta = u''
        if not elem(u'Darbavieta') is None:
            darbavieta = u' ' + elem(u'Darbavieta', u'Lokatīvs')
        persona = u''
        if not elem(u'Persona') is None:
            persona = elem(u'Persona') + u' '

        return persona + u'strādājis' + laiks + darbavieta + amats + vieta

    if frame_type == 10: # Darba sākums
        darbavieta = u''
        if not elem(u'Darbavieta') is None:
            darbavieta = u' ' + elem(u'Darbavieta', u'Lokatīvs')
        persona = u''
        if not elem(u'Persona') is None:
            persona = elem(u'Persona') + u' '

        return persona + laiks + u' kļuvis' + darbavieta + amats + vieta

    if frame_type == 13: # Vēlēšanas
        if elem(u'Dalībnieks') is None or elem(u'Vēlēšanas') is None:
            print "Vēlēšanas bez dalībnieka vai vēlēšanām :( ", frame
            return None
        return elem(u'Dalībnieks') + laiks + u' ievēlēts ' + elem(u'Vēlēšanas', u'Lokatīvs') + amats

    if frame_type == 22: # Sasniegums
        sacensiibas = u''
        if not elem(u'Sacensības') is None:
            sacensiibas = elem(u'Sacensības')
        if elem(u'Sasniegums') is None:
            print "Sasniegums bez sasnieguma :( ", frame
            return None

        return sacensiibas + laiks + u' saņēmis ' + elem(u'Sasniegums', u'Akuzatīvs')

    else:
        return None



def print_entity_frames(entity_list):
    for entity in entity_list:

        print "-"*80
        pprint((entity.entity["Name"], entity.entity["EntityId"]))
        print

        print "Total frames:", len(entity.frames)
        print "Consolidated frames:", len(entity.cons_frames)
        print

        print "> Consolidated frames: <"
        print

        for frame in entity.cons_frames:
            frametext = frame.get("FrameText")
            frame_type = frame["FrameType"]

            if not frametext is None:
                print frametext
            else: 
                print f_info.type_name_from_id(frame_type)
            # for element in frame["FrameData"]:
            #     role = f_info.elem_name_from_id(frame_type,element["Key"]-1)
            #     entity = mentioned_entities[element["Value"]["Entity"]]
            #     print role, ': ', entity["NameInflections"]
                
            print


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
    entity_list = [10,42,120272]
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

