#!/usr/bin/env python
# -*- coding: utf8 -*-

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import json
from pprint import pprint

import EntityFrames as EF

from SemanticApi import SemanticApi
from FrameInfo import FrameInfo


f_info = FrameInfo("input/frames-new-modified.xlsx")

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
            log.warning("Entity not found. Error code [%s], message: %s" % (answer["Answer"], answer["AnswerTypeString"]))
    return entity_data # Dict no entītiju id uz entītijas pilnajiem datiem

# Izveidot smuku aprakstu freimam
def get_frame_text(mentioned_entities, frame):
    frame_type = frame["FrameType"]
    roles = {}
    for element in frame["FrameData"]:
        role = f_info.elem_name_from_id(frame_type,element["Key"]-1)

        # handle incorrect frame data
        #  - when entity does not exist for the e_ID mentioned in the frame
        try:
            entity = mentioned_entities[element["Value"]["Entity"]]
        except KeyError, e:
            log.error("Entity ID %s (used in a frame element) not found! Can not generate frame text. Data:\n%r\n", element["Value"]["Entity"], frame)
            print "\nERROR: Entity ID %s (in frame element) not found! Can not generate frame text. Data:\n%r\n" % (element["Value"]["Entity"], frame)
            continue
        
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
    if elem(u'Laiks') is not None:
        laiks = u' ' + elem(u'Laiks',u'Lokatīvs') # ' 2002. gadā'

    vieta = u''
    if elem(u'Vieta') is not None:
        vieta = u' ' + elem(u'Vieta',u'Lokatīvs') # ' Ķemeros'

    amats = u''
    if elem(u'Amats') is not None:
        amats = u' par ' + elem(u'Amats',u'Akuzatīvs')

    if frame_type == 0: # Dzimšana
        if elem(u'Bērns') is None:
            print "Dzimšana bez bērna :( ", frame
            return None
        radinieki = u''    
        if elem(u'Radinieki') is not None:
            radinieki = u' ' + elem(u'Radinieki',u'Lokatīvs')
        return elem(u'Bērns') + " ir dzimis" + vieta + laiks + radinieki
        # TODO - radinieki var būt datīvā vai lokatīvā atkarībā no konteksta, jāapdomā

    if frame_type == 3: # Attiecības
        if elem(u'Partneris_1') is None or elem(u'Partneris_2') is None or elem(u'Attiecības') is None:
            print "Attiecības bez pilna komplekta :( ", frame
            return None
        return elem(u'Partneris_1', u'Ģenitīvs') + u' ' + elem(u'Attiecības') + u' ir ' + elem(u'Partneris_2')

    if frame_type == 6: # Izglītība    
        if elem(u'Students') is None:
            print "Izglītība bez studenta :( ", frame
            return None
        iestaade = u''    
        if elem(u'Iestāde') is not None:
            iestaade = u' ' + elem(u'Iestāde', u'Lokatīvs')    
        return elem(u'Students') + laiks + u' ir mācījies' + iestaade

    if frame_type == 7: # Nodarbošanās
        if elem(u'Persona') is None or elem(u'Nodarbošanās') is None:
            print "Nodarbošanās bez pašas nodarbošanās vai dalībnieka :( ", frame
            return None
        return elem(u'Persona') + " ir " + elem(u'Nodarbošanās')

    if frame_type == 9: # Amats
        if elem(u'Sākums') is not None:
            laiks = laiks + u' no ' + elem(u'Sākums', u'Ģenitīvs') # ' 2002. gadā no janvāra'
        if elem(u'Beigas') is not None:
            laiks = laiks + u' līdz ' + elem(u'Beigas', u'Datīvs') # ' 2002. gadā no janvāra līdz maijam'
        darbavieta = u''
        if elem(u'Darbavieta') is not None:
            darbavieta = u' ' + elem(u'Darbavieta', u'Lokatīvs')
        persona = u''
        if elem(u'Persona') is not None:
            persona = elem(u'Persona') + u' '

        return persona + u'strādājis' + laiks + darbavieta + amats + vieta

    if frame_type == 10: # Darba sākums
        darbavieta = u''
        if elem(u'Darbavieta') is not None:
            darbavieta = u' ' + elem(u'Darbavieta', u'Lokatīvs')
        persona = u''
        if elem(u'Persona') is not None:
            persona = elem(u'Persona') + u' '

        return persona + laiks + u' kļuvis' + darbavieta + amats + vieta

    if frame_type == 13: # Vēlēšanas
        if elem(u'Dalībnieks') is None or elem(u'Vēlēšanas') is None:
            print "Vēlēšanas bez dalībnieka vai vēlēšanām :( ", frame
            return None
        return elem(u'Dalībnieks') + laiks + u' ievēlēts ' + elem(u'Vēlēšanas', u'Lokatīvs') + amats

    if frame_type == 22: # Sasniegums
        sacensiibas = u''
        if elem(u'Sacensības') is not None:
            sacensiibas = elem(u'Sacensības')
        if elem(u'Sasniegums') is None:
            print "Sasniegums bez sasnieguma :( ", frame
            return None

        return sacensiibas + laiks + u' saņēmis ' + elem(u'Sasniegums', u'Akuzatīvs')

    else:
        return None



def main():
    # do nothing for now
    pass
# ---------------------------------------- 

if __name__ == "__main__":
    main()

