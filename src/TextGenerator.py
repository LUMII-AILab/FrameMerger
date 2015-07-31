#!/usr/bin/env python
# -*- coding: utf8 -*-
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

# enable logging, but default to null logger (no output)
from __future__ import unicode_literals

import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import json, re
from pprint import pprint

import EntityFrames as EF
from FrameInfo import FrameInfo
import Relationships

import sys
if sys.version_info >= (3, 0, 0):
    basestring = str # python3 compatibility...

import os
f_info = FrameInfo(os.path.join(os.path.dirname(__file__), "../input/frames-new-modified.xlsx"))
#f_info = FrameInfo("input/frames-new-modified.xlsx")

# pašpietiekama funkcija, kas verbalizē izolētu *summary* freimu, paņemot visu vajadzīgo no DB
def verbalizeframe(api, frameID):
    frame = api.summary_frame_by_id(frameID)
    if not frame:
        return None # Nav freima, nav rezultāta

    mentioned_entities = set()
    for element in frame["FrameData"]:
        mentioned_entities.add( element["Value"]["Entity"])
    entity_data = fetch_all_entities (mentioned_entities, api)

    return get_frame_data(entity_data, frame)[0].strip()

# Izvelkam sarakstu ar entīšu ID, kas šajā entīšu un to freimu kopā ir pieminēti
def get_mentioned_entities(entity_list, api):
    mentioned_entities = set()    
    for entity in entity_list:
        if entity and entity.frames: # .. ja nav None
            for frame in entity.frames: # FIXME - ja sauc pēc konsolidācijas, tad var ņemt cons_frames kuru ir mazāk un tas ir ātrāk
                for element in frame["FrameData"]:
                    mentioned_entities.add( element["Value"]["Entity"])
            for frame in entity.blessed_summary_frames:
                for element in frame["FrameData"]:
                    mentioned_entities.add( element.get("entityid"))

    return fetch_all_entities(mentioned_entities, api)

# Paprasam no api pilnos datus par iesaistītajām entītēm - lai būtu pieejami iepriekš uzģenerētie locījumi
def fetch_all_entities(mentioned_entities, api):
    entity_data = {}

    for e_id in mentioned_entities:

        entity = api.entity_data_by_id(e_id)

        if entity is not None:
            entity_data[entity["EntityId"]] = entity
        else:
            log.warning("Entity not found. Entity ID: %s" % (e_id,))

    return entity_data # Dict no entītiju id uz entītijas pilnajiem datiem

# Izveidot smuku aprakstu freimam, kā arī pārējo no freima entītijām atvasināto informāciju
# Atgriež 4-tuple - (verbalization, date, start_date, cvframecategory)
def get_frame_data(mentioned_entities, frame):

    def elem(role, case='Nominatīvs'):
        if not role in roles or roles[role] is None:
            return None
        try:
            return roles[role].get(case)
        except TypeError as e:
            log.exception("Exception in inflection of element: %s", str(e))
            log.error("""Error when fetching element name inflection:
                  - role: %s
                  - case: %s
                  - role data: %s
                  """, role, case, roles)
            return None

    def verbalization():
        # Fallback, kas uzskaita freima saturu ne teikuma veidā, bet tīri 'loma: entīte'
        def simpleVerbalization():
            # FIXME - te trūkst parametri - pieliec, ko vajag
            text = f_info.type_name_from_id( frame["FrameType"])
            for role in roles:
                try:
                    text = text + ' ' + role + ':' + elem(role)
                except TypeError as e:
                    log.exception("Exception in verbalisation: %s", str(e))
                    log.error("""Unicode conversion error when building verbalisation:
                      - frame: %r
                      - text: %s
                      - role: %s
                      - element: %s
                      - role data: %s
                      """, frame, text, role, elem(role), roles)
                    try:
                        text = text + ' ' + unicode(role) + ':' + unicode(elem(role))
                    except NameError:
                        text = text + ' ' + str(role) + ':' + str(elem(role))
                        # Python 3 nav unicode() funkcijas, jo visi stringi ir unicode

            return text

#----------  Verbalization ()

        # Tipiskās vispārīgās lomas
        # FIXME - šausmīga atkārtošanās sanāk...
        laiks = ''
        if elem('Laiks') is not None:
            laiks = ' ' + elem('Laiks','Lokatīvs') # ' 2002. gadā'

        vieta = ''
        if elem('Vieta') is not None:
            if elem('Vieta','Lokatīvs'):
                vieta = ' ' + elem('Vieta','Lokatīvs') # ' Ķemeros'
            else:
                print('Vietas entītija bez lokatīva')
                print(roles.get('Vieta'))
                log.error('Vietas entītija bez lokatīva %s', frame)
                vieta = ' ' + elem('Vieta') 

        statuss = ''
        if elem('Statuss') is not None:
            statuss = ' ' + elem('Statuss')  

        if frame_type == 0: # Dzimšana
            if elem('Bērns') is None:
                log.debug("Dzimšana bez bērna :( %s", frame)
                return simpleVerbalization()
            radinieki = ''    
            if elem('Radinieki') is not None:
                radinieki = ' ' + elem('Radinieki','Lokatīvs')

            return elem('Bērns') + ' piedzima' + laiks + vieta + radinieki

        if frame_type == 1: # Vecums
            if elem('Persona') is None or elem('Vecums') is None:
                log.debug("Vecums bez pilna komplekta :( %s", frame)
                return simpleVerbalization()
            return elem('Persona', 'Ģenitīvs') + ' vecums ir ' + elem('Vecums')

        if frame_type == 2: # Miršana
            if elem('Mirušais') is None:
                log.debug("Miršana bez mirēja :( %s", frame)
                return simpleVerbalization() 
            veids = ''
            if elem('Veids') is not None:
                veids = ' ' + elem('Veids') # TODO - jānočeko kādi reāli veidi parādās, jo locījumu īsti nevar saprast
            ceelonis = ''
            if elem('Cēlonis') is not None:
                ceelonis = '. Cēlonis - ' + elem('Cēlonis')

            if elem('Mirušais', 'Kategorija') == 2: # Organizācija
                return elem('Mirušais') + ' likvidēta' + laiks + vieta + veids + ceelonis

            return elem('Mirušais') + ' mira' + laiks + vieta + veids + ceelonis

        if frame_type == 3: # Attiecības
            if elem('Partneris_1') is None or elem('Attiecības') is None:
                log.debug("Attiecības bez pilna komplekta :( %s", frame)
                return simpleVerbalization()

            inversaas_relaacijas_lokaamais_partneris = 'Partneris_1' # šis ir mazliet wtf, bet tā sanāk - ja inversā relācija ir "P2 ir P1 tēvs/māte", tad dzimti jāņem no P1
            if elem('Attiecības') in ('šķīries', 'šķīrusies', 'precējies', 'precējusies'):
                inversaas_relaacijas_lokaamais_partneris = 'Partneris_2' # ... bet ja inversā relācija ir "P2 ir precējies/usies ar P1", tad dzimti jāņem no P2
            gender = 'male'
            if elem(inversaas_relaacijas_lokaamais_partneris, 'Dzimte') == 'Sieviešu':
                gender = 'female'
            inv_relation = Relationships.inverted_relations_text.get(elem('Attiecības'))

            def desc_relations(rel, p1, p2):
                if rel in ('šķīries', 'šķīrusies'):
                    text = elem(p1) + ' ir ' + rel
                    if elem(p2):
                        text += ' no ' + elem(p2, 'Ģenitīvs')
                    return laiks + ' ' + text

                if rel in ('precējies', 'precējusies'):
                    text = elem(p1) + ' ir ' + rel
                    if elem(p2):
                        text += ' ar ' + elem(p2, 'Akuzatīvs')
                    return laiks + ' ' +  text

                if elem(p2) is None:
                    return laiks + ' ' +  elem(p1) + ' ir ' + rel
                else:
                    return laiks + ' ' +  elem(p1, 'Ģenitīvs') + ' ' + rel + ' ir ' + elem(p2)

            if inv_relation and elem('Partneris_2'):
                # print('%s -> %s' % (elem('Attiecības'), inv_relation.get(gender)))
                return json.dumps( { elem('Partneris_1','ID') : desc_relations(elem('Attiecības'), 'Partneris_1', 'Partneris_2').strip(),
                                     elem('Partneris_2','ID') : desc_relations(inv_relation.get(gender), 'Partneris_2', 'Partneris_1').strip() }, ensure_ascii=False )
            else:
                return desc_relations(elem('Attiecības'), 'Partneris_1', 'Partneris_2')

        if frame_type == 4: # Vārds alternatīvais
            if elem('Vārds') is None or elem('Entītija') is None:
                log.debug("Cits nosaukums bez pilna komplekta :( %s", frame)
                return simpleVerbalization()

            if elem('Tips') is None:
                if elem('Entītija', 'Dzimte') == 'Sieviešu':
                    return elem('Entītija') + ' saukta arī par ' + elem('Vārds')
                else:
                    return elem('Entītija') + ' saukts arī par ' + elem('Vārds')
            else:
                return elem('Entītija', 'Ģenitīvs') + ' ' + elem('Tips') + ' ir ' + elem('Vārds')

        if frame_type == 5: # Dzīvesvieta
            if elem('Rezidents') is None or elem('Vieta') is None:
                log.debug("Dzīvesvieta bez minimālā komplekta :(%s", frame)
                return simpleVerbalization()

            biezhums = ''
            if elem('Biežums') is not None:
                biezhums = ' ' + elem('Biežums')

            targetword = frame.get('TargetWord')
            if targetword and targetword.lower() in ['adrese'] and elem('Vieta', 'Nelocīts'):
                # Lai nemēģina locīt tādas frāzes kā "Brīvības 68, Rīga, LV-1011"
                return laiks + ' ' + elem('Rezidents') + biezhums + ' adrese: ' + elem('Vieta', 'Nelocīts')

            if elem('Rezidents', 'Kategorija') == 2: # Organizācija
                return laiks + ' ' + elem('Rezidents') + biezhums + ' atrodas' + vieta

            return laiks + ' ' + elem('Rezidents') + biezhums + ' uzturējās' + vieta

        if frame_type == 6: # Izglītība    
            if elem('Students') is None:
                log.debug("Izglītība bez studenta :( %s", frame)
                return simpleVerbalization()

            nozare = ''    
            if elem('Nozare') is not None:
                nozare = ' nozarē/specialitātē: ' + elem('Nozare')    
            graads = ''    
            if elem('Grāds') is not None:
                graads = ' iegūstot ' + elem('Grāds', 'Akuzatīvs')    

            verbs = ' mācījās'
            iestaadeslociijums = 'Lokatīvs'
            targetword = frame.get('TargetWord')
            if targetword and targetword.lower() in ['beidzis', 'beigusi', 'beidza', 'absolvējis', 'absolvējusi', 'pabeidzis', 'pabeigusi'] :
                verbs = ' pabeidza'
                iestaadeslociijums = 'Akuzatīvs'

            iestaade = ''    
            if elem('Iestāde') is not None:
                iestaade = ' ' + (elem('Iestāde', iestaadeslociijums) or elem('Iestāde'))   # FIXME: fallback uz noklusēto locījumu

            return laiks + vieta + ' ' + elem('Students') + verbs + iestaade + graads + nozare

        if frame_type == 7: # Nodarbošanās
            if elem('Persona') is None or elem('Nodarbošanās') is None:
                log.debug("Nodarbošanās bez pašas nodarbošanās vai dalībnieka :( %s", frame)
                return simpleVerbalization()

            if elem('Persona', 'Kategorija') == 2: # Organizācija
                return laiks + ' ' + elem('Persona') + ' nozare : ' + statuss + ' ' + elem('Nodarbošanās', 'Nelocīts')

            return laiks + ' ' + elem('Persona') + ' ir' + statuss + ' ' + elem('Nodarbošanās')

        if frame_type == 8: # Izcelsme
            if elem('Persona') is None:
                log.debug("Izcelsme bez personas :( %s", frame)
                return simpleVerbalization()

            if elem('Tautība') is not None:
                return elem('Persona') + ' ir ' + elem('Tautība')

            if elem('Izcelsme') is not None:
                return elem('Persona') + ' nāk no ' + elem('Izcelsme', 'Ģenitīvs') #TODO - jānočeko cik labi iet kopā ar reālajām izcelsmēm

            log.debug("Izcelsme bez izcelsmes :( %s", frame)
            return simpleVerbalization()

        if frame_type == 9: # Amats
            if elem('Darbinieks') is None:
                log.debug("Amats bez darbinieka :( %s", frame)
                return simpleVerbalization()

            if elem('Sākums') is not None:
                laiks = laiks + ' no ' + elem('Sākums', 'Ģenitīvs') # ' 2002. gadā no janvāra'
            if elem('Beigas') is not None:
                laiks = laiks + ' līdz ' + elem('Beigas', 'Datīvs') # ' 2002. gadā no janvāra līdz maijam'
            darbavieta = ''
            if elem('Darbavieta') is not None:
                darbavieta = ' ' + (elem('Darbavieta', 'Lokatīvs') or elem('Darbavieta'))   # FIXME: fallback uz noklusēto locījumu
            if elem('Amats', 'Ģenitīvs') is not None:
                return laiks + ' ' + elem('Darbinieks') + ' bija' + statuss + ' ' + elem('Amats','Ģenitīvs') + ' amatā' + darbavieta + vieta
            else:
                return laiks + ' ' + elem('Darbinieks') + ' strādāja' + statuss + darbavieta + vieta

        if frame_type == 10: # Darba sākums
            if elem('Darbinieks') is None:
                log.debug("Darba sākums bez darbinieka :( %s", frame)
                return simpleVerbalization()

            darbavieta = ''
            if elem('Darbavieta', 'Lokatīvs') is not None:
                darbavieta = ' ' + elem('Darbavieta', 'Lokatīvs')
            veids = ''
            if elem('Veids') is not None:
                veids = ' ' + elem('Veids') # TODO - jānočeko kādi reāli veidi parādās, jo locījumu īsti nevar saprast
            amats = ''
            ieprieksh = ''
            if elem('Iepriekšējais_darbinieks') is not None:
                ieprieksh = '. Iepriekš amatā bija ' + elem('Iepriekšējais_darbinieks')

            if elem('Darba_devējs') is not None:
                if elem('Amats') is not None:
                    amats = ' ' + elem('Amats', 'Ģenitīvs') + ' amatā'
                return laiks + ' ' + elem('Darba_devējs') + veids + ' iecēla ' + elem('Darbinieks', 'Akuzatīvs') + amats + darbavieta + vieta + ieprieksh
            else:
                if elem('Amats') is not None:
                    amats = ' kļuva par ' + elem('Amats', 'Akuzatīvs')
                else:
                    amats = ' sāka strādāt'
                return laiks + ' ' + elem('Darbinieks') + veids + amats + darbavieta + vieta + ieprieksh

        if frame_type == 11: # Darba beigas
            if elem('Darbinieks') is None:
                log.debug("Darba beigas bez darbinieka :( %s", frame)
                return simpleVerbalization()

            darbavieta = ''
            if elem('Darbavieta') is not None:
                darbavieta = ' ' + elem('Darbavieta', 'Lokatīvs')
            veids = ''
            if elem('Veids') is not None:
                veids = ' ' + elem('Veids', 'Nelocīts') 
            amats = ''
            naakamais = ''
            if elem('Nākamais_darbinieks') is not None:
                naakamais = '. Turpmāk šo amatu veiks ' + elem('Nākamais_darbinieks')

            if elem('Darba_devējs') is not None:
                if elem('Amats') is not None:
                    amats = ' no ' + elem('Amats', 'Ģenitīvs') + ' amata'
                return laiks + ' ' + elem('Darba_devējs') + veids + ' atcēla ' + elem('Darbinieks', 'Akuzatīvs') + amats + darbavieta + vieta + naakamais
            else:
                if elem('Amats') is not None:
                    amats = elem('Amats', 'Ģenitīvs') + ' amatu'
                return laiks + ' ' + elem('Darbinieks') + veids + ' atstāja ' + amats + darbavieta + vieta + naakamais

        if frame_type == 12: # Dalība
            if elem('Biedrs') is None or elem('Organizācija') is None:
                log.debug("Dalība bez biedra vai organizācijas :( %s", frame)
                return simpleVerbalization()

            targetword = frame.get('TargetWord')
            if targetword and targetword.lower() in ['izstājies', 'izstājusies']:
                return laiks + ' ' + elem('Biedrs') + statuss + ' ' + targetword + ' no ' + elem('Organizācija', 'Ģenitīvs') 

            verb = ' bija '
            if targetword and targetword.lower() in ['iestājies', 'iestājusies']:
                verb = ' ' + targetword + ' '

            return laiks + ' ' + elem('Biedrs') + verb + statuss + ' ' + elem('Organizācija', 'Lokatīvs') 

        if frame_type == 13: # Vēlēšanas
            if elem('Dalībnieks') is None or elem('Vēlēšanas') is None:
                log.debug("Vēlēšanas bez dalībnieka vai vēlēšanām :( %s", frame)
                return simpleVerbalization()
            amats = ''
            if elem('Amats') is not None:
                amats = ' par ' + elem('Amats', 'Akuzatīvs')
            saraksts = ''
            if elem('Uzvarētājs') is not None: # Te mums ir hack - uzvarētāja laukā liek sarakstu
                saraksts = ' no saraksta ' + elem('Uzvarētājs')

            if not elem('Rezultāts'):
                return laiks + vieta + ' ' + elem('Dalībnieks', 'Nominatīvs') + ' piedalījās' + amats + ' ' + elem('Vēlēšanas', 'Lokatīvs') + saraksts
            elif 'evēlē' in elem('Rezultāts'):
                return laiks + vieta + ' ' + elem('Dalībnieks', 'Akuzatīvs') + ' ievēlēja' + amats + ' ' + elem('Vēlēšanas', 'Lokatīvs') + saraksts 
            elif 'andidē' in elem('Rezultāts'):
                return laiks + vieta + ' ' + elem('Dalībnieks', 'Nominatīvs') + ' kandidēja' + amats + ' ' + elem('Vēlēšanas', 'Lokatīvs') + saraksts
            else:
                return laiks + vieta + ' ' + elem('Dalībnieks', 'Nominatīvs') + ' piedalījās' + amats + ' ' + elem('Vēlēšanas', 'Lokatīvs') + saraksts + ', rezultāts: ' + elem('Rezultāts')

        if frame_type == 14: # Atbalsts
            if elem('Atbalstītājs') is None or elem('Saņēmējs') is None:
                log.debug("Atbalsts bez dalībnieka vai saņēmēja :( %s", frame)
                return simpleVerbalization()

            atbalsts = ''
            if elem('Tēma') is not None:
                # atbalsts = ', atbalsta forma - ' + elem('Tēma')
                atbalsts = ' ar ' + elem('Tēma', 'Akuzatīvs')

            laiks = ''
            if elem('Laiks'):
                laiks = elem('Laiks', 'Lokatīvs') + ' '

            return laiks +  elem('Atbalstītājs') + ' atbalstīja ' + elem('Saņēmējs', 'Akuzatīvs') + atbalsts

        if frame_type == 15: # Dibināšana
            if elem('Organizācija') is None:
                log.debug("Dibināšana bez dibinātā :( %s", frame)
                return simpleVerbalization()
            veids = ''
            if elem('Veids') is not None:
                veids = ' ' + elem('Veids') # TODO - jānočeko kādi reāli veidi parādās, jo locījumu īsti nevar saprast
            nozare = ''
            if elem('Nozare') is not None:
                nozare = ', kuras nozare ir ' + elem('Nozare')

            if elem('Dibinātājs') is not None:
                return laiks + vieta + ' ' + elem('Dibinātājs') + veids + ' dibināja ' + elem('Organizācija', 'Akuzatīvs') + nozare
            else:
                return elem('Organizācija') + ' ir dibināta ' + laiks + vieta + veids + nozare

        if frame_type == 16: # Piedalīšanās
            if elem('Notikums') is None:
                log.debug("Piedalīšanās bez notikuma :( %s", frame)
                return simpleVerbalization()
            veids = ''
            if elem('Veids') is not None:
                veids = ' ' + elem('Veids') # TODO - jānočeko kādi reāli veidi parādās, jo locījumu īsti nevar saprast

            if elem('Dalībnieks') is not None:
                org = ''
                if elem('Organizētājs') is not None:
                    org = ', kuru organizēja ' + elem('Organizētājs')
                return laiks + vieta + ' ' + elem('Dalībnieks') + veids + ' piedalījās ' + elem('Notikums', 'Lokatīvs') + org
            else:
                if elem('Organizētājs') is not None:
                    return laiks + vieta + ' ' + elem('Organizētājs') + veids + ' organizēja ' + elem('Notikums', 'Akuzatīvs')
                else:
                    return laiks + vieta + ' ' + veids + ' notika ' + elem('Notikums', 'Akuzatīvs')
                
        if frame_type == 17: # Finanses
            if elem('Organizācija') is None:
                log.debug("Finanses bez organizācijas :( %s", frame)
                return simpleVerbalization()

            avots = ''
            if elem('Avots') is not None:
                avots = ', ienākumu avots - ' + elem('Avots')

            vieniibas = ''
            if elem('Vienības') is not None:
                vieniibas = ' ' + elem('Vienības')

            #TODO - pieaugumam nav parauga īsti
            pieaugums = ''
            if elem('Pieaugums') is not None:
                pieaugums = ', izmainoties par ' + elem('Pieaugums', 'Akuzatīvs')

            if elem('Ienākumi') is not None:
                if elem('Peļņa') is not None: # ir abi divi
                    return laiks + ' ' + elem('Organizācija', 'Ģenitīvs') + ' apgrozījums bija ' + elem('Ienākumi') + ', bet peļņa - ' + elem('Peļņa') + vieniibas + pieaugums + avots 
                else: # tikai ienākumi
                    return elem('Organizācija', 'Ģenitīvs') + ' apgrozījums' + laiks + ' bija ' + elem('Ienākumi') + vieniibas + pieaugums + avots 
            else:
                if elem('Peļņa') is not None: # tikai peļņa
                    return elem('Organizācija', 'Ģenitīvs') + ' peļņa' + laiks + ' bija ' + elem('Peļņa') + vieniibas + pieaugums + avots 
                else: #hmm, ne viens ne otrs... FIXME, nezinu vispār vai te ko var darī†
                    log.debug("Finanses bez peļņas vai apgrozījuma ;( %s", frame)
                    return elem('Organizācija', 'Ģenitīvs') + ' finanses' + laiks + ' bija ' + vieniibas + pieaugums + avots 

        if frame_type == 18: # Īpašums
            if elem('Īpašnieks') is None or elem('Īpašums') is None:
                log.debug("Īpašuma freims bez īpašnieka vai paša īpašuma :( %s", frame)
                return simpleVerbalization()

            if elem('Daļa') is None:
                return laiks + ' ' + elem('Īpašnieks', 'Datīvs') + ' pieder ' + elem('Īpašums', 'Nelocīts') # Nav nominatīvs, lai tiek galā ar 'licence veiksmes spēļu organizēšanai pa tālruni'
            else:
                return laiks + ' ' + elem('Īpašnieks', 'Datīvs') + ' pieder ' + elem('Daļa', 'Nelocīts') + ' no ' + elem('Īpašums', 'Ģenitīvs')

        if frame_type == 19: # Parāds
            if elem('Parādnieks') is None and elem('Aizdevējs') is None:
                log.debug("Parādam nav ne aizdevēja ne parādnieka :( %s", frame)
                return simpleVerbalization()    

            aizdevums = ''
            if elem('Aizdevums') is not None:
                aizdevums = ' ' + elem('Aizdevums', 'Akuzatīvs')

            vieniibas = ''
            if elem('Vienības') is not None:
                vieniibas = ' ' + elem('Vienības')

            kjiila = ''
            if elem('Ķīla') is not None:
                kjiila = ' (ķīla - ' + elem('Ķīla') + ')'

            if elem('Aizdevējs') is None:
                return laiks + ' ' + elem('Parādnieks') + ' aizņēmās' + aizdevums + vieniibas + kjiila
            else:
                paraadnieks = ''
                # if elem('Ķīla') is not None:
                if elem('Parādnieks') is not None:
                    paraadnieks = ' ' + elem('Parādnieks', 'Datīvs')
                return laiks + ' ' + elem('Aizdevējs') + ' aizdeva' + paraadnieks + aizdevums + vieniibas + kjiila

        if frame_type == 20: # Tiesvedība
            if elem('Apsūdzētais') is None:
                log.debug("Tiesvedība bez apsūdzētā :( %s", frame)  #FIXME - teorētiski varētu arī būt teikums kur ir tikai prasītājs kas apsūdz kādu nekonkrētu
                return simpleVerbalization()  

            tiesa = ''
            if elem('Tiesa') is not None:
                tiesa = ' ' + elem('Tiesa', 'Lokatīvs')
            apsuudziiba = ''
            if elem('Apsūdzība') is not None:
                apsuudziiba = ', apsūdzība - ' + elem('Apsūdzība', 'Nelocīts')
            prasiitaajs = ''
            if elem('Prasītājs') is not None:
                prasiitaajs = ', prasītājs - ' + elem('Prasītājs')
            advokaats = ''
            if elem('Advokāts') is not None:
                advokaats = ', advokāts - ' + elem('Advokāts')
            tiesnesis = ''
            if elem('Tiesnesis') is not None:
                tiesnesis = ', tiesnesis - ' + elem('Tiesnesis')


            return laiks + vieta + tiesa + ' lieta pret ' + elem('Apsūdzētais', 'Akuzatīvs') + apsuudziiba + prasiitaajs + advokaats + tiesnesis

        if frame_type == 21: # Uzbrukums
            if elem('Cietušais') is None:
                log.debug("Uzbrukums bez upura :( %s", frame)  #FIXME - teorētiski varētu arī būt teikums kur ir info par pašu uzbrucēju, nesakot kam uzbruka
                return simpleVerbalization() 

            apstaaklji = ''
            if elem('Apstākļi') is not None:
                apstaaklji = elem('Apstākļi', 'Lokatīvs') + ' '

            veids = ''
            if elem('Veids') is not None:
                veids = ' Veids - ' + elem('Veids', 'Nelocīts') + '.'
            iemesls = ''
            if elem('Iemesls') is not None:
                iemesls = ' Iemesls - ' + elem('Iemesls', 'Nelocīts') + '.'
            sekas = ''
            if elem('Sekas') is not None:
                sekas = ' Sekas - ' + elem('Sekas', 'Nelocīts') + '.'
            ierocis = ''
            if elem('Ierocis') is not None:
                ierocis = ' ar ' + elem('Ierocis', 'Akuzatīvs')

            if elem('Uzbrucējs') is not None:
                # ir gan uzbrucējs, gan upuris: LAIKS VIETA APSTĀKĻI UZBRUCĒJS VEIDS uzbruka UPURIS (Dat.sg.) ar IEROCIS (Acc.sg.). Iemesls - IEMESLS. Sekas - SEKAS.
                return laiks + vieta + ' ' + apstaaklji + ' ' + elem('Uzbrucējs') + ' uzbruka ' + elem('Cietušais', 'Datīvs') + ierocis + '.' + iemesls + sekas + veids
            else:
                # ir tikai upuris: LAIKS VIETA APSTĀKĻI notika uzbrukums UPURIS (Dat.sg.) ar IEROCIS (Acc.sg.). Iemesls - IEMESLS. Sekas - SEKAS.
                return laiks + vieta + apstaaklji + ' notika uzbrukums ' + elem('Cietušais', 'Datīvs') + ierocis + iemesls + '.' + sekas + veids

        if frame_type == 22: # Sasniegums
            core_verb = ' ieguva '

            sacensiibas = ''
            org = ''
            org2 = ''
            if elem('Sacensības') is not None:
                sacensiibas = ' ' + elem('Sacensības', 'Lokatīvs')
                if elem('Organizētājs') is not None:
                    org = ', kuru organizēja ' + elem('Organizētājs') + ','
            else:
                if elem('Organizētājs') is not None:
                    org2 = elem('Organizētājs', 'Ģenitīvs') + ' '

            daliibnieks = ''
            if elem('Dalībnieks') is not None:
                daliibnieks = ' ' + elem('Dalībnieks')
            sasniegums = ''
            if elem('Sasniegums') is not None:
                sasniegums = elem('Sasniegums', 'Akuzatīvs')
            rangs = ''
            if elem('Rangs') is not None:
                if 'viet' not in elem('Rangs'):
                    core_verb = ' bija '
                    rangs = elem('Rangs')
                else:
                    targetword = frame.get('TargetWord')
                    if targetword and targetword.lower() in ['ierindots', 'ierindota']:
                        core_verb = ' ' + targetword + ' '
                        rangs = elem('Rangs', 'Lokatīvs')
                    else:
                        rangs = elem('Rangs', 'Akuzatīvs')

            rezultaats = ''
            if elem('Rezultāts') is not None:
                rezultaats = '. Rezultāts : ' + elem('Rezultāts', 'Nelocīts')
            citi = ''
            if elem('Pretinieks') is not None:
                citi = '. Citi pretendenti: ' + elem('Pretinieks')

            if elem('Sasniegums') is None and elem('Rangs') is None:
                targetword = frame.get('TargetWord')
                if targetword and targetword.lower() in ['iekļauts', 'iekļauta', 'iekļāvis', 'iekļāvusi', 'ierindots', 'ierindota', 'minēts', 'minēta', 'minējis', 'minējusi']:
                    if org.endswith(','):
                        org = ', kuru organizēja ' + elem('Organizētājs')
                    if targetword and targetword.lower() in ['minējis', 'minējusi']:
                        if elem('Dalībnieks', 'Dzimte') == 'Sieviešu':
                            targetword = 'minēta'    
                        else:
                            targetword = 'minēts'

                    return laiks + vieta + daliibnieks + ' ' + targetword + org2 + sacensiibas + org + rezultaats + citi
                else:
                    log.debug("Sasniegums bez sasnieguma :( %s", frame)
                    return simpleVerbalization()
            else:
                return laiks + vieta + sacensiibas + org + daliibnieks + core_verb + org2 + sasniegums + rangs + rezultaats + citi

        if frame_type == 23: # Ziņošana
            if elem('Ziņa') is None:
                log.debug("Ziņošana bez ziņas :( %s", frame)
                return simpleVerbalization() 

            avots = ''
            if elem('Avots') is not None:
                avots = ' ' + elem('Avots')
            autors = ''
            if elem('Autors') is not None:
                autors = ' ' + elem('Autors')

            return laiks + avots + autors + ' informē: ' + elem('Ziņa', 'Nelocīts')

        if frame_type == 24: # Publiskais iepirkums
            if elem('Uzvarētājs') is None:
                log.debug("Iepirkums bez uzvarētāja :( %s", frame)  #FIXME - teorētiski varētu arī būt teikums kur ir info par iepirkuma sludināšanu vai pretendentu, bet to LETA vienojās nelikt
                return simpleVerbalization() 

            organizators = ''
            if elem('Iepircējs') is not None:
                organizators = ' ' + elem('Iepircējs', 'Ģenitīvs') + ' '

            teema = ''
            if elem('Tēma') is not None:
                teema = 'par ' + elem('Tēma', 'Akuzatīvs') + ' '

            summa = ''
            if elem('Paredzētā_Summa') is not None:
                summa = ' Paredzētā summa - ' + elem('Paredzētā_Summa', 'Nelocīts') + '.'
            rezultaats = ''
            if elem('Rezultāts') is not None:
                rezultaats = ' Rezultāts - ' + elem('Rezultāts', 'Nelocīts') + '.'

            # LAIKĀ (Loc.) ORGANIZĀCIJA (Gen.sg.) iepirkumā par TĒMA (Acc.sg.) uzvarēja UZVARĒTĀJS (Nom.sg.). Paredzētā summa - SUMMA.  Rezultāts - REZULTĀTS.
            return laiks + organizators + ' iepirkumā ' + teema + 'uzvarēja ' + elem('Uzvarētājs') + '.' + summa + rezultaats

        if frame_type == 25: # Zīmols
            if elem('Organizācija') is None or (elem('Zīmols') is None and elem('Produkts') is None):
                log.debug("Zīmols bez īpašnieka vai paša zīmola :( %s", frame)
                return simpleVerbalization() 

            produkts = ''
            if elem('Produkts') is not None:
                produkts = ' ' + elem('Produkts', 'Nelocīts')
            ziimols = ''
            if elem('Zīmols') is not None:
                ziimols = ' ' + elem('Zīmols')

            if elem('Produkts') is not None:
                return elem('Organizācija') + ' piedāvā:' + produkts + ziimols
            else:
                return elem('Organizācija', 'Ģenitīvs') + ' populārs zīmols:' + ziimols

        if frame_type == 26: # Nestrukturēts
            return elem('Īpašība', 'Nelocīts')

        # ja nekas nav atrasts
        # log.debug("Nemācējām apstrādāt %s", frame)
        return simpleVerbalization()

# --------  pats get_frame_data()

    frame_type = frame["FrameType"]
    roles = {}
    for element in frame["FrameData"]:
        role = f_info.elem_name_from_id(frame_type,element["Key"]-1)

        # handle incorrect frame data
        #  - when entity does not exist for the e_ID mentioned in the frame
        try:
            entity = mentioned_entities[element["Value"]["Entity"]]
        except KeyError as e:
            log.error("Entity ID %s (used in a frame element) not found! Can not generate frame text. Data:\n%r\n", element["Value"]["Entity"], frame)
            continue
        
        # try to load NameInflections
        roles[role] = None
        if entity["NameInflections"] is not None:
            try:
                nameInflections = json.loads(entity["NameInflections"])
                if isinstance(nameInflections, basestring):    
                    nameInflections = json.loads(nameInflections) # Workaround izčakarētiem datiem, kur šis dict ir lieki vēlreiz noeskeipots un ielikts datubāzē kā string
                roles[role] = nameInflections
            except Exception as e:
                log.exception('Slikti inflectioni entītijai %s: "%s"\n%s', element["Value"]["Entity"], entity["NameInflections"], str(e))
            if not isinstance(roles[role], dict):
                log.exception('Slikti inflectioni entītijai %s: "%s"', element["Value"]["Entity"], entity["NameInflections"])
                roles[role] = None

        # fallback: no inflection info available
        if not roles[role]:
            # log.debug('Entītija %s bez locījumiem', entity)  # zinam jau ka tādas ir CV datos
            roles[role] = { # Fallback, lai ir vismaz kautkādi apraksti
                'Nominatīvs': entity['Name'],
                'Ģenitīvs': entity['Name'],
                'Datīvs': entity['Name'],
                'Akuzatīvs': entity['Name'],
                'Lokatīvs': entity['Name']}

        roles[role]['Nelocīts'] = entity['Name']        
        roles[role]['Kategorija'] = entity['Category']
        roles[role]['ID'] = entity['EntityId']


    #---- datumu atrašana
    date = elem('Laiks')
    start_date = None
    if frame_type in [9]: # Amats
        if elem('Sākums'):
            start_date = elem('Sākums')
        if elem('Beigas'):
            date = elem('Beigas')
    if date and '-' in date:
        parts = date.split('-')
        start_date = parts[0]
        date = '0'.join(parts[1:])
    if not start_date:
        start_date = None #Lai nav iespējas par tukšo string vai ko tādu

    # print('%s -> %s' % (date, formatdate(date)))
    # print('%s -> %s' % (start_date, formatdate(start_date)))
    # print('haha %s' % (verbalization(), ))
    return (verbalization(), formatdate(date), formatdate(start_date), get_cv_frame_category(mentioned_entities, frame))


# Pārveido datumus no entītijas kanoniskā vārda formāta uz Didža formātu - yyyymmdd kā integer un ja nezināms mēnesis/diena, tad 0
def formatdate(date):
    if not date:
        return date

    # re.match(r'\d*$', date, re.UNICODE)
    # if m:
    #     return date

    meeneshi = {
        'janvāris'  : '01',
        'februāris' : '02',
        'marts'     : '03',
        'aprīlis'   : '04',
        'maijs'     : '05',
        'jūnijs'    : '06',
        'jūlijs'    : '07',
        'augusts'   : '08',
        'septembris': '09',
        'oktobris'  : '10',
        'novembris' : '11',
        'decembris' : '12',
        'janvārī'   : '01',
        'februārī'  : '02',
        'martā'     : '03',
        'aprīlī'    : '04',
        'maijā'     : '05',
        'jūnijā'    : '06',
        'jūlijā'    : '07',
        'augustā'   : '08',
        'septembrī' : '09',
        'oktobrī'   : '10',
        'novembrī'  : '11',
        'decembrī'  : '12'
        }

    m = re.match(r'(\d{4})\. gada (\d{1,2})\. (\w*)', date, re.UNICODE)
    if m:
        month = meeneshi.get(m.group(3), '00')
        day = m.group(2)
        if len(day)==1:
            day = '0'+day
        return m.group(1) + month + day

    m = re.match(r'(\d{4})\. gada (\w*)', date, re.UNICODE)
    if m:
        month = meeneshi.get(m.group(2), '00')
        return m.group(1) + month + '00'

    m = re.match(r'( ?\d{4})', date, re.UNICODE)
    if m:
        return m.group(1) + "0000"

    log.debug('Nesaprasts datums %s' % date)
    return None


# Atgriež LETA gribēto sašķirošanu pa CV 'kartiņas' sadaļām / kategorijām
# NB! te ir hardkodēti šo kategoriju ID, kam jāatbilst datubāzes tabulas 'cvframecategories' saturam
def get_cv_frame_category(mentioned_entities, frame):
    result = {}
    frame_type = frame.get('FrameType')
    for element in frame["FrameData"]:
        # handle incorrect frame data
        #  - when entity does not exist for the e_ID mentioned in the frame
        try:
            entity = mentioned_entities[element["Value"]["Entity"]]
        except KeyError as e:
            # Error should have been logged during frame text verbalization already
            continue
        
        if entity.get('Category') == 2: # Organization            
            if frame_type in [0,1,2,4,8]: # Dzimšana, vecums, miršana, pseidonīmi, izcelsme
                category = 101 # Pamatdati
            elif frame_type == 5: # Dzīvesvieta
                category = 102 # Kontakti
            elif frame_type == 6: # Izglītība
                category = 103 # Izglītība
            elif frame_type in [9,10,11,12]: # Amats, darba sākums, darba beigas, dalība
                category = 104 # Amatpersonas, īpašnieki, darbinieki
            elif frame_type == 7: # Nodarbošanās
                category = 105 # Nozares
            elif frame_type in [14]: # Atbalsts
                category = 106 # Ziedojumi, dāvinājumi
            elif frame_type in [20, 21]: # Tiesvedība, uzbrukums
                category = 107 # Incidenti un tiesvedības
            elif frame_type in [13]: # vēlēšanas, piedalīšanās
                category = 108 # Sabiedriskās un politiskās darbības
            elif frame_type in [25]: # Zīmols
                category = 109 # Produkti
            elif frame_type in [22]: # Sasniegums                
                category = 110 # Apbalvojumi un sasniegumi
            elif frame_type in [24]: # Publisks iepirkums
                category = 111 # Publisks iepirkums
            elif frame_type in [17, 19]: # Finanses, Parāds
                category = 112 # Finanses
            elif frame_type in [15]: # Dibināšana
                category = 101 # Pamatdati - ja nav nekas īpašs
                creator = None
                for element2 in frame["FrameData"]:
                    if element2.get('Key') == 2: # dibinātājs
                        creator = mentioned_entities[element2["Value"]["Entity"]]
                if creator and creator.get('Category') == 3: # ... ja dibināja persona
                    category = 104 # Amatpersonas, īpašnieki, darbinieki
                if creator and creator.get('Category') == 2: # ... ja dibināja organizācija
                    category = 113 # Saistītie uzņēmumi
            elif frame_type == 3: # Attiecības
                # LETAs dokumentā tur nezkāpēc kautkādas prasības, kas nesakrīt ar freimu definīciju - org<>pers attiecības "amatpersonas, īpašnieki, darbinieki" nedrīkst būt šajā freima veidā
                category = 113 # Saistītie uzņēmumi                
                for element2 in frame["FrameData"]:
                    if element2.get('Key') == 4: # attiecību veids
                        attieciibas = mentioned_entities[element2["Value"]["Entity"]]
                        if attieciibas and attieciibas.get('Name').lower() == 'struktūrvienība':
                            category = 115 # Struktūrvienības
            elif frame_type == 18: # Īpašums
                owner = None
                category = 114 # Default value - 'other'
                for element2 in frame["FrameData"]:
                    if element2.get('Key') == 1: # īpašnieks
                        owner = mentioned_entities[element2["Value"]["Entity"]]
                if owner and owner.get('Category') == 3: # ... ja pieder personai
                    category = 104 # Amatpersonas, īpašnieki, darbinieki
                if owner and owner.get('Category') == 2: # ... ja pieder organizācijai
                    category = 113 # Saistītie uzņēmumi                        
            elif frame_type == 26: # Nestrukturēta info
                category = 114 # Default value - 'other'
                for element2 in frame["FrameData"]:
                    if element2.get('Key') == 3: # kategorija
                        category = { # Kategoriju teksti no web ui entītijām letacv2 datubāzē 2015.01.25
                            'Nodarbošanās un publiskā darbība' : 108,
                            'Pamatdati' : 101,
                            'Ģimene' : 113,
                            'Tuvinieki' : 113,
                            'Izglītība' : 103,
                            'Darbs un nodarbošanās' : 104,
                            'Sabiedriskās un politiskās darbības' : 108,
                            'Uzņēmējdarbība' : 105,
                            'Incidenti un tiesvedības' : 107,
                            'Īpašumi un finanses' : 112,
                            'Kontakti' : 102,
                            'Amatpersonas, īpašnieki, darbinieki' : 104,
                            'Nozares' : 105,
                            'Apbalvojumi, interesanti fakti un izteicieni' : 110,
                            'Apbalvojumi, sasniegumi' : 110,
                            'Ziedojumi, dāvinājumi' : 106,
                            'Publiskie iepirkumi' : 11,
                            'Produkti' : 109 ,
                            'Finanses' : 112,
                            'Saistītie uzņēmumi' : 113,
                            'Cits' : 114                        
                        }.get(mentioned_entities[element2["Value"]["Entity"]].get('Name'))
                
            else:
                category = 114 # Default value - 'other'

            try:
                result[unicode(element["Value"]["Entity"])] = category
            except NameError:
                result[str(element["Value"]["Entity"])] = category
                # Python 3 nav unicode() funkcijas, jo visi stringi ir unicode

        if entity.get('Category') == 3: # Person
            if frame_type in [0,1,2,4,5,8]: # Dzimšana, vecums, miršana, pseidonīmi, dzīvesvieta, izcelsme
                category = 1 # Pamatdati
            elif frame_type == 3: # Attiecības
                # LETAs dokumentā tur nezkāpēc kautkādas prasības, kas nesakrīt ar freimu definīciju - org<>pers attiecības "amatpersonas, īpašnieki, darbinieki" nedrīkst būt šajā freima veidā
                category = 2 # Ģimene                
            elif frame_type == 6: # Izglītība
                category = 3 # Izglītība
            elif frame_type in [7,9,10,11]: # Nodarbošanās, amats, darba sākums, darba beigas
                category = 4 # Darbs un nodarbošanās
            elif frame_type in [12,13,16]: # Dalība, vēlēšanas, piedalīšanās
                category = 5 # Sabiedriskās un politiskās darbības
            elif frame_type in [15]: # Dibināšana
                category = 6 # Uzņēmējdarbība
            elif frame_type in [22]: # Sasniegums
                category = 7 # Apbalvojumi un sasniegumi
            elif frame_type in [20, 21]: # Tiesvedība, uzbrukums
                category = 8 # Incidenti un tiesvedības
            elif frame_type == 18: # Īpašums
                category = 9 # Īpašumi un finanses                
                item = None
                for element2 in frame["FrameData"]:
                    if element2.get('Key') == 2: # pats īpašums
                        item = mentioned_entities.get(element2["Value"]["Entity"]) # FIXME: nomainīts uz get(), lai nav KeyError, ar to pietiek ?
                if item and item.get('Category') == 2: # ... ja pieder organizācija
                    category = 6 # Uzņēmējdarbība
            elif frame_type in [19]: # Parāds
                category = 9 # Īpašumi un finanses
            elif frame_type in [14]: # Atbalsts
                category = 10 # Ziedojumi, dāvinājumi
            elif frame_type in [24]: # Publisks iepirkums
                category = 11 # Publisks iepirkums
            elif frame_type == 26: # Nestrukturēta info
                category = 12 # Default value - 'other'                
                for element2 in frame["FrameData"]:
                    if element2.get('Key') == 3: # kategorija
                        category = { # Kategoriju teksti no web ui entītijām letacv2 datubāzē 2015.01.25
                            'Nodarbošanās un publiskā darbība' : 5,
                            'Pamatdati' : 1,
                            'Ģimene' : 2,
                            'Tuvinieki' : 2,
                            'Izglītība' : 3,
                            'Darbs un nodarbošanās' : 4,
                            'Sabiedriskās un politiskās darbības' : 5,
                            'Uzņēmējdarbība' : 6,
                            'Incidenti un tiesvedības' : 8,
                            'Īpašumi un finanses' : 9,
                            'Kontakti' : 12,
                            'Amatpersonas, īpašnieki, darbinieki' : 12,
                            'Nozares' : 12,
                            'Apbalvojumi, interesanti fakti un izteicieni' : 7,
                            'Apbalvojumi, sasniegumi' : 7,
                            'Ziedojumi, dāvinājumi' : 10,
                            'Publiskie iepirkumi' : 11,
                            'Produkti' : 12 ,
                            'Finanses' : 9,
                            'Saistītie uzņēmumi' : 9,
                            'Cits' : 12
                        }.get(mentioned_entities[element2["Value"]["Entity"]].get('Name'))
            else:
                category = 12 # Default value - 'other'

            try:
                result[unicode(element["Value"]["Entity"])] = category
            except NameError:
                result[str(element["Value"]["Entity"])] = category
                # Python 3 nav unicode() funkcijas, jo visi stringi ir unicode

    if result:
        return result
    else:
        return None  # If no involved entities need this, then let's have a null value

def main():
    # do nothing for now
    pass
# ---------------------------------------- 

if __name__ == "__main__":
    main()

