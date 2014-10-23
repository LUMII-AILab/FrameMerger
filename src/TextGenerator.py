#!/usr/bin/env python
# -*- coding: utf8 -*-

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import json, re
from pprint import pprint

import EntityFrames as EF
from FrameInfo import FrameInfo

import sys
if sys.version_info >= (3, 0, 0):
    basestring = str # python3 compatibility...

f_info = FrameInfo("input/frames-new-modified.xlsx")

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

# Izveidot smuku aprakstu freimam
def get_frame_data(mentioned_entities, frame):

    def elem(role, case=u'Nominatīvs'):
        if not role in roles or roles[role] is None:
            return None
        try:
            return roles[role][case]
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
                    text = text + ' ' + unicode(role) + ':' + unicode(elem(role))

            return text

#----------  Verbalization ()

        # Tipiskās vispārīgās lomas
        # FIXME - šausmīga atkārtošanās sanāk...
        laiks = u''
        if elem(u'Laiks') is not None:
            laiks = u' ' + elem(u'Laiks',u'Lokatīvs') # ' 2002. gadā'

        vieta = u''
        if elem(u'Vieta') is not None:
            vieta = u' ' + elem(u'Vieta',u'Lokatīvs') # ' Ķemeros'

        statuss = u''
        if elem(u'Statuss') is not None:
            statuss = u' ' + elem(u'Statuss')  

        if frame_type == 0: # Dzimšana
            if elem(u'Bērns') is None:
                log.debug("Dzimšana bez bērna :( %s", frame)
                return simpleVerbalization()
            radinieki = u''    
            if elem(u'Radinieki') is not None:
                radinieki = u' ' + elem(u'Radinieki',u'Lokatīvs')
            return elem(u'Bērns') + u" piedzima" + laiks + vieta + radinieki

        if frame_type == 1: # Vecums
            if elem(u'Persona') is None or elem(u'Vecums') is None:
                log.debug("Vecums bez pilna komplekta :( %s", frame)
                return simpleVerbalization()
            return elem(u'Persona', u'Ģenitīvs') + u" vecums ir " + elem(u'Vecums')

        if frame_type == 2: # Miršana
            if elem(u'Mirušais') is None:
                log.debug("Miršana bez mirēja :( %s", frame)
                return simpleVerbalization() 
            ceelonis = u''
            if elem('Cēlonis') is not None:
                ceelonis = u'. Cēlonis - ' + elem(u'Cēlonis')
            return elem(u'Mirušais') + u" mira" + laiks + vieta + ceelonis

        if frame_type == 3: # Attiecības
            if elem(u'Partneris_1') is None or elem(u'Attiecības') is None:
                log.debug("Attiecības bez pilna komplekta :( %s", frame)
                return simpleVerbalization()

            laiks = ''
            if elem(u'Laiks'):
                laiks = ' no ' + elem(u'Laiks', u'Ģenitīvs')

            if elem(u'Partneris_2') is None:
                return elem(u'Partneris_1') + u' ir ' + elem(u'Attiecības') + laiks
            # TODO - te jāšķiro 'Jāņa sieva ir Anna' vs 'Jānis apprecējās ar Annu', ko atšķirt var tikai skatoties uz Attiecību lauku
            else:
                return elem(u'Partneris_1', u'Ģenitīvs') + u' ' + elem(u'Attiecības') + u' ir ' + elem(u'Partneris_2') + laiks

        if frame_type == 4: # Vārds alternatīvais
            if elem(u'Vārds') is None or elem(u'Entītija') is None:
                log.debug("Cits nosaukums bez pilna komplekta :( %s", frame)
                return simpleVerbalization()

            if elem(u'Tips') is None:
                return elem(u'Entītija') + u" saukts arī par " + elem(u'Vārds')
            else:
                return elem(u'Entītija', u'Ģenitīvs') + u" " + elem(u'Tips') + u" ir " + elem(u'Vārds')

        if frame_type == 5: # Dzīvesvieta
            if elem(u'Rezidents') is None or elem(u'Vieta') is None:
                log.debug("Dzīvesvieta bez minimālā komplekta :(%s", frame)
                return simpleVerbalization()

            biezhums = u''
            if elem('Biežums') is not None:
                biezhums = u' ' + elem(u'Biežums')

            return laiks + u' ' + elem(u'Rezidents') + biezhums + u' uzturējās' + vieta

        if frame_type == 6: # Izglītība    
            if elem(u'Students') is None:
                log.debug("Izglītība bez studenta :( %s", frame)
                return simpleVerbalization()

            iestaade = u''    
            if elem(u'Iestāde') is not None:
                iestaade = u' ' + elem(u'Iestāde', u'Lokatīvs')    
            nozare = u''    
            if elem(u'Nozare') is not None:
                nozare = u' ' + elem(u'Nozare', u'Lokatīvs')    
            graads = u''    
            if elem(u'Grāds') is not None:
                nozare = u' iegūstot ' + elem(u'Grāds', u'Akuzatīvs')    

            # TODO - Laura ieteica šķirot pabaigšanu/nepabeigšanu pēc grāda lauka
            return laiks + vieta + u' ' + elem(u'Students') + u' mācījās' + iestaade + nozare + graads

        if frame_type == 7: # Nodarbošanās
            if elem(u'Persona') is None or elem(u'Nodarbošanās') is None:
                log.debug("Nodarbošanās bez pašas nodarbošanās vai dalībnieka :( %s", frame)
                return simpleVerbalization()

            return laiks + u' ' + elem(u'Persona') + u" ir" + statuss + u' ' + elem(u'Nodarbošanās')

        if frame_type == 8: # Izcelsme
            if elem(u'Persona') is None:
                log.debug("Izcelsme bez personas :( %s", frame)
                return simpleVerbalization()

            if elem(u'Tautība') is not None:
                return elem(u'Persona') + u' ir ' + elem(u'Tautība')

            if elem(u'Izcelsme') is not None:
                return elem(u'Persona') + u' nāk no ' + elem(u'Izcelsme', u'Ģenitīvs') #TODO - jānočeko cik labi iet kopā ar reālajām izcelsmēm

            log.debug("Izcelsme bez izcelsmes :( %s", frame)
            return simpleVerbalization()

        if frame_type == 9: # Amats
            if elem(u'Darbinieks') is None:
                log.debug("Amats bez darbinieka :( %s", frame)
                return simpleVerbalization()

            if elem(u'Sākums') is not None:
                laiks = laiks + u' no ' + elem(u'Sākums', u'Ģenitīvs') # ' 2002. gadā no janvāra'
            if elem(u'Beigas') is not None:
                laiks = laiks + u' līdz ' + elem(u'Beigas', u'Datīvs') # ' 2002. gadā no janvāra līdz maijam'
            darbavieta = u''
            if elem(u'Darbavieta') is not None:
                darbavieta = u' ' + elem(u'Darbavieta', u'Lokatīvs')
            amats = u''
            if elem(u'Amats') is not None:
                amats = u' ' + elem(u'Amats',u'Ģenitīvs') + u' amatā'

            return laiks + u' ' + elem(u'Darbinieks') + u' bija' + statuss + amats + darbavieta + vieta

        if frame_type == 10: # Darba sākums
            if elem(u'Darbinieks') is None:
                log.debug("Darba sākums bez darbinieka :( %s", frame)
                return simpleVerbalization()

            darbavieta = u''
            if elem(u'Darbavieta') is not None:
                darbavieta = u' ' + elem(u'Darbavieta', u'Lokatīvs')
            veids = u''
            if elem(u'Veids') is not None:
                veids = u' ' + elem(u'Veids') # TODO - jānočeko kādi reāli veidi parādās, jo locījumu īsti nevar saprast
            amats = u''
            ieprieksh = u''
            if elem(u'Iepriekšējais_darbinieks') is not None:
                ieprieksh = u'. Iepriekš amatā bija ' + elem(u'Iepriekšējais_darbinieks')

            if elem(u'Darba_devējs') is not None:
                if elem(u'Amats') is not None:
                    amats = u' ' + elem(u'Amats', u'Ģenitīvs') + u' amatā'
                return laiks + u' ' + elem(u'Darba_devējs') + veids + u' iecēla ' + elem(u'Darbinieks', u'Akuzatīvs') + amats + darbavieta + vieta + ieprieksh
            else:
                if elem(u'Amats') is not None:
                    amats = u' kļuva par ' + elem(u'Amats', u'Akuzatīvs')
                else:
                    amats = u' sāka strādāt'
                return laiks + u' ' + elem(u'Darbinieks') + veids + amats + darbavieta + vieta + ieprieksh

        if frame_type == 11: # Darba beigas
            if elem(u'Darbinieks') is None:
                log.debug("Darba beigas bez darbinieka :( %s", frame)
                return simpleVerbalization()

            darbavieta = u''
            if elem(u'Darbavieta') is not None:
                darbavieta = u' ' + elem(u'Darbavieta', u'Lokatīvs')
            veids = u''
            if elem(u'Veids') is not None:
                veids = u' ' + elem(u'Veids') # TODO - jānočeko kādi reāli veidi parādās, jo locījumu īsti nevar saprast
            amats = u''
            naakamais = u''
            if elem(u'Nākamais_darbinieks') is not None:
                naakamais = u'. Turpmāk šo amatu veiks ' + elem(u'Nākamais_darbinieks')

            if elem(u'Darba_devējs') is not None:
                if elem(u'Amats') is not None:
                    amats = u' no ' + elem(u'Amats', u'Ģenitīvs') + u' amata'
                return laiks + u' ' + elem(u'Darba_devējs') + veids + u' atcēla ' + elem(u'Darbinieks', u'Akuzatīvs') + amats + darbavieta + vieta + naakamais
            else:
                if elem(u'Amats') is not None:
                    amats = elem(u'Amats', u'Ģenitīvs') + u' amatu'
                return laiks + u' ' + elem(u'Darbinieks') + veids + u' atstāja ' + amats + darbavieta + vieta + naakamais

        if frame_type == 12: # Dalība
            if elem(u'Biedrs') is None or elem(u'Organizācija') is None:
                log.debug("Dalība bez biedra vai organizācijas :( %s", frame)
                return simpleVerbalization()

            return laiks + u' ' + elem(u'Biedrs') + u' ir ' + statuss + u' ' + elem(u'Organizācija', u'Lokatīvs') 

        if frame_type == 13: # Vēlēšanas
            if elem(u'Dalībnieks') is None or elem(u'Vēlēšanas') is None:
                log.debug("Vēlēšanas bez dalībnieka vai vēlēšanām :( %s", frame)
                return simpleVerbalization()
            amats = u''
            if elem(u'Amats') is not None:
                amats = u' par ' + elem(u'Amats', u'Akuzatīvs')
            saraksts = u''
            if elem(u'Uzvarētājs') is not None: # Te mums ir hack - uzvarētāja laukā liek sarakstu
                saraksts = u' no saraksta ' + elem(u'Uzvarētājs')

            if not elem(u'Rezultāts'):
                return laiks + vieta + u' ' + elem(u'Dalībnieks', u'Nominatīvs') + u' piedalījās' + amats + u' ' + elem(u'Vēlēšanas', u'Lokatīvs') + saraksts
            elif u'evelē' in elem(u'Rezultāts'):
                return laiks + vieta + u' ' + elem(u'Dalībnieks', u'Akuzatīvs') + u' ievēlēja' + amats + u' ' + elem(u'Vēlēšanas', u'Lokatīvs') + saraksts 
            elif u'andidē' in elem(u'Rezultāts'):
                return laiks + vieta + u' ' + elem(u'Dalībnieks', u'Nominatīvs') + u' kandidēja' + amats + u' ' + elem(u'Vēlēšanas', u'Lokatīvs') + saraksts
            else:
                return laiks + vieta + u' ' + elem(u'Dalībnieks', u'Nominatīvs') + u' piedalījās' + amats + u' ' + elem(u'Vēlēšanas', u'Lokatīvs') + saraksts + u' rezultāts: ' + elem(u'Rezultāts')

        if frame_type == 14: # Atbalsts
            if elem(u'Atbalstītājs') is None or elem(u'Saņēmējs') is None:
                log.debug("Atbalsts bez dalībnieka vai saņēmēja :( %s", frame)
                return simpleVerbalization()

            atbalsts = u''
            if elem(u'Tēma') is not None:
                amats = u', atbalsta forma - ' + elem(u'Tēma')

            return atbalsts + elem(u'Atbalstītājs') + u' atbalstīja ' + elem(u'Saņēmējs', u'Akuzatīvs') + atbalsts

        if frame_type == 15: # Dibināšana
            if elem(u'Organizācija') is None:
                log.debug("Dibināšana bez dibinātā :( %s", frame)
                return simpleVerbalization()
            veids = u''
            if elem(u'Veids') is not None:
                veids = u' ' + elem(u'Veids') # TODO - jānočeko kādi reāli veidi parādās, jo locījumu īsti nevar saprast
            nozare = u''
            if elem(u'Nozare') is not None:
                nozare = u', kuras nozare ir ' + elem(u'Nozare')

            if elem(u'Dibinātājs') is not None:
                return laiks + vieta + u' ' + elem(u'Dibinātājs') + veids + u' dibināja ' + elem(u'Organizācija', u'Akuzatīvs') + nozare
            else:
                return elem(u'Organizācija') + u' ir dibināta ' + laiks + vieta + veids + nozare

        if frame_type == 16: # Piedalīšanās
            if elem(u'Notikums') is None:
                log.debug("Piedalīšanās bez notikuma :( %s", frame)
                return simpleVerbalization()
            veids = u''
            if elem(u'Veids') is not None:
                veids = u' ' + elem(u'Veids') # TODO - jānočeko kādi reāli veidi parādās, jo locījumu īsti nevar saprast

            if elem(u'Dalībnieks') is not None:
                org = u''
                if elem(u'Organizētājs') is not None:
                    org = u', kuru organizēja ' + elem(u'Organizētājs')
                return laiks + vieta + elem(u'Dalībnieks') + veids + u' piedalījās' + elem(u'Notikums', u'Akuzatīvs') + org
            else:
                if elem(u'Organizētājs') is not None:
                    return laiks + vieta + u' ' + elem(u'Organizētājs') + veids + u' organizēja ' + elem(u'Notikums', u'Akuzatīvs')
                else:
                    return laiks + vieta + u' ' + veids + u' notika ' + elem(u'Notikums', u'Akuzatīvs')
                
        if frame_type == 17: # Finanses
            if elem(u'Organizācija') is None:
                log.debug("Finanses bez organizācijas :( %s", frame)
                return simpleVerbalization()

            avots = u''
            if elem(u'Avots') is not None:
                avots = u', ienākumu avots - ' + elem(u'Avots')

            vieniibas = u''
            if elem(u'Vienības') is not None:
                vieniibas = u' ' + elem(u'Vienības')

            #TODO - pieaugumam nav parauga īsti
            pieaugums = u''
            if elem(u'Pieaugums') is not None:
                pieaugums = u', izmainoties par ' + elem(u'Pieaugums', u'Akuzatīvs')

            if elem(u'Ienākumi') is not None:
                if elem(u'Peļņa') is not None: # ir abi divi
                    return laiks + elem(u'Organizācija', u'Ģenitīvs') + u' apgrozījums bija ' + elem(u'Ienākumi') + u', bet peļņa - ' + elem(u'Peļņa') + vieniibas + pieaugums + avots 
                else: # tikai ienākumi
                    return elem(u'Organizācija', u'Ģenitīvs') + u' apgrozījums' + laiks + u' bija ' + elem(u'Ienākumi') + vieniibas + pieaugums + avots 
            else:
                if elem(u'Peļņa') is not None: # tikai peļņa
                    return elem(u'Organizācija', u'Ģenitīvs') + u' peļņa' + laiks + u' bija ' + elem(u'Peļņa') + vieniibas + pieaugums + avots 
                else: #hmm, ne viens ne otrs... FIXME, nezinu vispār vai te ko var darī†
                    log.debug("Finanses bez peļņas vai apgrozījuma ;( %s", frame)
                    return elem(u'Organizācija', u'Ģenitīvs') + u' finanses' + laiks + u' bija ' + vieniibas + pieaugums + avots 

        if frame_type == 18: # Īpašums
            if elem(u'Īpašnieks') is None or elem(u'Īpašums') is None:
                log.debug("Īpašuma freims bez īpašnieka vai paša īpašuma :( %s", frame)
                return simpleVerbalization()

            if elem(u'Daļa') is None:
                return laiks + u' ' + elem(u'Īpašnieks', u'Datīvs') + u' pieder ' + elem(u'Īpašums')
            else:
                return laiks + u' ' + elem(u'Īpašnieks', u'Datīvs') + u' pieder ' + elem(u'Daļa') + u' no ' + elem(u'Īpašums', u'Ģenitīvs')

        if frame_type == 19: # Parāds
            if elem(u'Parādnieks') is None and elem(u'Aizdevējs') is None:
                log.debug("Parādam nav ne aizdevēja ne parādnieka :( %s", frame)
                return simpleVerbalization()    

            aizdevums = u''
            if elem(u'Aizdevums') is not None:
                aizdevums = u' ' + elem(u'Aizdevums', u'Akuzatīvs')

            vieniibas = u''
            if elem(u'Vienības') is not None:
                vieniibas = u' ' + elem(u'Vienības')

            kjiila = u''
            if elem(u'Ķīla') is not None:
                kjiila = u' (ķīla - ' + elem(u'Ķīla') + u')'

            if elem(u'Aizdevējs') is None:
                return laiks + elem(u'Parādnieks') + u' aizņēmās' + aizdevums + vieniibas + kjiila
            else:
                paraadnieks = u''
                # if elem(u'Ķīla') is not None:
                if elem(u'Parādnieks') is not None:
                    paraadnieks = u' ' + elem(u'Parādnieks', u'Datīvs')
                return laiks + elem(u'Aizdevējs') + u' aizdeva' + paraadnieks + aizdevums + vieniibas + kjiila

        if frame_type == 20: # Tiesvedība
            if elem(u'Apsūdzētais') is None:
                log.debug("Tiesvedība bez apsūdzētā :( %s", frame)  #FIXME - teorētiski varētu arī būt teikums kur ir tikai prasītājs kas apsūdz kādu nekonkrētu
                return simpleVerbalization()  

            tiesa = u''
            if elem(u'Tiesa') is not None:
                tiesa = u' ' + elem(u'Tiesa', u'Lokatīvs')
            apsuudziiba = u''
            if elem(u'Apsūdzība') is not None:
                apsuudziiba = u', apsūdzība - ' + elem(u'Apsūdzība')
            advokaats = u''
            if elem(u'Advokāts') is not None:
                advokaats = u', advokāts - ' + elem(u'Advokāts')
            tiesnesis = u''
            if elem(u'Tiesnesis') is not None:
                tiesnesis = u', tiesnesis - ' + elem(u'Tiesnesis')

            return laiks + vieta + tiesa + u' lieta pret ' + elem(u'Apsūdzētais', u'Akuzatīvs') + apsuudziiba + advokaats + tiesnesis

        # 21 - uzbrukums... TODO, pagaidām nav sampļu pietiekami

        if frame_type == 22: # Sasniegums
            if elem(u'Sasniegums') is None:
                log.debug("Sasniegums bez sasnieguma :( %s", frame)
                return simpleVerbalization()

            sacensiibas = u''
            if elem(u'Sacensības') is not None:
                sacensiibas = u' ' + elem(u'Sacensības', u'Lokatīvs')
            org = u''
            if elem(u'Organizētājs') is not None:
                org = u', kuru organizēja ' + elem(u'Organizētājs') + ','
            daliibnieks = u''
            if elem(u'Dalībnieks') is not None:
                daliibnieks = u' ' + elem(u'Dalībnieks')
            rangs = u''
            if elem(u'Rangs') is not None:
                rangs = u' par ' + elem(u'Rangs')
            rezultaats = u''
            if elem(u'Rezultāts') is not None:
                rangs = u' ar rezultātu ' + elem(u'Rezultāts')
            citi = u''
            if elem(u'Pretinieks') is not None:
                citi = u'. Citi pretendenti: ' + elem(u'Pretinieks')

            return laiks + vieta + sacensiibas + org + daliibnieks + u' ieguva ' + elem(u'Sasniegums', u'Akuzatīvs') + rangs + rezultaats + citi

        if frame_type == 23: # Ziņošana
            if elem(u'Ziņa') is None:
                log.debug("Ziņošana bez ziņas :( %s", frame)
                return simpleVerbalization() 

            avots = u''
            if elem(u'Avots') is not None:
                avots = u' ' + elem(u'Avots')
            autors = u''
            if elem(u'Autors') is not None:
                autors = u' ' + elem(u'Autors')

            return laiks + avots + autors + u' informē: ' + elem(u'Ziņa')

        # 24 - Publiskais iepirkums ... TODO, pagaidām nav sampļu pietiekami

        if frame_type == 25: # Zīmols
            if elem(u'Organizācija') is None or (elem(u'Zīmols') is None and elem(u'Produkts') is None):
                log.debug("Zīmols bez īpašnieka vai paša zīmola :( %s", frame)
                return simpleVerbalization() 

            produkts = u''
            if elem(u'Produkts') is not None:
                produkts = u' ' + elem(u'Produkts')
            ziimols = u''
            if elem(u'Zīmols') is not None:
                ziimols = u' ' + elem(u'Zīmols')

            if elem(u'Produkts') is not None:
                return elem(u'Organizācija') + u' piedāvā:' + produkts + ziimols
            else:
                return elem(u'Organizācija', u'Ģenitīvs') + u' populārs zīmols:' + ziimols

        if frame_type == 26: # Nestrukturēts
            return elem(u'Īpašība', u'Nelocīts')

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
                log.exception(u'Slikti inflectioni entītijai %s: "%s"\n%s', element["Value"]["Entity"], entity["NameInflections"], str(e))
            if not isinstance(roles[role], dict):
                log.exception(u'Slikti inflectioni entītijai %s: "%s"', element["Value"]["Entity"], entity["NameInflections"])
                roles[role] = None

        # fallback: no inflection info available
        if not roles[role]:
            # log.debug('Entītija %s bez locījumiem', entity)  # zinam jau ka tādas ir CV datos
            roles[role] = { # Fallback, lai ir vismaz kautkādi apraksti
                u'Nominatīvs': entity[u'Name'],
                u'Ģenitīvs': entity[u'Name'],
                u'Datīvs': entity[u'Name'],
                u'Akuzatīvs': entity[u'Name'],
                u'Lokatīvs': entity[u'Name']}

        roles[role][u'Nelocīts'] = entity[u'Name']


    #---- datumu atrašana
    date = elem(u'Laiks')
    start_date = None
    if frame_type in [9]: # Amats
        if elem(u'Sākums'):
            start_date = elem(u'Sākums')
        if elem(u'Beigas'):
            date = elem(u'Beigas')
    if date and '-' in date:
        parts = date.split('-')
        start_date = parts[0]
        date = '0'.join(parts[1:])
    if not start_date:
        start_date = None #Lai nav iespējas par tukšo string vai ko tādu

    return (verbalization(), formatdate(date), formatdate(start_date))


# Pārveido datumus no entītijas kanoniskā vārda formāta uz Didža formātu - yyyymmdd kā integer un ja nezināms mēnesis/diena, tad 0
def formatdate(date):
    if not date:
        return date

    # re.match(r'\d*$', date, re.UNICODE)
    # if m:
    #     return date

    m = re.match(r'(\d{4})\. gada (\d{1,2})\. (\w*)', date, re.UNICODE)
    if m:
        month = {
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
        }.get(m.group(3), '00')
        day = m.group(2)
        if len(day)==1:
            day = '0'+day
        return m.group(1) + month + day

    m = re.match(r'( ?\d{4})', date, re.UNICODE)
    if m:
        return m.group(1) + "0000"

    log.debug('Nesaprasts datums %s' % date)
    return None

def main():
    # do nothing for now
    pass
# ---------------------------------------- 

if __name__ == "__main__":
    main()

