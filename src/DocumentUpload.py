#!/usr/bin/env python
# coding=utf-8
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

from __future__ import print_function
from __future__ import unicode_literals

import json, time, datetime, re, copy, uuid, codecs, pylru
try:    
    from urllib.request import quote # For Python 3.0 and later
except ImportError:    
    from urllib import quote # Fall back to Python 2's urllib

from collections import Counter
from frameinfo2 import getFrameCode, getFrameName, \
                       getElementCode, getElementName, \
                       getEntityTypeCode, getEntityTypeName, \
                       getDefaultEnityType, getPlausibleTypes, \
                       getDeterminerElement
from Relationships import isRelationshipName
from InflectEntity import inflectEntity
#from FrameInfo import FrameInfo
#f_info = FrameInfo("input/frames-new-modified.xlsx")
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection
from db_config import api_conn_info
import CDC
import logging as log

realUpload = True # Vai lādēt DB pa īstam - lai testu laikā nečakarē DB datus
showInserts = False # Vai rādīt uz console to, ko mēģina insertot DB
showDisambiguation = False # Vai rādīt uz console entītiju disambiguācijas debug
entityCreationDebuginfo = False # Vai rādīt uz console potenciālās jaunradītās entītijas

api = None

def connect():
    global api
    conn = PostgresConnection(api_conn_info)
    api = SemanticApiPostgres(conn) # TODO - šo te uz uploadJSON.py

# Ielādē teikumos minētos faktus faktu postgres db; pirms tam atrodot entītiju globālos id vai izveidojot entītijas, uz kurām atsaucas atrastie freimi
def upload2db(document, api=api): # document -> dict ar pilniem dokumenta+ner+freimu datiem, kāds nāk no Didža Framer vai arī saglabātā JSON formāta
    if not api:
        log.error('upload2db - nav datubāzes pieslēgums')
        sys.stderr.write('upload2db - nav datubāzes pieslēgums')                

    if realUpload: 
        api.cleanupDB(document.id) # iztīram šī dokumenta ID agrāk ielasītos neblesotos raw-frames
        blessed_sentences = set(api.doc_blessed_frame_sentences(document.id)) # paskatamies, kas šim dokumenta ID ir blesots bijis

    sentences = document.sentences
    # Vai mums vispār vajag šitās entītes? Varbūt tās, kam makeEntityIfNeeded
    # nav uzsetojis savu source, pēc tam atfiltrēt?
    entities = document.namedEntities
    for e in entities:
        entities[e]['source'] = 'initial NER/coref'

    neededEntities = set() # Savācam entītijas, kuras dokumentā piemin kāds freims
    for sentenceID, sentence in enumerate(sentences):
        for frame in sentence.frames:
            addDate(frame, document.date) # Fills the Time attribute for selected frames from document metadata, if it is empty
            frame['sentence'] = sentence # backpointer tālākai apstrādei
            
            # Noskaidro vai, ir viena loma, kas nosaka citu elementu tipus.
            detElement = getDeterminerElement(getFrameCode(frame.type))
            detType = None
            doneElem = None
            # Ja šāda loma ir, tad tās entīti uztaisa pirmo, lai būtu zināms tās tips.
            if detElement is not None:
                for element in frame.elements:
                    if element.name == detElement:
                        entityid, detType = makeEntityIfNeeded(entities, sentence.tokens, element.tokenIndex, frame, element, None)
                        neededEntities.add(entityid)
                        doneElem = element
                        break
                        
            # Apstrādā pārējās lomas.
            for element in frame.elements:
                if element is not doneElem:     # Lai neapstrādā determinējošo lomu vēlreiz.
                    entityid, entityType = makeEntityIfNeeded(entities, sentence.tokens, element.tokenIndex, frame, element, detType)
                        # te  modificē NE sarakstu, pieliekot freima elementus ja tie tur jau nav
                        # te pie esošajām entītijām pieliek norādi uz freimiem, kuros entītija piedalās - TODO: pēc tam novākt, lai nečakarē json seivošanu
                    neededEntities.add(entityid)
            
        for token in sentence.tokens: # Pie reizes arī savācam entītiju pieminējumu vietas, kuras insertot datubāzē
            neIDs = token.get('namedEntityID')
            if neIDs is not None: # ja šis tokens satur norādes uz entītijām
                for neID in neIDs:
                    ne = entities[str(neID)]
                    locations = ne.get('locations')
                    if not locations:
                        locations = []
                        
                    locations.append([sentenceID+1, token.get('index')])
                    ne['locations'] = locations
                                
    for entity in entities.values():
        if ((entity['id'] not in neededEntities) and (entity.get('type') == 'person') or (entity.get('type') == 'organization')):
            neededEntities.add(entity['id']) # personas un organizācijas insertojam vienmēr, lai piefiksētu tās, kas dokumentā ir pieminētas bet nav freimos
            entity['notReallyNeeded'] = True  # UI noslēpsim tos šādi pieliktos, kam nav neviena freima; manuprāt jāfiltrē tikai pēc visu dok. importa bet 2014.06.05 seminārā lēma šādi. TODO - review.

    # Entītiju nosaukumu filtrs - aizvietojam relatīvos laikus ('vakar'); likvidējam nekonkrētos aliasus ('viņš').
    filterEntityNames(entities, document.date)
                    
    #Katrai entītijai piekārtojam globālo ID
    fetchGlobalIDs(entities, neededEntities, sentences, document.id, api) # TODO - varbūt jau sākumā pie tās pašas sentenču apstaigāšanas arī jāsaveido entītiju contextbag 
    
    # Tagad vēlreiz apskatam visus freimus, un insertojam tos DB
    requests = []
    for sentenceID, sentence in enumerate(sentences):
        for frame in sentence.frames:
            if len(frame.elements) < 2:
                continue # mikrofreimus neliekam. TODO: te varētu vēl pat agresīvāk filtrēt
            frameType = getFrameCode(frame.type)

            elements = {} # dict no elementu lomas koda uz attiecīgās entītes ID
            filledRoles = set() 
            usedEntities = set()
            for element in frame.elements:

                elementCode = getElementCode(frameType, element.name)
                if element.entityID:
                    entityID = element.entityID
                    token_str = entities[str(entityID)]['representative'] 
                elif element.localEntityID:
                    entityID = element.localEntityID
                    token_str = entities[str(entityID)]['representative'] 
                else:
                    entityID = sentence.tokens[element.tokenIndex-1].namedEntityID[0]  # entītija kas atbilst freima elementam - iepriekšējā ciklā 100% visām jābūt korekti saformētām
                    token_str = sentence.tokens[element.tokenIndex-1].form
                globalID = entities[str(entityID)].get('GlobalID')
                
                if globalID is None and realUpload:
                    log.error('Neatradu globalID entītijai %s', entities[str(entityID)].get('representative'))                
                if elementCode in filledRoles: # Freimu analizators nedrīkstētu iedot 2x vienādas lomas, bet ja nu tomēr, tad lai te nenomirst
                    log.debug('Lomai %s vairāki varianti: %s ir par daudz', element.name, token_str )
                elif globalID in usedEntities:                     
                    log.debug('Entītija #%d atkal parādās tai pašā freimā pie vārda %s', globalID, token_str )
                    # principā šāds varētu rasties ja koreferences saliek 2 NER-atrastas entītijas kopā, un vienā freimā pieliek pie katru savas lomas (piemēram, amats+personvārds?) - bet īsti labi tas nav
                elif globalID: # nekonkrētos elementus šajā posmā nometam
                    elements[elementCode] = globalID
                    filledRoles.add(elementCode)
                    usedEntities.add(globalID)
                    
            if showInserts:
                print('Gribētu insertot freimu', frame.type, elements)
            if realUpload:             
                if frame.tokenIndex:
                    targetword = sentence.tokens[frame.tokenIndex-1].form
                elif frame.targetWord:
                    targetword = frame.targetWord
                else:
                    targetword = None
                source = 'Pipeline parse at '+datetime.datetime.now().isoformat()

                approvalType = 0 # default
                if document.get('type', '').lower() in {'person', 'organization'}: # Personas CV vai organizāciju profilu dokumenti - uzskatam to datus par ticamākiem nekā citus
                    approvalType = 1
                    source = 'LETA CV'
                    if frame.get('ruleID'):
                        source = source + ' ' + frame.get('ruleID')
                    if (sentenceID+1) in blessed_sentences:
                        continue # Ja šajā CV teikumā jau cilvēks ir blesojis faktus, tad to neliekam DB

                api.insertFrame(frameType, elements, document.id, source, sentenceID+1, targetword, document.date.isoformat(), approvedTypeID=approvalType)

    if realUpload: 
        for entity in entities.values():
            hidden = entity.get('hidden')
            if hidden == None:
                hidden = hideEntity(entity['representative'], entity.get('type'))
            if hidden == False and ((entity.get('type') == 'person') or (entity.get('type') == 'organization')):
                api.dirtyEntity(entity.get('GlobalID'))
        api.insertDocument(document.id, document.date.isoformat(), document.get('type'), compact_document(document))
        api.api.commit

    # Iztīram krossreferences, lai document objektu var smuki noseivot kā JSON
    for sentence in sentences:
        for frame in sentence.frames:
            frame.pop('sentence', None)
    for entity in entities.values():
        entity.pop('frames', None)

# Uztaisa entītiju ar atbilstošo tekstu un ieliek to entities sarakstā
def makeEntity(entities, phrase, namedEntityType):
    # tagad skatamies - ja tāda frāze mums jau kautkur bija, tad atsaucamies uz esošo entīti (no ner nevajadzētu būt, bet palīdzēs lai šis kods neģenerē dublikātus)
    # ja ne, tad atrodam lielāko idu
    matchingEntity = None
    entityID = 1
    for entity in entities.values(): # atrodam max_id +1
        if entity['representative'] == phrase and \
                'type' in entity and entity['type'] == namedEntityType:
            matchingEntity = entity
        if int(entity['id']) >= entityID:
            entityID = int(entity['id']) + 1

    if matchingEntity is None:            
        inflections = json.loads(inflectEntity(phrase, entity.get('type')))
        if entity.get('type') not in ['descriptor','person','organization']:  
            phrase = inflections.get('Nominatīvs')

        entity = {
            'id': entityID,
            'type': namedEntityType,  # TODO - te varētu nočekot vai NER iedotais tips iederas šajā lomā
            'representative': phrase, # Te varētu arī nebūt pamatforma; bet to risinās vēlāk - lai freimmarķiera atrastās entītijas apstrādā identiski ar NER atrastajām
            'source': 'framemarker',  # Lai pēc tam debugUI var atšķirt no LVTagger/LVCoref veidotajām entītēm
            'aliases': [phrase],
            'inflections': inflections
        }
        entities[str(entityID)] = entity
    else:
        entityID = matchingEntity['id']
    return entityID

def entityPhraseByTree(tokenIndex, tokens, frameName, roleName, entityType):
    """
    Apstaigājam atrastajam galvasvārdam pakļautos vārdus un izveidojam entītes "tekstu".
    """
    included = {tokenIndex}
    dirty = True # Vai vajag vēl dziļāk apstaigāt
    terminator = False # Vai esam atraduši ko tādu, ka tok dziļāk nelienam

    while dirty and not terminator: 
        dirty = False
        newincluded = copy.copy(included)
        for token in tokens:
            if (token.parentIndex in included) and not (token.index in included): # pievienojam visus bērnus kamēr vairs nav ko pievienot
                #TODO - vajag pārbaudi lai neiekļautu apakšteikumus
                if token.lemma in ('(', ')', ';'):
                    terminator = True
                dirty = True
                newincluded.add(token.index) 
        included = newincluded # šāda maisīšana tādēļ, lai terminators nogriež visu dziļumā x, nevis aiziet pa ciklu līdz teikuma galam

    phrase = []
    included = sorted(included)
    for token in tokens:
        if token.index in included:
            phrase.append(token)
            
    # Atmet pirmos tokenus, ja tie ir komati, punkti, domuzīmes vai saikļi
    firstToken = 0
    for token in phrase:
        if token.tag in ('zc', 'zd', 'zs') or token.tag[0] == 'c' :
            firstToken += 1
        else:
            break
    # Atmet pēdējos tokenus, ja tie ir komati, punkti, domuzīmes vai saikļi
    lastToken = len(phrase)
    for token in reversed(phrase):
        if token.tag in ('zc', 'zd', 'zs') or token.pos == 'c':
            lastToken -= 1
        else:
            break
    phrase = phrase[firstToken:lastToken]
    
    # Ja grāda entīte satur vārdu "grāds", atmet visu, kas ir pēc vārda "grāds".
    if entityType == 'qualification':
        degreeIndex = 0
        for token in phrase:
            degreeIndex += 1
            if token.lemma.lower() in {'grāds', 'izglītība'}:
                break
        if degreeIndex is not None:
            phrase = phrase[:degreeIndex]
        
    phrase = " ".join(map(lambda t: t.form, phrase))
    if phrase.endswith(' un'):
        phrase = phrase[:-3] # noņemam un
    if phrase.startswith(', '):
        phrase = phrase[2:] # 'ziņojuma' entītijas mēdz sākties ar ", ka tas un tas"

    if phrase.startswith('(') and phrase.endswith(')'):
        phrase = phrase[1:-1] # noņemam iekavas

    return phrase

# Apstaigājam atrastajam galvasvārdam blakus esošos vārdus ar vienādu NER kategoriju
#def entityPhraseByNER(tokenIndex, tokens):    
#    tips = tokens[tokenIndex-1].namedEntityType
#
#    low = tokenIndex-1
#    while low > 0 and tokens[low-1].namedEntityType == tips:
#        low -= 1
#    high = tokenIndex-1
#    while high < len(tokens)-1 and tokens[high+1].namedEntityType == tips:
#        high += 1
#
#    phrase = []
#    for token in tokens[low:high+1]:
#        phrase.append(token.form)
#    phrase = " ".join(phrase)
#    return phrase

def entityPhraseByNERMention (start, end, tokens):
    """
    Izvelk frāzi, kas ir konkrētais pieminējums, nevis NER reprezentatīvais vārds
    NER/coref numurē tokenus sākot ar 1, python masīva elementus - sākot ar 0
    """
    phrase = []
    # Riskants risinājums, vajadzētu patiesībā ņemt pēc token.index
    for token in tokens[start-1:end]:
        phrase.append(token.form)
    return " ".join(phrase)

def makeEntityIfNeeded(entities, tokens, tokenIndex, frame, element, determinerElementType):
    """
    Noformē entītijas vārdu, ja ir dots teikums + entītes galvenā vārda (head) id,
    ja tādas entītijas nav, tad pievieno jaunu sarakstam
    """
    # Ja ir jau datos norāde uz entītijas ID (piemēram, kā CV importā), tad to arī atgriežam
    if element.entityID:
        entities[str(element.entityID)]['source'] = 'defined in document'
        return (element.entityID, entities[str(element.entityID)]['type'])

    if tokenIndex > len(tokens):
        log.error('Error: entity sākas tokenā #%d no %d datos : %s',tokenIndex,len(tokens), repr(tokens))
        return (0, None)
    else: 
        headtoken = tokens[tokenIndex-1]    # Tokens, ap kuru visa ņemšanās notiek.
        frameCode = getFrameCode(frame.type)
        elementCode = getElementCode(frameCode, element.name)
        permitedTypes = getPlausibleTypes(frameCode, elementCode, determinerElementType)
        
        # Paņemam NER/NEL/coref entīti un paskatamies, vai der.
        entityID = None
        representativePhrase = None
        mentionPhrase = None
        entityType = None
        
        # Dažas lomas vienmēr ir jāaizpilda ar koka fragmentu, dažas var arī ar
        # NER/NEL/coref entītēm vai to pieminējumiem.
        if (frame.type, element.name) not in [('Statement', 'Message')]:
        
            # Mēģinās aizpildīt ar NER/NEL/coref palīdzību
            if 'mentions' in headtoken.keys():
                entityID = headtoken['mentions'][0]['id']
                #representativePhrase = entities[str(entityID)].get('representative')
                representativePhrase = entities[str(entityID)]['representative']
                mentionPhrase = entityPhraseByNERMention(
                    headtoken['mentions'][0]['start'], headtoken['mentions'][0]['end'], tokens)

                # Reizēm coref mentioniem nav tipu, ja NER tur neko jēdzīgu nav atradis.
                mentionType = 'unk'
                if 'type' in headtoken['mentions'][0]:
                    mentionType = headtoken['mentions'][0]['type']
                # Pabrīdina, ja ir vairāk par vienu mention, jo tas šobrīd
                # netiek ņemts vērā.
                if len(headtoken['mentions']) > 1 and entityCreationDebuginfo:
                    print('Tokenam {0} ir {1} pieminējumi.\n'.format(
                        headtoken['form'], len(headtoken['mentions'])))
               
                # Atradās entīte ar pieļaujamu tipu + ir tāda loma, kurā to varam lietot, viss forši
                if 'type' in entities[str(entityID)] and entities[str(entityID)]['type'] in permitedTypes:
                    entities[str(entityID)]['source'] = 'phrase extraction, from NER/coref'
                    entityType = entities[str(entityID)]['type']
                    
                # Atradās mentions ar pieļaujamu tipu, varam uztaisīt no tā jaunu entīti
                elif mentionType in permitedTypes:
                    entityType = mentionType
                    entityID = makeEntity(entities, mentionPhrase, entityType)
                    entities[str(entityID)]['source'] = 'phrase extraction, from NER/coref mention'
                
                # Tipi nav piemēroti, nomainīs uz defaulto
                else:
                    entityType = getDefaultEnityType(frameCode, elementCode, determinerElementType)
                    #Freimi, kuriem ņem entītes reprezentatīvo frāzi.
                    if (frame.type, element.name) not in [('People_by_vocation', 'Vocation')]:
                        entityID = makeEntity(entities, representativePhrase, entityType)
                        entities[str(entityID)]['source'] = 'phrase extraction, from NER/coref with changed type'
                    #Freimi, kuriem ņem entītes pieminējumu
                    else:
                        entityID = makeEntity(entities, mentionPhrase, entityType)
                        entities[str(entityID)]['source'] = 'phrase extraction, from NER/coref mention with changed type'
                    
            if entityID is None and headtoken.pos == 'p': # vietniekvārds, kas nav ne ar ko savilkts
                entityType = getDefaultEnityType(frameCode, elementCode, determinerElementType)
                entityID = makeEntity(entities, '_NEKONKRĒTS_', entityType)
                entities[str(entityID)]['source'] = 'phrase extraction, unspecified entity'

            # Pamēģinam paskatīties parent - reizēm freimtageris norāda uz vārdu bet NER ir entītijas galvu ielicis uzvārdam.
            #if entityID is None or (headtoken.form == 'Rīgas' and headtoken.namedEntityType == 'organization'):   # Tas OR ir hacks priekš Rīgas Tehniskās universitātes kuru nosauc par Rīgu..
                # Pa lielam, ja 'atrastais' vārds ir pareiza tipa entītijas iekšpusē tad ieliekam nevis tagera atrasto bet visu lielo entīti
            #    parent = tokens[headtoken.parentIndex-1]
            #    if headtoken.namedEntityType == parent.namedEntityType:
            #        entityType = parent.namedEntityType
            #        entityID = parent.namedEntityID
            #        if entityID:
            #            entities[str(entityID)]['source'] = 'workaround #2 - entity from syntactic parent'

        # Ja nu toč nav tādas entītijas (vai arī tas ir Ziņojums)... tad veidojam no koka
        if entityID is None: 
            entityType = getDefaultEnityType(frameCode, elementCode, determinerElementType)
            phrase = entityPhraseByTree(tokenIndex, tokens, frame.type, element.name, entityType)
            if (phrase is not None and phrase != ''):
                entityID = makeEntity(entities, phrase, entityType)
                entities[str(entityID)]['source'] = 'phrase extraction, entity built from syntactic tree'
                if entityCreationDebuginfo:
                    print('No koka uztaisīja freima {3} elementu vārdā {2} ar tipu {1} un saturu:\t{0}'.format(
                        phrase, entityType, element.name, frame.type))
            else:
                entityID = makeEntity(entities, headtoken.form, entityType)
                entities[str(entityID)]['source'] = 'phrase extraction, entity built from single token'
                if entityCreationDebuginfo:
                    print('No tokena (jo no koka neizdevās) uztaisīja freima {3} elementu vārdā {2} ar tipu {1} un saturu:\t{0}'.format(
                        phrase, entityType, element.name, frame.type))
            
    if 'namedEntityID' not in headtoken:
        headtoken['namedEntityID'] = []
    headtoken['namedEntityID'].append(entityID)
    element['localEntityID'] = entityID
    frames = entities[str(entityID)].get("frames")
    if frames is None:
        frames = []
    frames.append(frame)
    entities[str(entityID)]["frames"] = frames
    return (entityID, entityType)

# Fills the Time attribute for selected frames from document metadata, if it is empty
def addDate(frame, documentdate): 
    return # FIXME - jāsaprot kuriem vajag

# Saņem entītiju sarakstu un dokumenta radīšanas datumu; satīra nosaukumus - aizvietojam relatīvos laikus ('vakar'); likvidējam nekonkrētos ('viņš').
def filterEntityNames(entities, documentdate):
    def updateName(name):
        if name.lower() in {'šodien', 'patlaban', 'tagad', 'pašlaik', 'šonedēļ'}:
            return documentdate.isoformat().split(' ')[0]
        if name.lower() == 'vakar':
            return (documentdate - datetime.timedelta(days=1)).isoformat().split(' ')[0]
        if name.lower() == 'rīt':
            return (documentdate + datetime.timedelta(days=1)).isoformat().split(' ')[0]
        if name.lower() == 'šogad':
            return str(documentdate.year)
        if name.lower() in {'pagājušgad', 'pērn', 'pagājušajā gads', 'pagājušajā gadā'}:
            return str(documentdate.year - 1)
        # TODO - šie ir biežākie, bet vēl vajadzētu relatīvos laikus: 'jūlijā' -> pēdējais jūlijs, kas ir bijis (šis vai iepriekšējais gads) utml.
        return name
    
    def goodName(type, name):
        """
        Vārdu saraksti, kas atfiltrē tipiskos gadījumus, kad entītes vārds
        liecina par nekonkrētu vai aplamu entīti.
        """
        if len(name) <= 2 and not re.match(r'[A-ZĀČĒĢĪĶĻŅŠŪŽ]+|\d+$', name, re.UNICODE): # tik īsi drīkst būt tikai cipari vai organizāciju (partiju) saīsinājumi
            return False
        if name.lower() in {'viņš', 'viņs', 'viņa', 'viņam', 'viņu', 'viņā', 'viņas', 'viņai', 'viņās', 'viņi', 'viņiem', 'viņām',
                            'es', 'mēs', 'man', 'mūs', 'mums', 'tu', 'tev', 'jūs', 'jums', 'jūsu',
                            'tas', 'tā', 'tie', 'tās', 'tajā', 'tam', 'tām', 'to', 'tos', 'tai', 'tiem', 'tur', 'kur',
                            'kas', 'kam', 'ko', 
                            'sava', 'savu', 'savas', 'savus', 
                            'kurš', 'kuru', 'kura', 'kuram', 'kuri', 'kuras', 'kurai', 'kuriem', 'kurām', 'kurā', 'kurās',
                            'būs', 'arī', 'jau', 'kā arī', 'kā_arī'}: 
            return False
        if name in {'Var', 'gan'}:
            return False
        if type not in {'qualification', 'descriptor'} and name.lower() in {'dr.'}:
            return False
        if name.lower() in {'cilvēki', 'personas', 'darbinieki', 'vadība',
                            'pircēji', 'vīrieši', 'sievietes', 'latvija iedzīvotāji',
                            'savienība biedrs', 'personība', 'viesi', 'viesis',
                            'ieguvējs', 'vide', 'amats', 'amati', 'domas', 'idejas', 'vakars',
                            'norma', 'elite', 'būtisks', 'tālākie', 'guvēji', 'valdība'}: 
            return False
        if type != 'relationship' and isRelationshipName(name.lower()):
            return False
        if type not in {'descriptor', 'relationship'} and name.lower() in {
                'investori', 'konkurenti', 'klients'}:
            return False
        if type not in {'descriptor', 'profession'} and name.lower() in {
                'skolēni', 'studenti'}:
            return False
        if name.lower() in {'kompānija', 'firma', 'firmas', 'tiesa', 'dome',
                            'komisija', 'apvienība', 'organizācija', 
                            'studentu sabiedrība', 'sabiedrība'}: 
            return False
        if type not in {'descriptor', 'industry', 'profession'} and name.lower() in {
                'uzņēmums', 'aģentūra', 'portāls', 'banka', 'koncerns',
                'partija', 'birojs', 'augstskola', 'žurnāls', 'skola',
                'studija', 'frakcija', 'iestāde', 'fonds', 'korporācija'}:
            return False
        if name.lower() in {'gads', 'gada'}: 
            return False
        if name.lower() in {'a/s', 'sia', 'as'}: # lai nav kā aliasi šie 'noplēstie' prefiksi
            return False
        return True

    for e_id in entities.keys():
        entity = entities[e_id] 
        entity['representative'] = updateName(entity.get('representative'))
        entity['aliases'] = [updateName(alias) for alias in entity.get('aliases')]
        entity['aliases'] = [alias for alias in entity['aliases'] if goodName(entity.get('type'), alias)] #list(filter((lambda name: goodName(entity.get('type'), name)), entity.get('aliases')))
        if not entity.get('aliases'): # Hmm, tātad nekas nebija labs
            entity['aliases'] = ['_NEKONKRĒTS_'] #FIXME - jāsaprot vai nevar labāk to norādīt
            # entity['representative'] = '_NEKONKRĒTS_' 
        if not goodName(entity.get('type'), entity.get('representative')):
            entity['representative'] = entity.get('aliases')[0] # Pieņemam, ka gan jau pirmais derēs
        entities[e_id] = entity
        
    
# Boolean f-ja - aliasi kas principā pie entītijas ir pieļaujami, bet pēc kuriem nevajag meklēt citas entītijas
def goodAlias(name):
    if name.lower() in {'izglītība', 'karjera'}:  # headingu virsraksti cv importā
        return False
    if name.lower() in {'direktors', 'deputāts', 'loceklis', 'ministrs', 'latvietis', 'domnieks', 'sociālists', 'latvietis', 'premjers', 'sportists', 'vietnieks', 'premjerministrs', 'prezidents', 'vīrs', 'sieva', 'māte', 'deputāte'}: 
        # nekonkrētie - TODO: lai LVCoref kad veido sarakstu ar anaforu cluster alternatīvajiem vārdiem, jau uzreiz šos neiekļauj
        return False
    if name.lower() in {'dome'}:  # Ja nav atrasts specifiskāk 'Ventspils dome'
        return False
    return True

def fixName(name):
    fixname = re.sub('[«»“”„‟‹›〝〞〟＂]', '"', name, re.UNICODE)  # Aizvietojam pēdiņas
    fixname = re.sub("[‘’‚`‛]", "'", fixname, re.UNICODE)
    fixname = re.sub(' /$','', fixname, re.UNICODE) # čakars ar nepareizām entītiju robežām
    fixname = re.sub('_',' ', fixname, re.UNICODE) # divvārdu tokeni nāk formā kā_arī
    return fixname

#TODO - varbūt visu šo loģiku labāk SemanticApiPosgres modulī?
def personAliases(name):
    insertalias = [name] 
    if re.match(r'[A-ZĀČĒĢĪĶĻŅŠŪŽ]\w+ [A-ZČĒĢĪĶĻŅŠŪŽ]\w+$', name, re.UNICODE):
        extra_alias = re.sub(r'([A-ZČĒĢĪĶĻŅŠŪŽ])\w+ ', r'\1. ', name, flags=re.UNICODE )
        if not extra_alias in insertalias:
            insertalias.append(extra_alias)
    return insertalias

orgTypes = [
    ['SIA', 'Sabiedrība ar ierobežotu atbildību'],
    ['AS', 'A/S', 'Akciju sabiedrība'],
    ['apdrošināšanas AS', 'Apdrošināšanas akciju sabiedrība'],
    ['ZS', 'Z/S', 'Zemnieka saimniecība'],
    ['IU', 'Individuālais uzņēmums'],
    ['Zvejnieka saimniecība'],
    ['UAB'],
    ['VAS'],
    ['valsts aģentūra'],
    ['biedrība'],
    ['fonds'],
    ['mednieku biedrība'],
    ['mednieku klubs'],
    ['mednieku kolektīvs'],
    ['kooperatīvā sabiedrība'],
    ['nodibinājums'],
    ['komandītsabiedrība'],
    ['zvērinātu advokātu birojs'],
    ['advokātu birojs'],
    ['partija'],
    ['dzīvokļu īpašnieku kooperatīvā sabiedrība'],
    ['dzīvokļu īpašnieku biedrība'],
    ['Pilnsabiedrība', 'PS']
    ]

def orgAliases(name):
    aliases = set()
    aliases.add(name)
    representative = name

    fixname = fixName(name)
    aliases.add(fixname)
    if re.match(r'^"[^"]+"$', fixname, re.UNICODE):
        fixname = fixname[1:-1] # noņemam pirmo/pēdējo simbolu, kas ir pēdiņa
        aliases.add(fixname)

    if re.search(r'vidusskola', fixname, re.UNICODE): # vidusskolu saīsinājumi
        aliases.add(re.sub('vidusskola', 'vsk.', fixname, re.UNICODE))
        aliases.add(re.sub('vidusskola', 'vsk', fixname, re.UNICODE))

    understood = False
    for orgGroup in orgTypes:
        maintitle = orgGroup[0]
        clearname = None
        #TODO - šos regexpus varētu 1x sagatavot un nokompilēt, ja paliek par lēnu
        p1 = re.compile(r'^"([\w\s\.,\-\'\+/!:\(\)@&]+)" ?, (%s)$' % '|'.join(orgGroup), re.UNICODE) # "Kautkas", SIA
        m = p1.match(fixname)
        if m:
            clearname = m.group(1)
        p2 = re.compile(r'^(%s) " ?([\w\s\.,\-\'\+/!:\(\)@&]+) ?"$' % '|'.join(orgGroup), re.UNICODE) # SIA "Kautkas"
        m = p2.match(fixname)
        if m:
            clearname = m.group(2)

        if clearname:
            understood = True            
            representative = '%s "%s"' % (maintitle, clearname)  # SIA "Nosaukums"
            aliases.add(representative)
            for title in orgGroup:  # Visiem uzņēmējdarbības veida variantiem
                aliases.add('%s "%s"'     % (title, clearname)) # SIA "Nosaukums"
                aliases.add('%s %s'       % (title, clearname)) # SIA Nosaukums
                aliases.add('%s, %s'      % (clearname, title)) # Nosaukums, SIA
                aliases.add('"%s", %s'    % (clearname, title)) # "Nosaukums", SIA
                aliases.add('"%s"'        % (clearname, ))      # "Nosaukums"
                # aliases.add('%s'          % (clearname, ))      # Nosaukums   TODO - šis ir bīstams!   A/S "Dzintars" pārvērtīsies par Dzintars, kas konfliktēs ar personvārdiem, līdzīgi ļoti daudz firmu kam ir vietvārdi, utml
                # modifikācijas ar atstarpēm, kādas liek morfotageris
                aliases.add('" %s " , %s' % (clearname, title)) # " Nosaukums " , SIA  
                aliases.add('%s " %s "'   % (title, clearname)) # SIA " Nosaukums "
                aliases.add('" %s "'      % (clearname, ))      # " Nosaukums "
            break # nemeklējam tālāk

    if not understood:
        if not '"' in fixname and re.search(r' (partija|pārvalde|dome|iecirknis|aģentūra|augstskola|koledža|vēstniecība|asociācija|apvienība|savienība|centrs|skola|federācija|fonds|institūts|biedrība|teātris|pašvaldība|arodbiedrība|[Šš]ķīrējtiesa)$', fixname, re.UNICODE):
            aliases.add( clearOrgName(fixname) )
            understood = True # 'hardkodētie' nosaukumi kuriem bez standartformas citu aliasu nebūs
        elif re.search(r'(filiāle Latvijā|Latvijas filiāle|korporācija|biedrība|krājaizdevu sabiedrība|klubs|kopiena|atbalsta centrs|asociācija)$', fixname, re.UNICODE):
            aliases.add( clearOrgName(fixname) )
            understood = True # šādus nevar normāli normalizēt

    if not understood:
        # print 'Not understood', fixname # debuginfo - ja ir "labs" avots kur itkā vajadzētu būt 100% sakarīgiem nosaukumiem
        aliases.add( clearOrgName(fixname) )

    aliases.remove(representative)
    return [representative] + list(aliases)

# mēģina attīrīt organizāciju nosaukumus no viskautkā
def clearOrgName(name): 
    norm = re.sub('[«»“”„‟‹›〝〞〟＂"‘’‚‛\']', '', name, re.UNICODE)  # izmetam pēdiņas
    norm = re.sub('(AS|SIA|A/S|VSIA|VAS|Z/S|Akciju sabiedrība) ', '', norm, re.UNICODE)  # izmetam prefiksus
    norm = re.sub(', (AS|SIA|A/S|VSIA|VAS|Z/S|Akciju sabiedrība)', '', norm, re.UNICODE)  # ... postfixotie nosaukumi ar komatu
    norm = re.sub(' (AS|SIA|A/S|VSIA|VAS|Z/S|Akciju sabiedrība)', '', norm, re.UNICODE)  # ... arī beigās šādi reizēm esot bijuši
    norm = re.sub('\s\s+', ' ', norm, re.UNICODE)  # ja nu palika dubultatstarpes
    return norm

# Vai forma izskatās pēc 'pareizas' kas būtu rādāma UI - atradīs arī vispārīgas entītijas (piem. 'Latvijas uzņēmēji') kuras freimos jārāda, bet nevajag iekļaut nekur.
def hideEntity(name, category):
    if category == 'person':
        return not re.match(r'[A-ZĀČĒĢĪĶĻŅŠŪŽ]\w+\.? [A-ZČĒĢĪĶĻŅŠŪŽ]\w+$', name, re.UNICODE) # Personām par normāliem uzskatam vai nu 'Vārds Uzvārds' vai 'V. Uzvārds'
    if category == 'organization':
        return name == name.lower() # Organizācijām der praktiski jebkas, izņemam sugasvārdu frāzes kas ir all-lowercase
    if category in {'location', 'event', 'media', 'product'}:
        return False # Šos nemākam filtrēt
    return True # Citas entītiju kategorijas ir "atribūti nevis klasifikatori" un attiecīgi ir hidden

# Veic API requestu par vārdiem atbilstošām entītijām
# entities - dokumenta entīšu saraksts; neededEntities - kuras no tām parādās freimos un attiecīgi vajag likt globālajā stuff
# ... un globālo ID pielikt pie NE objekta lai tas pēc tam pieseivojas
def fetchGlobalIDs(entities, neededEntities, sentences, documentId, api=api):
    insertables = []
    request = []
    toDisambiguate = []
    disambiguationWhitelist = set( api.getBlessedEntityMentions(documentId) )

    for localID in neededEntities:
        entity = entities[str(localID)]
        if 'type' not in entity:
            print ("Grib likt datubāzē entīti bez tipa: {0}".format(entity['representative']))
        if entity.get('uqid'): # Ja mums jau izejas datos ir iedots unikālais ārējais ID (leta profila # vai personkods vai kas tāds)
            globalid = api.entity_id_by_outer_id(entity.get('uqid'))
            if globalid:
                entity['GlobalID'] = globalid
                continue

        if not entity.get('locations'):
            log.info("Entity %s without token locations", entity.get('representative'))

        if entity.get('representative')  == '_NEKONKRĒTS_':
            entity['GlobalID'] = 0 # Šādas entītijas datubāzei nederēs
            entity['hidden'] = True
            continue 

        matchedEntities = set()
        if not entity.get('representative') is None: 
            representative = fixName( entity.get('representative') )
            entity['representative'] = representative 
            matchedEntities = api.entity_ids_types_by_name_list(representative)
            # print('%d : %s' % (len(matchedEntities), representative))

        if len(matchedEntities) == 0 and entity.get('type') in {'person', 'organization'} : # neatradām - paskatīsimies pēc aliasiem NB! tikai priekš klasifikatoriem (pers/org)
            for alias in filter(goodAlias, entity.get('aliases')):
                matchedEntities = matchedEntities + api.entity_ids_types_by_name_list(alias)
                matchedEntities = matchedEntities + api.entity_ids_types_by_name_list(clearOrgName(alias))
            # Te varētu filtrēt, vai pēc aliasa nav atrasts kautkas nekorekts, kam neatbilst tips
            # bet tad ir pēc ID jānolasa to entītiju pilnie dati, ko skatīties; un tas būtu lēni.
        
        # No atrastajām entītēm mums patīesībā der tikai tās, kurām ir pareizais tips.
        matchedEntities = [e[0] for e in matchedEntities if getEntityTypeCode(entity['type']) == e[1]]
        
        if len(matchedEntities) == 0: # Tiešām neatradām - tātad nav tādas entītijas, insertosim
            insertables.append(localID) # šeit sakrājam entītiju objektu ID, lai pēc tam varētu piesiet pie API atbildēm

            representative = entity.get('representative')
            
            # pirms insertošanas personām pieliekam aliasu ar iniciāli, ja tāds tur jau nav
            if entity.get('type') == 'person':
                insertalias = personAliases(representative)
            elif entity.get('type') == 'organization':
                insertalias = orgAliases(representative)
                representative = insertalias[0] # Organizācijām te var izveidoties pilnāka pamatforma
            else:
                # Šeit ņemam tikai representative, nevis visus aliasus ko koreferences atrod. Ja ņemtu visus, tad te būtu interesanti jāfiltrē lai nebūtu nekorektas apvienošanas kā direktors -> skolas direktors un gads -> 1983. gads
                inflections = inflectEntity(representative, entity.get('type'))
                inflections = json.loads(inflections)
                entity['inflections'] = inflections
                insertalias = set(inflections.values())
                insertalias.add(representative)
                insertalias = list(insertalias)
                if entity.get('type') != 'descriptor':  
                    representative = inflections.get('Nominatīvs')

            category = getEntityTypeCode(entity.get('type'))
            outerId = [] # Organizācijām un personām pieliekam random UUID
            if category == 3:
                outerId = ['FP-' + str(uuid.uuid4())]
            if category == 2:
                outerId = ['JP-' + str(uuid.uuid4())]

            source = 'Upload %s, %s at %s' % (documentId, entity.get('source'), datetime.datetime.now().isoformat())

            hidden = hideEntity(representative, entity.get('type'))
            entity['hidden'] = hidden
            if entity.get('notReallyNeeded'): # override, ja entītijai nav freimu
                entity['hidden'] = True
            if showInserts or entityCreationDebuginfo:
                print('Gribētu insertot entītiju\t%s\t%s\t%r' % (representative, entity.get('type'), hidden))
            if realUpload:
                if entity.get('inflections'):
                    inflections = json.dumps(entity.get('inflections'), ensure_ascii=False)
                else:
                    # Ja NER nav iedevis, tad uzprasam lai webserviss izloka pašu atrasto
                    inflections = inflectEntity(representative, entity.get('type'))                
                entity['GlobalID'] = api.insertEntity(representative, insertalias, category, outerId, inflections, hidden, source=source, commit = False )
                api.insertMention(entity['GlobalID'], documentId, locations=entity.get('locations'))

        else: # Ir tāda entītija, piekārtojam globālo ID
            inWhitelist = False
            for candidateID in matchedEntities:
                if candidateID in disambiguationWhitelist:   # Ja ir blessed norāde, ka tieši šī globālā entītija ir šajā dokumentā
                    entity['GlobalID'] = candidateID
                    inWhitelist = True
            if inWhitelist: # Šajā gadījumā neskatamies kā disambiguēt
                continue 

            if len(matchedEntities) > 1 and entity.get('type') in {'person', 'organization'}: 
                toDisambiguate.append( (entity, matchedEntities) ) # pieglabājam tuple, lai apstrādātu tad kad visām viennozīmīgajām entītijām būs globalid atrasti
            else:
                entity['GlobalID'] = matchedEntities[0] # klasifikatoriem tāpat daudzmaz vienalga, vai pie kautkā piesaista vai veido jaunu
                if realUpload and entity.get('type') in {'person', 'organization'}: 
                    api.insertMention(matchedEntities[0], documentId, locations=entity.get('locations'))
        if 'GlobalID' in entity and showInserts:
            print ('Entītei piekārtoja globālo ID: {0} ({1}) - {2}'.format(representative, entity['type'], entity['GlobalID']))

    mentions = CDC.mentionbag(entities.values()) # TODO - šobrīd mentionbag visiem ir vienāds un tādēļ iekļauj arī pašas disambiguējamās entītijas vārdus
    for tup in toDisambiguate: # tuples - entity, matchedEntities
        entity = tup[0]
        matchedEntities = tup[1] 
        entity['GlobalID'] = cdcbags(entity, matchedEntities, mentions, sentences, documentId, api)  
        # entity['GlobalID'] = disambiguateEntity(entity, matchedEntities, entities, api) # izvēlamies ticamāko atbilstošo no šīm entītijām        
        if 'GlobalID' in entity and showInserts:
            print ('Entītei piekārtoja globālo ID ar disambiguāciju: {0} ({1}) - {2}'.format(representative, entity['type'], entity['GlobalID']))

def cdcbags(entity, matchedEntities, mentions, sentences, documentId, api=api):
    if showDisambiguation: # debuginfo    
        print()
        print(' ---  Disambiguācija vārdam', entity['representative'], '(', entity.get('type'), ')')
        print(entity['representative'], entity['aliases'], ": ", len(matchedEntities), " varianti..", matchedEntities)
        print('name bag :', CDC.namebag(entity))
        print('mention bag :', mentions)
        print('context bag :', CDC.contextbag(entity, sentences))
        print('----')

    best_k = None
    best_score = -99999
    for kandidaats in matchedEntities:
        bags = api.getCDCWordBags(kandidaats)
        if bags is None: # šai entītijai nekad nav ģenerēti CDC bagi... uzģenerēsim!    TODO - varbūt šo efektīvāk veikt kā batch job kautkur citur, piemēram, pie freimu summarizācijas šai entītei
            bags = buildGlobalEntityBags(kandidaats, api)
            api.putCDCWordBags(kandidaats, bags)

        if showDisambiguation: # debuginfo    
            db_info = api.entity_data_by_id(kandidaats)
            print('kandidāts', kandidaats, db_info['Name'], db_info['OuterId'])
            print(bags.get('namebag'))
            print('Name match:', CDC.cosineSimilarity(bags.get('namebag'), CDC.namebag(entity)))
            print(bags.get('mentionbag'))
            print('Mention match:', CDC.cosineSimilarity(bags.get('mentionbag'), mentions))
            print(bags.get('contextbag'))
            print('Context match:', CDC.cosineSimilarity(bags.get('contextbag'), CDC.contextbag(entity, sentences)))

        score = CDC.cosineSimilarity(bags.get('namebag'), CDC.namebag(entity)) + CDC.cosineSimilarity(bags.get('mentionbag'), mentions) + CDC.cosineSimilarity(bags.get('contextbag'), CDC.contextbag(entity, sentences))        
        if score > best_score:
            best_score = score
            best_k = kandidaats

    if showDisambiguation:
        print()
        print('Izvēlējāmies ', best_k)
        print()

    api.insertMention(best_k, documentId, True, best_score, locations=entity.get('locations')) # TODO - jāieliek arī ne-labākie mentioni
    return best_k

# Savāc visu vajadzīgo lai uztaisītu globālajai entītijai CDC datus
def buildGlobalEntityBags(globalID, api=api):
    db_info = api.entity_data_by_id(globalID)
    namebag = Counter() # namebag liksim aliasus, kā arī amatus/nodarbošanās - jo tie nonāk dokumenta entītijas aliasos
    mentionbag = Counter() # mentionbag liksim visas 'ID-entītes' kas labajos freimos ir saistītas ar šo ID - personas, organizācijas, vietas
    contextbag = Counter() 
    if not db_info:  # Ja entītija nav atrasta (jānoskaidro, kapēc) - tad ir tukši bag'i
        return {'namebag':namebag, 'mentionbag':mentionbag, 'contextbag':contextbag}

    for alias in db_info['OtherName']:
        namebag.update(alias.split()) # vairākvārdu aliasiem ieliekam katru atsevišķo vārdu
    
    freimi = api.summary_frame_data_by_id(globalID) # paņemam no DB visus summarizētos freimus par šo entīti

    for frame in freimi: 
        if not frame["Blessed"] and frame["SourceId"] != 'LETA CV dati': # Ņemam vērā manuāli blesotos freimus - tie ir autoritāte par to, ka saite attiecas tieši uz šo konkrēto 'vārdabrāli'
            continue

        elementi = convert_api_framedata(frame)
        if frame["FrameType"] == 9: # Being_employed
            amataID = elementi.get('Position')
            if amataID:
                amats = api.entity_data_by_id(amataID, False)['Name']
                namebag.update(amats.split())
        if frame["FrameType"] == 7: # People_by_vocation
            amataID = elementi.get('Vocation')
            if amataID:
                amats = api.entity_data_by_id(amataID, False)['Name']
                namebag.update(amats.split())
        if frame["FrameType"] == 26: # Unstructured - piemēram, abstraktā info no CV
            aprakstaID = elementi.get('Property')
            if aprakstaID:
                apraksts = api.entity_data_by_id(aprakstaID, False)['Name']
                contextbag.update(apraksts.split()) # Pieņemam, ka brīvā teksta apraksti labi korelē ar kontekstu reālajos rakstos

        for element in frame["FrameData"]:
            entityID = element['entityid']
            # Šis aizkomentētais būtu production variants - mazāk korekti nekā tiešās entīšu kategorijas, bet vajadzētu (nav pārbaudīts) būt būtiski ātrāk jo nav lieku DB request
            # defaultroletype = getDefaultEnityType(frame['FrameType'], element['Key'])
            # if (defaultroletype == 'person') or (defaultroletype == 'organization') or (defaultroletype == 'location'):
            #     mentionbag.add(entityID)   
            entity = api.entity_data_by_id(entityID, False)
            if entity and entity['Category'] <= 3 and entityID != globalID: # 1=location 2=organization 3=person
                mentionbag[entity.get('Name')] += 1

    for fact in api.entity_text_facts(globalID): # Analogs freima tipa #26 datiem, tikai atsevišķa tabula pēc 2014 struktūras izmaiņām
        contextbag.update(fact.split()) # Pieņemam, ka brīvā teksta apraksti labi korelē ar kontekstu reālajos rakstos
    
    if showDisambiguation: # debuginfo    
        print()
        print(' ---  Vācam whitelist datus entītijai', globalID, db_info['Name'], db_info['OuterId'])
    return {'namebag':namebag, 'mentionbag':mentionbag, 'contextbag':contextbag}

# NB! Šī ir legacy metode ar heiristikām; kodā šobrīd tiek lietota CDC metode ar bag-of-words cosine similarity principu; bet nākotnē varētu gribēties to kombinēt ar šo pieeju, tāpēc kods paliek
# entity: objekts
# matchingEntities: sarasts ar globālajiem id'iem
# Ja entītijai (personai) ir vairāki objekti ar vienādu vārdu, tad izvēlas, kurš no tiem IDiem atbilst labāk
def disambiguateEntity(entity, matchedEntities, entities, api=api):
    freimi = entity.get("frames")
    amati = []
    darbavietas = []
    for freims in freimi: # Atrodam 'signālus' par šo entītiju no dokumentā atrastajiem freimiem
        elementi = convert_elements(freims['elements'], freims['sentence'], entities)
        if freims['type'] == 'Being_employed':
            amats = elementi.get('Position')
            if amats:
                amati.append(amats) # TODO - te ielien dublikāti
            darbavieta = elementi.get('Employer')
            if darbavieta:
                darbavietas.append(darbavieta)
        if freims['type'] == 'Win_prize':
            None
        # else: #TODO - te vēl ar citiem freimu tipiem jāpadarbojas - piemēram, vocation, membership 
        #     print freims["type"], ":", freims["elements"]

    if showDisambiguation: # debuginfo
        print(' ---  Disambiguācija vārdam', entity['representative'], '(', entity.get('type'), ')')
        print(entity['representative'], entity['aliases'], ": ", len(matchedEntities), " varianti..", matchedEntities)

        print('Dokumenta objekts:')
        for amats in amati:
            print('Amats:', amats['representative'], '/', amats.get('GlobalID'))
        for darbavieta in darbavietas:
            print('Darbavieta:', darbavieta['representative'], '/', darbavieta.get('GlobalID'))
        print()

    best_k = None
    best_score = -99999
    for i in range(len(matchedEntities)):
        freimi = entityFrames(matchedEntities[i]) # paņemam no DB visus summarizētos freimus par šo entīti
        db_info = api.entity_data_by_id(matchedEntities[i])
        k = {} # Kandidāta datu objekts
        k['name'] = db_info['Name']
        k['aliases'] = db_info['OtherName']
        k['globalID'] = db_info['EntityId']
        k['outerID'] = db_info['OuterId'] 
        if isinstance(k['outerID'], list):
            k['outerID'] = k['outerID'][0] # TODO - jāizdomā kā apieties ja ir vairāki outerID (ja nākotnē sāks tur likt personkodus vai ko tādu)
        k['category'] = getEntityTypeName(db_info['Category'])
        k['amati'] = set()
        k['darbavietas'] = set()
        # if len(freimi) == 0:
        #     print '\tHmm, nav nekāda info summaryfreimos'
        for freims in freimi: 
            elementi = convert_api_framedata(freims)
            if freims["FrameType"] == 9: # Being_employed
                amats = elementi.get('Position')
                if amats:
                    k['amati'].add(amats)
                darbavieta = elementi.get('Employer')
                if darbavieta:
                    k['darbavietas'].add(darbavieta)
            # else: #TODO - te vēl ar citiem freimu tipiem jāpadarbojas - piemēram, vocation, membership 
            #     print getFrameName(freims["FrameType"]), ":", freims["FrameText"]

        # ... score novērtējums ... TODO - amatu/darbavietu/profesiju salīdzināšana
        score = 0
        score += outer_id_score(k['outerID'])
        if score > best_score:
            best_score = score
            best_k = k

        if showDisambiguation: # debuginfo
            print('Kandidāts',i,':',matchedEntities[i],'\tscore:', score)
            print('\t', k)
            print()

    result = best_k['globalID'] # Entītijai ar lielāko score
    if showDisambiguation:
        print() 
        print('Izvēlējāmies ', result)
        print()
    return result

# paņem sarakstu formā [{'tokenIndex': 3, 'name': 'Employer'}, {'tokenIndex': 5, 'name': 'Position'}] un pārveido uz Dict no lomas nosaukuma uz entītiju
def convert_elements(elements, sentence, entities):
    result = {}
    for element in elements:
        token = sentence.tokens[element['tokenIndex']-1]
        result[element['name']] = entities[str(token['namedEntityID'])]
    return result

# paņem API atgriezto framedata formātu un pārveido uz Dict no lomas nosaukuma uz entītijas global-ID
# DB dati ir domāti formā "[{"frameid":1377293,"roleid":1,"entityid":1563176}, {"frameid":1377293,"roleid":3,"entityid":1923390}]"
# Rezultāts ir domāts formā {"Employee":1563176, "Position":1923390}
def convert_api_framedata(frame):
    result = {}
    for element in frame["FrameData"]:
        rolename = getElementName(frame["FrameType"], element['roleid'])
        globalid = element['entityid']
        result[rolename] = globalid
    return result

# Entītiju disambiguācijai - konkrētā ID 'a priori' ticamība
def outer_id_score(id):
    # TODO - ideālā gadījumā mums būtu dati par to, cik dokumentos kura entīte parādās
    # Piemēram, ImantsZiedonis1 ir 3300 dokumentos, ImantsZiedonis2 ir 0 dokumentos.
    # Šobrīd tādu datu, šķiet, nav (Arņiem Vilkiem bija norādīts, citiem nemanīju)    

    # manuāli placeholderi testam (tās no test-50 personām, kam ir vārdabrāļi)
    if id == "F6A8C3B7-AC39-11D4-9D85-00A0C9CFC2DB": return 100 # Imants Ziedonis, vienīgais pieminētais
    if id == "3AFC2FB8-4879-11D5-AE84-0010B5A3DE2F": return -100 # šis neparādās reālos dokumentos

    if id == "F5C17E17-AC39-11D4-9D85-00A0C9CFC2DB": return 100 # Gunārs Upenieks, vienīgais pieminētais
    if id == "8E336987-D834-47F8-A590-D0D473FABADE": return -100 # šis neparādās reālos dokumentos

    if id == "F6A8BD85-AC39-11D4-9D85-00A0C9CFC2DB": return 50 # 'primārais' Gunārs Freimanis (ir citi kam nav LETA-profilu)
    if id == "CD900493-1E9A-11D5-AE08-0010B594D402": return -50 # šis gandrīz neparādās reālos dokumentos

    if id == "F6A8B27C-AC39-11D4-9D85-00A0C9CFC2DB": return 50 # 'primārais' Jānis Freimanis (ir citi kam nav LETA-profilu)
    if id == "F6A8B27F-AC39-11D4-9D85-00A0C9CFC2DB": return -100 # šis neparādās reālos dokumentos
    if id == "F6A8B279-AC39-11D4-9D85-00A0C9CFC2DB": return -1000 # tukšs profils - gļuks LETA sourcedatos http://www.leta.lv/archive/search/?patern=Freimanis%20J%C4%81nis&item=F6A8B279-AC39-11D4-9D85-00A0C9CFC2DB

    if id == "F6A8BDCF-AC39-11D4-9D85-00A0C9CFC2DB": return 50 # biežākais Jānis Gailis (ir arī citi kam nav LETA-profilu)
    if id == "F6A8BDD2-AC39-11D4-9D85-00A0C9CFC2DB": return 30 # retāk pieminēts 
    if id == "F6A8BDD5-AC39-11D4-9D85-00A0C9CFC2DB": return 10 # pavisam reti pieminēts 

    if id is None: return -100 # ja nu ir izvēle starp tādu entītiju kam ir profils un tādu, kam nav - liekam pie 'zināmās'
    if id.startswith("FP-") or id.startswith("JP-"): return -10 # Kamēr nav entītiju blesošana, šādi prioritizējam LETA iepriekšējos profilus (VIP) no automātiski veidotajiem
    return 0

# Izveido dokumenta kompakto reprezentāciju, lai to varētu saglabāt datubāzē; šo f-ju veidojis Didzis
def compact_document(document):
    global frameroleids, frametypeids

    sentences = []

    for sentence in document.sentences:
        sent = [[]]
        for frame in sentence.frames:
            if not frame.tokenIndex:
                continue
            frametypeid = getFrameCode(frame.type)
            fr = [[frametypeid,frame.tokenIndex]]
            for element in frame.elements:
                if not element.tokenIndex:
                    continue
                frameroleid = getElementCode(frametypeid, element.name)
                fr.append([frameroleid,element.tokenIndex])
            sent[0].append(fr)

        sent.append('|'.join(token.form.replace('|','&sp;') for token in sentence.tokens))
        sentences.append(sent)

    return json.dumps(sentences, indent=None, separators=(',',':'), check_circular=False, ensure_ascii=False)


