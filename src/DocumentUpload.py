#!/usr/bin/env python
# coding=utf-8

import requests, json, time, datetime, re, copy, uuid, codecs, pylru, urllib
from collections import Counter
from frameinfo2 import getFrameType, getElementCode, getNETypeCode, getDefaultRole, getFrameName, getElementName, getNETypeName
#from FrameInfo import FrameInfo
#f_info = FrameInfo("input/frames-new-modified.xlsx")
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection
from db_config import api_conn_info, inflection_webservice
import CDC
import logging as log

realUpload = True # Vai lādēt DB pa īstam - lai testu laikā nečakarē DB datus
showInserts = False # Vai rādīt uz console to, ko mēģina insertot DB
showDisambiguation = False # Vai rādīt uz console entītiju disambiguācijas debug
entityCreationDebuginfo = False # Vai rādīt uz console potenciālās jaunradītās entītijas

conn = PostgresConnection(api_conn_info)
api = SemanticApiPostgres(conn) # TODO - šo te uz uploadJSON.py

# Ielādē teikumos minētos faktus faktu postgres db; pirms tam atrodot entītiju globālos id vai izveidojot entītijas, uz kurām atsaucas atrastie freimi
def upload2db(document): # document -> dict ar pilniem dokumenta+ner+freimu datiem, kāds nāk no Didža Framer vai arī saglabātā JSON formāta
    if realUpload: 
        api.cleanupDB(document.id) # iztīram šī dokumenta ID agrāk ielasītos raw-frames

    sentences = document.sentences
    entities = document.namedEntities

    neededEntities = set() # Savācam entītijas, kuras dokumentā piemin kāds freims
    for sentence in sentences:
        for frame in sentence.frames:
            addDate(frame, document.date) # Fills the Time attribute for selected frames from document metadata, if it is empty
            frame['sentence'] = sentence # backpointer tālākai apstrādei
            
            for element in frame.elements:
                entityid = makeEntityIfNeeded(entities, sentence.tokens, element.tokenIndex, frame, element)
                    # te  modificē NE sarakstu, pieliekot freima elementus ja tie tur jau nav
                    # te pie esošajām entītijām pieliek norādi uz freimiem, kuros entītija piedalās - TODO: pēc tam novākt, lai nečakarē json seivošanu
                neededEntities.add(entityid)

    for entity in entities.values():
        if (entity.get('type') == 'person') or (entity.get('type') == 'organization'):
            neededEntities.add(entity['id']) # personas un organizācijas insertojam vienmēr, lai piefiksētu tās, kas dokumentā ir pieminētas bet nav freimos

    # Entītiju nosaukumu filtrs - aizvietojam relatīvos laikus ('vakar'); likvidējam nekonkrētos aliasus ('viņš').
    filterEntityNames(entities, document.date)

    #Katrai entītijai piekārtojam globālo ID
    fetchGlobalIDs(entities, neededEntities, sentences, document.id) # TODO - varbūt jau sākumā pie tās pašas sentenču apstaigāšanas arī jāsaveido entītiju contextbag 

    # Tagad vēlreiz apskatam visus freimus, un insertojam tos DB
    requests = []
    for sentenceID, sentence in enumerate(sentences):
        for frame in sentence.frames:
            if len(frame.elements) < 2:
                continue # mikrofreimus neliekam. TODO: te varētu vēl pat agresīvāk filtrēt
            frameType = getFrameType(frame.type)

            elements = {} # dict no elementu lomas koda uz attiecīgās entītes ID
            filledRoles = set() 
            usedEntities = set()
            for element in frame.elements:
                elementCode = getElementCode(frameType, element.name)
                entityID = sentence.tokens[element.tokenIndex-1].namedEntityID  # entītija kas atbilst freima elementam - iepriekšējā ciklā 100% visām jābūt korekti saformētām
                globalID = entities[str(entityID)].get(u'GlobalID')
                if globalID is None and realUpload:
                    log.error('Neatradu globalID entītijai %s', entities[str(entityID)].get('representative'))
                
                if elementCode in filledRoles: # Freimu analizators nedrīkstētu iedot 2x vienādas lomas, bet ja nu tomēr, tad lai te nenomirst
                    log.debug('Lomai %s vairāki varianti: %s ir par daudz', element.name, sentence.tokens[element.tokenIndex-1].form )
                elif globalID in usedEntities: 
                    log.debug('Entītija #%d atkal parādās tai pašā freimā pie vārda %s', globalID, sentence.tokens[element.tokenIndex-1].form)
                    # principā šāds varētu rasties ja koreferences saliek 2 NER-atrastas entītijas kopā, un vienā freimā pieliek pie katru savas lomas (piemēram, amats+personvārds?) - bet īsti labi tas nav
                else:
                    # print frame.type, element.name, entityID, entities[str(entityID)]['representative'], '(', sentence.tokens[element.tokenIndex-1].form, ')', globalID
                    elements[elementCode] = globalID
                    #elements.append({'Key':elementCode, 'Value':{'Entity':globalID, 'PlaceInSentence':element.tokenIndex}})
                    filledRoles.add(elementCode)
                    usedEntities.add(globalID)
                    
            if showInserts:
                print 'Gribētu insertot freimu', frame.type, elements
            if realUpload:                
                targetword = sentence.tokens[frame.tokenIndex-1].form
                source = 'Pipeline parse at '+datetime.datetime.now().isoformat()
                api.insertFrame(frameType, elements, document.id, source, sentenceID+1, targetword, document.date.isoformat())

    if realUpload: 
        api.insertDocument(document.id, document.date.isoformat())
        for entity in entities.values():
            hidden = entity.get('hidden')
            if hidden == None:
                hidden = hideEntity(entity['representative'], entity['type'])
            if hidden == False and ((entity.get('type') == 'person') or (entity.get('type') == 'organization')):
                api.dirtyEntity(entity.get(u'GlobalID'))
        api.api.commit()

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
        if entity['representative'] == phrase:
            matchingEntity = entity
        if entity['id'] >= entityID:
            entityID = entity['id'] + 1

    if matchingEntity is None:            
        entity = {
            'id': entityID,
            'type': namedEntityType,  # TODO - te varētu nočekot vai NER iedotais tips iederas šajā lomā
            'representative': phrase, # Te varētu arī nebūt pamatforma; bet to risinās vēlāk - lai freimmarķiera atrastās entītijas apstrādā identiski ar NER atrastajām
            'source': 'framemarker',  # Lai pēc tam debugUI var atšķirt no LVTagger/LVCoref veidotajām entītēm
            'aliases': [phrase]
        }
        entities[str(entityID)] = entity
        # print entity
    else:
        entityID = matchingEntity['id']
    return entityID

# Apstaigājam atrastajam galvasvārdam pakļautos vārdus un izveidojam tādu
def entityPhraseByTree(tokenIndex, tokens):
    included = {tokenIndex}
    dirty = True # Vai vajag vēl dziļāk apstaigāt
    terminator = False # Vai esam atraduši ko tādu, ka tok dziļāk nelienam

    while dirty and not terminator: 
        dirty = False
        newincluded = copy.copy(included)
        for token in tokens:
            if (token.parentIndex in included) and not (token.index in included): # pievienojam visus bērnus kamēr vairs nav ko pievienot
                #TODO - vajag pārbaudi lai neiekļautu apakšteikumus
                if token.lemma in (u'(', u')', u';'):
                    terminator = True
                dirty = True
                newincluded.add(token.index) 
        included = newincluded # šāda maisīšana tādēļ, lai terminators nogriež visu dziļumā x, nevis aiziet pa ciklu līdz teikuma galam

    phrase = []
    for token in tokens:
        if token.index in included:
            phrase.append(token.form)
    phrase = " ".join(phrase)
    if phrase.endswith(u' un'):
        phrase = phrase[:-3] # noņemam un
    if phrase.startswith(u'(') and phrase.endswith(u')'):
        phrase = phrase[1:-1] # noņemam iekavas

    return phrase

# Apstaigājam atrastajam galvasvārdam blakus esošos vārdus ar vienādu NER kategoriju
def entityPhraseByNER(tokenIndex, tokens):    
    tips = tokens[tokenIndex-1].namedEntityType

    low = tokenIndex-1
    while low > 0 and tokens[low-1].namedEntityType == tips:
        low -= 1
    high = tokenIndex-1
    while high < len(tokens)-1 and tokens[high+1].namedEntityType == tips:
        high += 1

    phrase = []
    for token in tokens[low:high+1]:
        phrase.append(token.form)
    phrase = " ".join(phrase)
    return phrase


# Noformē entītijas vārdu, ja ir dots teikums + entītes galvenā vārda (head) id
# ja tādas entītijas nav, tad pievieno jaunu sarakstam
def makeEntityIfNeeded(entities, tokens, tokenIndex, frame, element):
    if tokenIndex > len(tokens):
        log.error('Error: entity sākas tokenā #%d no %d datos : %s',tokenIndex,len(tokens), repr(tokens))
        return 0
    else: 
        frameType = getFrameType(frame.type)
        elementCode = getElementCode(frameType, element.name)
        defaultType = getDefaultRole(frameType, elementCode)

        headtoken = tokens[tokenIndex-1] 

        if headtoken.namedEntityType is None or headtoken.namedEntityType == u'O':
            headtoken.namedEntityType = defaultType   # Ja NER nav iedevis tipu, tad mēs no freima elementa varam to izdomāt.

        if headtoken['namedEntityType'] == None:
            sys.stderr.write(u'Entītijai nav tipa :( un defaulttips ir %s', (defaultType, ))
        if headtoken['namedEntityType'] == u'None': 
            sys.stderr.write(u'Entītijai tips ir "None" un defaulttips ir %s', (defaultType, ))

        entityID = headtoken.namedEntityID 

        # PP 2013-11-24 - fix tam, ka LVCoref pagaidām mēdz profesijas pielinkot kā entītiju identisku personai
        if entityID is not None and entities[str(entityID)].get(u'type') == u'person' and headtoken.namedEntityType == u'profession':
            phrase = entityPhraseByNER(tokenIndex, tokens)
            entityID = makeEntity(entities, phrase, headtoken['namedEntityType'])

        if entityID is None and headtoken.pos == u'p': # vietniekvārds, kas nav ne ar ko savilkts
            entityID = makeEntity(entities, u'_NEKONKRĒTS_', headtoken['namedEntityType'])

        # Pamēģinam paskatīties parent - reizēm freimtageris norāda uz vārdu bet NER ir entītijas galvu ielicis uzvārdam.
        if entityID is None or (headtoken.form == u'Rīgas' and headtoken.namedEntityType == u'organization'):   # Tas OR ir hacks priekš Rīgas Tehniskās universitātes kuru nosauc par Rīgu..
            # Pa lielam, ja 'atrastais' vārds ir pareiza tipa entītijas iekšpusē tad ieliekam nevis tagera atrasto bet visu lielo entīti
            parent = tokens[headtoken.parentIndex-1]
            if headtoken.namedEntityType == parent.namedEntityType:
                entityID = parent.namedEntityID

        # TODO - varbūt pirms koka vajadzētu uz NER robežām paskatīties? jānotestē kas dod labākus rezultātus
        # if entityID is None:
        #     phrase = entityPhraseByNER(tokenIndex, tokens)
        #     entityID = makeEntity(entities, phrase, headtoken['namedEntityType'])

        # Ja nu toč nav tādas entītijas... tad veidojam no koka
        if entityID is None: 
            phrase = entityPhraseByTree(tokenIndex, tokens)
            entityID = makeEntity(entities, phrase, headtoken['namedEntityType'])

    headtoken['namedEntityID'] = entityID
    frames = entities[str(entityID)].get("frames")
    if frames is None:
        frames = []
    frames.append(frame)
    entities[str(entityID)]["frames"] = frames
    return entityID

# Fills the Time attribute for selected frames from document metadata, if it is empty
def addDate(frame, documentdate): 
    return # FIXME - jāsaprot kuriem vajag

# Saņem entītiju sarakstu un dokumenta radīšanas datumu; satīra nosaukumus - aizvietojam relatīvos laikus ('vakar'); likvidējam nekonkrētos ('viņš').
def filterEntityNames(entities, documentdate):
    def updateName(name):
        if name.lower() in {u'šodien', u'patlaban', u'tagad', u'pašlaik', u'šonedēļ'}:
            return documentdate.isoformat()
        if name.lower() == u'vakar':
            return (documentdate - datetime.timedelta(days=1)).isoformat()
        if name.lower() == u'rīt':
            return (documentdate + datetime.timedelta(days=1)).isoformat()
        if name.lower() == u'šogad':
            return str(documentdate.year)
        if name.lower() in {u'pagājušgad', u'pērn', u'pagājušajā gads', u'pagājušajā gadā'}:
            return str(documentdate.year - 1)
        # TODO - šie ir biežākie, bet vēl vajadzētu relatīvos laikus: 'jūlijā' -> pēdējais jūlijs, kas ir bijis (šis vai iepriekšējais gads) utml.
        return name

    def goodName(name):
        if len(name) <= 2 and not re.match(r'[A-ZĀČĒĢĪĶĻŅŠŪŽ]+|\d+$', name, re.UNICODE): # tik īsi drīkst būt tikai cipari vai organizāciju (partiju) saīsinājumi
            return False
        if name.lower() in {u'viņš', u'viņs', u'viņa', u'viņam', u'viņu', u'viņā', u'viņas', u'viņai', u'viņās', u'viņi', u'viņiem', u'viņām',
                            u'es', u'mēs', u'man', u'mūs', u'mums', u'tu', u'tev', u'jūs', u'jums', u'jūsu',
                            u'tas', u'tā', u'tie', u'tās', u'tajā', u'kas', u'kam', u'tam', u'tām', u'ko', u'to', u'tos', u'tai', u'tiem',
                            u'sava', u'savu', u'savas', u'savus', 
                            u'kurš', u'kuru', u'kura', u'kuram', u'kuri', u'kuras', u'kurai', u'kuriem', u'kurām', u'kurā', u'kurās',
                            u'būs', u'arī', u'dr.', u'jau', u'tur'}: 
            return False
        if name in {u'Var', u'gan'}:
            return False
        if name.lower() in {u'uzņēmums', u'kompānija',  u'firma', u'firmas', u'aģentūra', u'portāls', u'tiesa', u'banka', u'fonds', u'koncerns',
                            u'komisija', u'partija', u'apvienība', u'frakcija', u'birojs', u'dome', u'organizācija', u'augstskola', u'investori',
                            u'studentu sabiedrība', u'studija', u'žurnāls', u'sabiedrība', u'iestāde', u'skola',
                            u'cilvēki', u'personas', u'darbinieki', u'vadība', u'pircēji', u'vīrieši', u'sievietes', u'konkurenti', u'latvija iedzīvotāji',
                            u'savienība biedrs', u'skolēni', u'studenti', u'personība', u'viesi', u'viesis', u'ieguvējs', u'klients',
                            u'vide', u'amats', u'amati', u'domas', u'idejas', u'vakars', u'norma', u'elite', u'būtisks', u'tālākie', u'guvēji'}: 
            return False
        if name.lower() in {u'gads', u'gada'}: 
            return False
        if name.lower() in {u'a/s', u'sia', u'as'}: # lai nav kā aliasi šie 'noplēstie' prefiksi
            return False
        return True

    for e_id in entities.keys():
        entity = entities[e_id]
        entity['representative'] = updateName(entity.get('representative'))
        entity['aliases'] = [updateName(alias) for alias in entity.get('aliases')]
        entity['aliases'] = filter(goodName, entity.get('aliases'))
        if not entity.get('aliases'): # Hmm, tātad nekas nebija labs
            entity['aliases'] = [u'_NEKONKRĒTS_'] #FIXME - jāsaprot vai nevar labāk to norādīt
            # entity['representative'] = u'_NEKONKRĒTS_' 
        if not goodName(entity.get('representative')):
            entity['representative'] = entity.get('aliases')[0] # Pieņemam, ka gan jau pirmais derēs
        entities[e_id] = entity
    
# Boolean f-ja - aliasi kas principā pie entītijas ir pieļaujami, bet pēc kuriem nevajag meklēt citas entītijas
def goodAlias(name):
    if name.lower() in {u'izglītība', u'karjera'}:  # headingu virsraksti cv importā
        return False
    if name.lower() in {u'direktors', u'deputāts', u'loceklis', u'ministrs', u'latvietis', u'domnieks', u'sociālists', u'latvietis', u'premjers', u'sportists', u'vietnieks', u'premjerministrs', u'prezidents', u'vīrs', u'sieva', u'māte', u'deputāte'}: 
        # nekonkrētie - TODO: lai LVCoref kad veido sarakstu ar anaforu cluster alternatīvajiem vārdiem, jau uzreiz šos neiekļauj
        return False
    if name.lower() in {u'dome'}:  # Ja nav atrasts specifiskāk 'Ventspils dome'
        return False
    return True

def fixName(name):
    fixname = re.sub(u'[«»“”„‟‹›〝〞〟＂]', '"', name, re.UNICODE)  # Aizvietojam pēdiņas
    fixname = re.sub(u"[‘’‚`‛]", "'", fixname, re.UNICODE)
    return fixname

#TODO - varbūt visu šo loģiku labāk SemanticApiPosgres modulī?
def personAliases(name):
    insertalias = [name] 
    if re.match(ur'[A-ZĀČĒĢĪĶĻŅŠŪŽ]\w+ [A-ZČĒĢĪĶĻŅŠŪŽ]\w+$', name, re.UNICODE):
        extra_alias = re.sub(ur'([A-ZČĒĢĪĶĻŅŠŪŽ])\w+ ', ur'\1. ', name, flags=re.UNICODE )
        if not extra_alias in insertalias:
            insertalias.append(extra_alias)
    return insertalias

orgTypes = [
    ['SIA', u'Sabiedrība ar ierobežotu atbildību'],
    ['AS', 'A/S', u'Akciju sabiedrība'],
    [u'apdrošināšanas AS', u'Apdrošināšanas akciju sabiedrība'],
    ['ZS', 'Z/S', u'Zemnieka saimniecība'],
    ['IU', u'Individuālais uzņēmums'],
    [u'Zvejnieka saimniecība'],
    ['UAB'],
    ['VAS'],
    [u'valsts aģentūra'],
    [u'biedrība'],
    [u'fonds'],
    [u'mednieku biedrība'],
    [u'mednieku klubs'],
    [u'mednieku kolektīvs'],
    [u'kooperatīvā sabiedrība'],
    [u'nodibinājums'],
    [u'komandītsabiedrība'],
    [u'zvērinātu advokātu birojs'],
    [u'advokātu birojs'],
    ['partija'],
    [u'dzīvokļu īpašnieku kooperatīvā sabiedrība'],
    [u'dzīvokļu īpašnieku biedrība'],
    [u'Pilnsabiedrība', u'PS']
    ]

def orgAliases(name):
    aliases = set()
    aliases.add(name)
    representative = name

    fixname = fixName(name)
    aliases.add(fixname)
    if re.match(ur'^"[^"]+"$', fixname, re.UNICODE):
        fixname = fixname[1:-1] # noņemam pirmo/pēdējo simbolu, kas ir pēdiņa
        aliases.add(fixname)

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
        if not '"' in fixname and re.search(ur' (partija|pārvalde|dome|iecirknis|aģentūra|augstskola|koledža|vēstniecība|asociācija|apvienība|savienība|centrs|skola|federācija|fonds|institūts|biedrība|teātris|pašvaldība|arodbiedrība|[Šš]ķīrējtiesa)$', fixname, re.UNICODE):
            aliases.add( clearOrgName(fixname) )
            understood = True # 'hardkodētie' nosaukumi kuriem bez standartformas citu aliasu nebūs
        elif re.search(ur'(filiāle Latvijā|Latvijas filiāle|korporācija|biedrība|krājaizdevu sabiedrība|klubs|kopiena|atbalsta centrs|asociācija)$', fixname, re.UNICODE):
            aliases.add( clearOrgName(fixname) )
            understood = True # šādus nevar normāli normalizēt

    if not understood:
        # print 'Not understood', fixname # debuginfo - ja ir "labs" avots kur itkā vajadzētu būt 100% sakarīgiem nosaukumiem
        aliases.add( clearOrgName(fixname) )

    aliases.remove(representative)
    return [representative] + list(aliases)

# mēģina attīrīt organizāciju nosaukumus no viskautkā
def clearOrgName(name): 
    norm = re.sub(u'[«»“”„‟‹›〝〞〟＂"‘’‚‛\']', '', name, re.UNICODE)  # izmetam pēdiņas
    norm = re.sub(u'(AS|SIA|A/S|VSIA|VAS|Z/S|Akciju sabiedrība) ', '', norm, re.UNICODE)  # izmetam prefiksus
    norm = re.sub(u', (AS|SIA|A/S|VSIA|VAS|Z/S|Akciju sabiedrība)', '', norm, re.UNICODE)  # ... postfixotie nosaukumi ar komatu
    norm = re.sub(u' (AS|SIA|A/S|VSIA|VAS|Z/S|Akciju sabiedrība)', '', norm, re.UNICODE)  # ... arī beigās šādi reizēm esot bijuši
    norm = re.sub(u'\s\s+', ' ', norm, re.UNICODE)  # ja nu palika dubultatstarpes
    return norm

def inflectEntity(name, category):
    r = requests.get('http://%s:%d/inflect_phrase/%s?category=%s' % (inflection_webservice.get('host'), inflection_webservice.get('port'), name, category) ) 
    return r.text # TODO - errorchecking

# Vai forma izskatās pēc 'pareizas' - atradīs arī vispārīgas entītijas (piem. 'Latvijas uzņēmēji') kuras freimos jārāda, bet nevajag iekļaut nekur.
def hideEntity(name, category):
    if category == u'person':
        return not re.match(ur'[A-ZĀČĒĢĪĶĻŅŠŪŽ]\w+\.? [A-ZČĒĢĪĶĻŅŠŪŽ]\w+$', name, re.UNICODE) # Personām par normāliem uzskatam vai nu 'Vārds Uzvārds' vai 'V. Uzvārds'
    if category == u'organization':
        return name == name.lower() # Organizācijām der praktiski jebkas, izņemam sugasvārdu frāzes kas ir all-lowercase
    if category in {u'location', u'event', u'media', u'product'}:
        return False # Šos nemākam filtrēt
    return True # Citas entītiju kategorijas ir "atribūti nevis klasifikatori" un attiecīgi ir hidden

# Veic API requestu par vārdiem atbilstošām entītijām
# entities - dokumenta entīšu saraksts; neededEntities - kuras no tām parādās freimos un attiecīgi vajag likt globālajā stuff
# ... un globālo ID pielikt pie NE objekta lai tas pēc tam pieseivojas
def fetchGlobalIDs(entities, neededEntities, sentences, documentId):
    insertables = []
    request = []
    toDisambiguate = []
    disambiguationWhitelist = set( api.getBlessedEntityMentions(documentId) )

    for localID in neededEntities:
        entity = entities[str(localID)]
        if entity.get('representative')  == u'_NEKONKRĒTS_':
            entity[u'GlobalID'] = 0 # Šādas entītijas datubāzei nederēs
            entity[u'hidden'] = True
            continue 

        matchedEntities = set()
        if not entity.get('representative') is None: 
            representative = fixName( entity.get('representative') )
            entity['representative'] = representative 
            matchedEntities = api.entity_ids_by_name_list(representative)

        if len(matchedEntities) == 0 and entity['type'] in {'person', u'person', 'organization', u'organization'} : # neatradām - paskatīsimies pēc aliasiem NB! tikai priekš klasifikatoriem (pers/org)
            for alias in filter(goodAlias, entity.get('aliases')):
                matchedEntities = matchedEntities + api.entity_ids_by_name_list(alias)
                matchedEntities = matchedEntities + api.entity_ids_by_name_list(clearOrgName(alias))
            # Te varētu filtrēt, vai pēc aliasa nav atrasts kautkas nekorekts, kam neatbilst tips
            # bet tad ir pēc ID jānolasa to entītiju pilnie dati, ko skatīties; un tas būtu lēni.

        if len(matchedEntities) == 0: # Tiešām neatradām - tātad nav tādas entītijas, insertosim
            insertables.append(localID) # šeit sakrājam entītiju objektu ID, lai pēc tam varētu piesiet pie API atbildēm

            representative = entity.get('representative')
            
            # pirms insertošanas personām pieliekam aliasu ar iniciāli, ja tāds tur jau nav
            if entity['type'] == 'person' or entity['type'] == u'person':                
                insertalias = personAliases(representative)
            elif entity['type'] == 'organization' or entity['type'] == u'organization':
                insertalias = orgAliases(representative)
                representative = insertalias[0] # Organizācijām te var izveidoties pilnāka pamatforma
            else:
                # Šeit ņemam tikai representative, nevis visus aliasus ko koreferences atrod. Ja ņemtu visus, tad te būtu interesanti jāfiltrē lai nebūtu nekorektas apvienošanas kā direktors -> skolas direktors un gads -> 1983. gads
                inflections = inflectEntity(representative, entity['type'])
                entity['inflections'] = inflections
                inflections = json.loads(inflections)
                representative = inflections.get(u'Nominatīvs')
                insertalias = list(set(inflections.values()))                

            category = getNETypeCode(entity['type'])
            outerId = [] # Organizācijām un personām pieliekam random UUID
            if category == 3:
                outerId = ['FP-' + str(uuid.uuid4())]
            if category == 2:
                outerId = ['JP-' + str(uuid.uuid4())]

            hidden = hideEntity(representative, entity['type'])
            entity[u'hidden'] = hidden
            if showInserts or entityCreationDebuginfo:
                print u'Gribētu insertot entītiju\t%s\t%s\t%r' % (representative, entity['type'], hidden)
            if realUpload:
                if entity.get('inflections'):
                    inflections = json.dumps(entity.get('inflections'))
                else:
                    # Ja NER nav iedevis, tad uzprasam lai webserviss izloka pašu atrasto
                    inflections = inflectEntity(representative, entity['type'])
                entity[u'GlobalID'] = api.insertEntity(representative, insertalias, category, outerId, inflections, hidden, commit = False )
                api.insertMention(entity[u'GlobalID'], documentId)

        else: # Ir tāda entītija, piekārtojam globālo ID
            inWhitelist = False
            for candidateID in matchedEntities:
                if candidateID in disambiguationWhitelist:   # Ja ir blessed norāde, ka tieši šī globālā entītija ir šajā dokumentā
                    entity[u'GlobalID'] = candidateID
                    inWhitelist = True
            if inWhitelist: # Šajā gadījumā neskatamies kā disambiguēt
                continue 

            if len(matchedEntities) > 1 and entity['type'] in {'person', u'person', 'organization', u'organization'}: 
                toDisambiguate.append( (entity, matchedEntities) ) # pieglabājam tuple, lai apstrādātu tad kad visām viennozīmīgajām entītijām būs globalid atrasti
            else:
                entity[u'GlobalID'] = matchedEntities[0] # klasifikatoriem tāpat daudzmaz vienalga, vai pie kautkā piesaista vai veido jaunu
                if realUpload and entity['type'] in {'person', u'person', 'organization', u'organization'}: 
                    api.insertMention(matchedEntities[0], documentId)

    mentions = CDC.mentionbag(entities.values()) # TODO - šobrīd mentionbag visiem ir vienāds un tādēļ iekļauj arī pašas disambiguējamās entītijas vārdus
    for tup in toDisambiguate: # tuples - entity, matchedEntities
        entity = tup[0]
        matchedEntities = tup[1]
        entity[u'GlobalID'] = cdcbags(entity, matchedEntities, mentions, sentences, documentId)  
        # entity[u'GlobalID'] = disambiguateEntity(entity, matchedEntities, entities) # izvēlamies ticamāko atbilstošo no šīm entītijām        

def cdcbags(entity, matchedEntities, mentions, sentences, documentId):
    if showDisambiguation: # debuginfo    
        print
        print ' ---  Disambiguācija vārdam', entity['representative'], '(', entity['type'], ')'
        print entity['representative'], entity['aliases'], ": ", len(matchedEntities), " varianti..", matchedEntities
        print 'name bag :', CDC.namebag(entity)
        print 'mention bag :', mentions
        print 'context bag :', CDC.contextbag(entity, sentences)
        print '----'

    best_k = None
    best_score = -99999
    for kandidaats in matchedEntities:
        bags = api.getCDCWordBags(kandidaats)
        if bags is None: # šai entītijai nekad nav ģenerēti CDC bagi... uzģenerēsim!    TODO - varbūt šo efektīvāk veikt kā batch job kautkur citur, piemēram, pie freimu summarizācijas šai entītei
            bags = buildGlobalEntityBags(kandidaats)
            api.putCDCWordBags(kandidaats, bags)

        if showDisambiguation: # debuginfo    
            db_info = api.entity_data_by_id(kandidaats)
            print 'kandidāts', kandidaats, db_info['Name'], db_info['OuterId']            
            print bags.get('namebag')
            print 'Name match:', CDC.cosineSimilarity(bags.get('namebag'), CDC.namebag(entity))
            print bags.get('mentionbag')
            print 'Mention match:', CDC.cosineSimilarity(bags.get('mentionbag'), mentions)
            print bags.get('contextbag')
            print 'Context match:', CDC.cosineSimilarity(bags.get('contextbag'), CDC.contextbag(entity, sentences))

        score = CDC.cosineSimilarity(bags.get('namebag'), CDC.namebag(entity)) + CDC.cosineSimilarity(bags.get('mentionbag'), mentions) + CDC.cosineSimilarity(bags.get('contextbag'), CDC.contextbag(entity, sentences))        
        if score > best_score:
            best_score = score
            best_k = kandidaats

    if showDisambiguation:
        print 
        print 'Izvēlējāmies ', best_k
        print 

    api.insertMention(best_k, documentId, True, best_score) # TODO - jāieliek arī ne-labākie mentioni
    return best_k

# Savāc visu vajadzīgo lai uztaisītu globālajai entītijai CDC datus
def buildGlobalEntityBags(globalID):
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
            # defaultroletype = getDefaultRole(frame['FrameType'], element['Key'])
            # if (defaultroletype == 'person') or (defaultroletype == 'organization') or (defaultroletype == 'location'):
            #     mentionbag.add(entityID)   
            entity = api.entity_data_by_id(entityID, False)
            if entity['Category'] <= 3 and entityID != globalID: # 1=location 2=organization 3=person
                mentionbag[entity.get('Name')] += 1

    
    if showDisambiguation: # debuginfo    
        print
        print ' ---  Vācam whitelist datus entītijai', globalID, db_info['Name'], db_info['OuterId']
    return {'namebag':namebag, 'mentionbag':mentionbag, 'contextbag':contextbag}

# NB! Šī ir legacy metode ar heiristikām; kodā šobrīd tiek lietota CDC metode ar bag-of-words cosine similarity principu; bet nākotnē varētu gribēties to kombinēt ar šo pieeju, tāpēc kods paliek
# entity: objekts
# matchingEntities: sarasts ar globālajiem id'iem
# Ja entītijai (personai) ir vairāki objekti ar vienādu vārdu, tad izvēlas, kurš no tiem IDiem atbilst labāk
def disambiguateEntity(entity, matchedEntities, entities):
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
        print ' ---  Disambiguācija vārdam', entity['representative'], '(', entity['type'], ')'
        print entity['representative'], entity['aliases'], ": ", len(matchedEntities), " varianti..", matchedEntities

        print 'Dokumenta objekts:'
        for amats in amati:
            print 'Amats:', amats['representative'], '/', amats.get('GlobalID')
        for darbavieta in darbavietas:
            print 'Darbavieta:', darbavieta['representative'], '/', darbavieta.get('GlobalID')
        print

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
        k['category'] = getNETypeName(db_info['Category'])
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
            print 'Kandidāts',i,':',matchedEntities[i],'\tscore:', score
            print '\t', k
            print

    result = best_k['globalID'] # Entītijai ar lielāko score
    if showDisambiguation:
        print 
        print 'Izvēlējāmies ', result
        print 
    return result

# paņem sarakstu formā [{'tokenIndex': 3, 'name': u'Employer'}, {'tokenIndex': 5, 'name': u'Position'}] un pārveido uz Dict no lomas nosaukuma uz entītiju
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
