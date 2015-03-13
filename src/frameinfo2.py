#!/usr/bin/env python
# coding=utf-8
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

# Mapping between frame names used in JSON; and the numeric codes in the DB (0-based index)
import sys

frameTypes = [
    "Being_born",
    "People_by_age",
    "Death",
    "Personal_relationship",
    "Being_named",
    "Residence",
    "Education_teaching",
    "People_by_vocation",
    "People_by_origin",
    "Being_employed",
    "Hiring",
    "Employment_end",
    "Membership",
    "Change_of_leadership",
    "Giving",
    "Intentionally_create",
    "Participation",
    "Earnings_and_losses",
    "Possession",
    "Lending",
    "Trial",
    "Attack",
    "Win_prize",
    "Statement",
    "Public_procurement", 
    "Product_line",
    "Unstructured"]

def getFrameType (name):
    if name in frameTypes:
        return frameTypes.index(name)
    else:
        sys.stderr.write("Nesaprasts freima tips '"+name+"'\n")
        return 0

def getFrameName (code):
    try:
        name = frameTypes[code]
    except IndexError:
        sys.stderr.write("Mēģinam dabūt vārdu freimam ar nelabu numuru"+str(code)+"\n")
        name = ""
    return name

# Frame elements should have 1-based index !!
frameElements = [
    ["Child", "Time", "Place", "Relatives"],
    ["Person", "Age"],
    ["Protagonist", "Time", "Place", "Manner", "Cause"],
    ["Partner_1", "Partner_2", "Partners", "Relationship", "Time"],
    ["Name", "Entity", "Type"],
    ["Resident", "Location", "Frequency", "Time"],
    ["Student", "Institution", "Subject", "Qualification", "Time", "Place"],
    ["Person", "Vocation", "Time", "Descriptor"],
    ["Origin", "Person", "Ethnicity"],
    ["Employee", "Employer", "Position", "Compensation", "Place_of_employment", "Time", "Manner", "Employment_start", "Employment_end"],
    ["Employee", "Employer", "Position", "Appointer", "Manner", "Place", "Time", "Previous_employee"],
    ["Employee", "Employer", "Position", "Appointer", "Manner", "Place", "Time", "Future_employee"], 
    ["Member", "Group", "Time", "Standing"],
    ["Candidate", "Body", "Role", "New_leader", "Result", "Time", "Place"], 
    ["Donor", "Recipient", "Theme", "Time"],
    ["Created_entity", "Creator", "Manner", "Industry", "Time", "Place"], 
    ["Participant_1", "Event", "Time", "Place", "Manner", "Institution"], 
    ["Earner", "Earnings", "Goods", "Profit", "Time", "Unit", "Growth"], 
    ["Owner", "Possession", "Time", "Share"],
    ["Borrower", "Lender", "Theme", "Collateral", "Time", "Units"], 
    ["Defendant", "Charges", "Court", "Prosecutor", "Lawyer", "Judge", "Place", "Time"], 
    ["Victim", "Assailant", "Result", "Circumstances", "Depictive", "Reason", "Weapon", "Manner", "Place", "Time"], 
    ["Competitor", "Prize", "Competition", "Result", "Rank", "Time", "Place", "Organizer", "Opponent"],
    ["Medium", "Speaker", "Message", "Time"],
    ["Institution", "Theme", "Expected_amount", "Candidates", "Winner", "Result", "Time"], 
    ["Brand", "Institution", "Products"],
    ["Entity","Property","Category"]]

def getElementCode(frameCode, name):
    if name=="Insititution": 
        name = "Institution" # NB! typo didža datos

    if name in frameElements[frameCode]:
        return frameElements[frameCode].index(name)+1 # +1 jo Freimu lomas numurējas no 1; 0-tā - targetvārds
    else:
        sys.stderr.write("Freimā '"+str(frameTypes[frameCode])+"' nesaprasts elements '"+str(name)+"'\n")
        return 0

def getElementName(frameCode, elementCode):
    try:
        name = frameElements[frameCode][elementCode-1] # -1 jo Freimu lomas numurējas no 1
    except IndexError:
        sys.stderr.write("Hmm mēģinam dabūt vārdu elementam ar nelabu numuru "+str(elementCode)+" freimā "+str(frameCode)+"\n")
        name = ""
    return name

entityTypeCodes = {
    'location': 1,
    'organization': 2,
    'person': 3,
    'profession': 4,
    'sum': 5,
    'time': 6,
    'relationship': 7,
    'qualification': 8,
    'descriptor': 9,
    'relatives': 10,
    'prize': 11,
    'media': 12 ,
    'product': 13,
    'event' : 14,
    'industry' : 15}

def getEntityTypeCode (name):
    if name is None: 
        # sys.stderr.write("Prasam NE tipu priekš None...")
        return 0
    if name == 'organizations':
        name = 'organization' # TODO - salabo reālu situāciju datos, taču nav skaidrs kurā brīdī tādi bugaini dati tika izveidoti.
    code = entityTypeCodes.get(name)
    if code is None:
        sys.stderr.write("Nesaprasts NE tips '"+name+"'\n")
        return 0
    else:        
        return code

def getEntityTypeName(code):
    for name in EnityTypeCodes:
        if entityTypeCodes[name] == code:
            return name
    sys.stderr.write("Hmm nesanāca dabūt vārdu entītijas tipam ar kodu "+str(code)+"\n")
    return ""

elementDefaultEntityTypes = [  # ja NE nav neko ielicis, bet freima elements uz šo norāda - kāda ir defaultā NE kategorija. reizēm var būt gan persona gan organizācija.. bet nu cerams tos NER palīdzēs.
    ['person', 'time', 'location', 'relatives'],        #Dzimšana/Being_born
    ['person', 'sum'],                                  #Vecums/People_by_age
    ['person', 'time', 'location', 'descriptor', 'descriptor'], #Miršana/Death
    ['person', 'person', 'descriptor', 'relationship', 'time'],  #Attiecības/Personal_relationship
    ['person', 'person', 'descriptor'],                 #Vārds/Being_named
    ['person', 'location', 'descriptor', 'time'],    #Dzīvesvieta/Residence
    ['person', 'organization', 'descriptor', 'qualification', 'time', 'location'], #Izglītība/Education_teaching # jāsaprot par grādiem - vai plikus deskriptorus vai ko precīzāku...
    ['person', 'profession', 'time', 'descriptor'],     #Nodarbošanās/People_by_vocation
    ['location', 'person', 'descriptor'],               #Izcelsme/People_by_origin
    ['person', 'organization', 'profession', 'sum', 'location', 'time', 'descriptor', 'time', 'time'],  #Amats/Being_employed
    ['person', 'organization', 'profession', 'person', 'descriptor', 'location', 'time', 'person'],     #Darba_sākums/Hiring
    ['person', 'organization', 'profession', 'person', 'descriptor', 'location', 'time', 'person'],     #Darba_beigas/Employment_end
    ['person', 'organization', 'time', 'descriptor'],   #Dalība/Membership
    ['person', 'organization', 'profession', 'organization', 'descriptor', 'time', 'location'],   #Vēlēšanas/Change_of_leadership
    ['person', 'organization', 'descriptor', 'time'],   #Atbalsts/Giving
    ['organization', 'person', 'descriptor', 'industry', 'time', 'location'], #Dibināšana/Intentionally_create # organizāciju nozares sintaktiski ir līdzīgas profesijām bet sarakstu varbūt jāliek citu
    ['person', 'event', 'time', 'location', 'descriptor', 'organization'], #Piedalīšanās/Participation
    ['organization', 'sum', 'descriptor', 'sum', 'time', 'descriptor', 'descriptor'],   #Finanses/Earnings_and_losses
    ['person', 'descriptor', 'time', 'descriptor'], #Īpašums/Possession
    ['organization', 'organization', 'sum', 'descriptor', 'time', 'descriptor'], #Parāds/Lending
    ['person', 'descriptor', 'organization', 'person', 'person', 'person', 'location', 'time'], #Tiesvedība/Trial
    ['person', 'person', 'descriptor', 'descriptor', 'descriptor', 'descriptor', 'descriptor', 'descriptor', 'location', 'time'],   #Uzbrukums/Attack
    ['person', 'prize', 'event', 'descriptor', 'prize', 'time', 'location', 'organization', 'person'],  #Sasniegums/Win_prize
    ['organization', 'person', 'descriptor', 'time'],   #Ziņošana/Statement
    ['organization', 'descriptor', 'sum', 'organization', 'organization', 'descriptor', 'time'],    #Publisks_iepirkums/Public_procurement
    ['descriptor', 'organization', 'product'],  #Zīmols/Product_line
    ['person','descriptor','descriptor']]   #Nestrukturēts/Unstructured

def getDefaultEnityType(frameCode, elementCode):
    try:
        type = elementDefaultEntityTypes[frameCode][elementCode-1] # -1 jo freimu lomas numurējas no 1
    except IndexError:
        sys.stderr.write("Hmm, mēģinām dabūt defaulto lomu elementam ar nelabu numuru "+str(elementCode)+" freimā "+str(frameCode)+"\n")
        type = ''
    return type

elementPlausibleEntityTypes = [
    [['person'], ['time'], ['location'], ['person', 'relatives']],  #Dzimšana/Being_born
    [['person'], ['sum', 'descriptor']],                            #Vecums/People_by_age
    [['person', 'organization', 'media'],
        ['time'], ['location'], ['descriptor'], ['descriptor']],    #Miršana/Death
    [['person', 'organization', 'media'],
        ['person', 'organization', 'media'],
        ['person', 'organization', 'media', 'relatives', 'descriptor'],
        ['relationship'], ['time']],                                #Attiecības/Personal_relationship
    [['person', 'organization', 'media'],
        ['person', 'organization', 'media'],
        ['descriptor']],                                            #Vārds/Being_named
    [['person', 'organization', 'media'],
        ['location'], ['descriptor'], ['time']],                    #Dzīvesvieta/Residence
    [['person'], ['organization'], 
        ['profession', 'descriptor'],
        ['qualification'], ['time'], ['location']],     #Izglītība/Education_teaching # jāsaprot par grādiem - vai plikus deskriptorus vai ko precīzāku...
    [['person', 'organization', 'media'],
        ['profession', 'industry', 'descriptor'],
        ['time'], ['descriptor']],                      #Nodarbošanās/People_by_vocation
    [['location'],
        ['person', 'organization', 'media'],
        ['descriptor']],                                #Izcelsme/People_by_origin
    [['person'],
        ['organization', 'media'],
        ['profession'],
        ['sum', 'descriptor'],
        ['location'], ['time'], ['descriptor'], ['time'], ['time']],    #Amats/Being_employed
    [['person'],
        ['organization', 'media'],
        ['profession'],
        ['person', 'organization'],
        ['descriptor'], ['location'], ['time'], ['person']],            #Darba_sākums/Hiring
    [['person'],
        ['organization', 'media'],
        ['profession'],
        ['person', 'organization'],
        ['descriptor'], ['location'], ['time'], ['person']],            #Darba_beigas/Employment_end
    [['person', 'organization', 'media'],
        ['organization'], ['time'], ['descriptor']],    #Dalība/Membership
    [['person', 'organization'],
        ['organization'], ['profession'], ['organization'],
        ['descriptor'], ['time'], ['location']],        #Vēlēšanas/Change_of_leadership # Uzvarētājs patiesībā ir saraksts
    [['person', 'organization', 'media'],
        ['person', 'organization', 'media'],
        ['sum', 'descriptor'],
        ['time']],                                  #Atbalsts/Giving
    [['organization', 'media'],
        ['person', 'organization', 'media'],
        ['descriptor'], ['industry'], ['time'], ['location']],    #Dibināšana/Intentionally_create # organizāciju nozares sintaktiski ir līdzīgas profesijām bet sarakstu varbūt jāliek citu
    [['person', 'organization', 'media'],
        ['event'], ['time'], ['location'], ['descriptor'],
        ['person', 'organization', 'media']],       #Piedalīšanās/Participation
    [['person', 'organization', 'media'],
        ['sum'], ['descriptor'], ['sum'], ['time'], ['descriptor'],
        ['sum', 'descriptor']],                     #Finanses/Earnings_and_losses
    [['person', 'organization', 'media'],
        ['organization', 'media', 'descriptor'],
        ['time'],
        ['sum', 'descriptor']],                     #Īpašums/Possession
    [['person', 'organization', 'media'],
        ['person', 'organization', 'media'],
        ['sum'], ['descriptor'], ['time'], ['descriptor']], #Parāds/Lending
    [['person', 'organization', 'media'],
        ['descriptor'], ['organization'],
        ['person', 'organization', 'media'],
        ['person', 'organization'],
        ['person'], ['location'], ['time']],       #Tiesvedība/Trial 
    [['person', 'organization', 'media'],
        ['person', 'organization'],
        ['descriptor'], ['descriptor'], ['descriptor'], ['descriptor'],
        ['product', 'descriptor'],
        ['descriptor'], ['location'], ['time']],   #Uzbrukums/Attack
    [['person', 'organization', 'media'],
        ['prize'], ['event'], ['descriptor'], ['prize'], ['time'], ['location'],
        ['person', 'organization', 'media'],
        ['person', 'organization', 'media']],       #Sasniegums/Win_prize
    [['organization', 'media'], ['person'], ['descriptor'], ['time']], #Ziņošana/Statement
    [['organization', 'media'],
        ['product', 'descriptor'],
        ['sum'],
        ['person', 'organization', 'media'],
        ['person', 'organization', 'media'],
        ['descriptor'], ['time']],                  #Publisks_iepirkums/Public_procurement
    [['product', 'descriptor'], ['person', 'organization', 'media'], ['product']],  #Zīmols/Product_line
    [['person'], ['descriptor'], ['descriptor']]]   #Nestrukturēts/Unstructured

def getPlausibleTypes(frameCode, elementCode):
    try:
        types = elementPlausibleEntityTypes[frameCode][elementCode-1] # -1 jo freimu lomas numurējas no 1
    except IndexError:
        sys.stderr.write("Mēģina dabūt lomai pieļaujamos entīšu tipus elementam ar nelabu numuru "+str(elementCode)+" freimā "+str(frameCode)+"\n")
        types = []
    return types
# Freimiem, kas ir kopīgi gan organizācijām, viena loma parasti nosaka, vai
# konkrētais freims ir par personu vai organizāciju, un šīs lomas entītes tips
# tālāk ļauj spriest par piemērotākajiem pārējo lomu entīšu tipiem.
# Šeit ir tās "noteicošās" lomas apkopotas.
determinerElement = [
    None,           #Dzimšana/Being_born
    None,           #Vecums/People_by_age
    None,           #Miršana/Death
    None,           #Attiecības/Personal_relationship - abi elementi nav vienādi
    "Entity",       #Vārds/Being_named - Entity un Name tipi ir vienādi
    None,           #Dzīvesvieta/Residence
    None,           #Izglītība/Education_teaching
    "Person",       #Nodarbošanās/People_by_vocation - Person nosaka Vocation tipu
    None,           #Izcelsme/People_by_origin
    None,           #Amats/Being_employed
    None,           #Darba_sākums/Hiring
    None,           #Darba_beigas/Employment_end
    None,           #Dalība/Membership
    None,           #Vēlēšanas/Change_of_leadership
    None,           #Atbalsts/Giving - abi elementi ir neatkarīgi
    None,           #Dibināšana/Intentionally_create
    None,           #Piedalīšanās/Participation - abi elementi ir neatkarīgi
    None,           #Finanses/Earnings_and_losses
    None,           #Īpašums/Possession
    None,           #Parāds/Lending
    None,           #Tiesvedība/Trial
    None,           #Uzbrukums/Attack 
    "Competitor",   #Sasniegums/Win_prize - Competitor un Opponent tipi ir vienādi
    None,           #Ziņošana/Statement
    None,           #Publisks_iepirkums/Public_procurement
    None,           #Zīmols/Product_line
    None]           #Nestrukturēts/Unstructured

def getDeterminerRole (frameCode):
    try:
        role = determinerElement[code]
    except IndexError:
        sys.stderr.write("Mēģinam dabūt vārdu freimam ar nelabu numuru"+str(code)+"\n")
        role = None
    return role
 
    