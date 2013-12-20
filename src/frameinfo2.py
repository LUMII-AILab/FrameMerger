#!/usr/bin/env python
# coding=utf-8

# Mapping between frame names used in JSON; and the numeric codes in the DB (0-based index)

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
        sys.stderr.write("Nesaprasts freima tips '"+name+"'")
        return 0

def getFrameName (code):
    try:
        name = frameTypes[code]
    except IndexError:
        sys.stderr.write("Mēģinam dabūt vārdu freimam ar nelabu numuru"+str(code))
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
    ["Participant_1", "Event", "Time", "Place", "Manner", "Insititution"], # NB! typo insitution didža datos
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
    if name in frameElements[frameCode]:
        return frameElements[frameCode].index(name)+1 # +1 jo Freimu lomas numurējas no 1
    else:
        sys.stderr.write("Freimā '"+frameTypes[frameCode]+"' nesaprasts elements '"+name+"'")
        return 0

def getElementName(frameCode, elementCode):
    try:
        name = frameElements[frameCode][elementCode-1] # -1 jo Freimu lomas numurējas no 1
    except IndexError:
        sys.stderr.write("Hmm mēģinam dabūt vārdu elementam ar nelabu numuru"+elementCode+"freimā"+frameCode)
        name = ""
    return name

NETypeCodes = {
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
    'event' : 14 }

def getNETypeCode (name):
    if name is None: 
        sys.stderr.write("Prasam NE tipu priekš None...")
        return 0
    code = NETypeCodes.get(name)
    if code is None:
        sys.stderr.write("Nesaprasts NE tips '"+name+"'")
        return 0
    else:        
        return code

def getNETypeName(code):
    for name in NETypeCodes:
        if NETypeCodes[name] == code:
            return name
    print "Hmm nesanāca dabūt vārdu entītijas tipam ar kodu ", code
    return ""

__roleDefaultNETypes__ = [  # ja NE nav neko ielicis, bet freima elements uz šo norāda - kāda ir defaultā NE kategorija. reizēm var būt gan persona gan organizācija.. bet nu cerams tos NER palīdzēs.
    ['person', 'time', 'location', 'relatives'],
    ['person', 'sum'],
    ['person', 'time', 'location', 'descriptor', 'descriptor'],
    ['person', 'person', 'relatives', 'relationship', 'time'],
    ['person', 'person', 'descriptor'],
    ['person', 'location', 'descriptor', 'time'], 
    ['person', 'organization', 'descriptor', 'descriptor', 'time', 'location'], # jāsaprot par grādiem - vai plikus deskriptorus vai ko precīzāku...
    ['person', 'profession', 'time', 'descriptor'],
    ['location', 'person', 'descriptor'],
    ['person', 'organization', 'profession', 'sum', 'location', 'time', 'descriptor', 'time', 'time'],
    ['person', 'organization', 'profession', 'person', 'descriptor', 'location', 'time', 'person'],
    ['person', 'organization', 'profession', 'person', 'descriptor', 'location', 'time', 'person'], 
    ['person', 'organization', 'time', 'descriptor'],
    ['person', 'organization', 'profession', 'person', 'descriptor', 'time', 'location'], 
    ['person', 'organization', 'descriptor', 'time'],
    ['organization', 'person', 'descriptor', 'profession', 'time', 'location'], # organizāciju nozares sintaktiski ir līdzīgas profesijām bet sarakstu varbūt jāliek citu
    ['person', 'descriptor', 'time', 'location', 'descriptor', 'organization'], 
    ['organization', 'sum', 'descriptor', 'sum', 'time', 'descriptor', 'descriptor'], 
    ['person', 'descriptor', 'time', 'descriptor'],
    ['organization', 'organization', 'descriptor', 'descriptor', 'time', 'descriptor'], 
    ['person', 'descriptor', 'organization', 'person', 'person', 'person', 'location', 'time'], 
    ['person', 'person', 'descriptor', 'descriptor', 'descriptor', 'descriptor', 'descriptor', 'descriptor', 'location', 'time'], 
    ['person', 'descriptor', 'descriptor', 'descriptor', 'descriptor', 'time', 'location', 'organization', 'person'], 
    ['organization', 'person', 'descriptor', 'time'],
    ['organization', 'descriptor', 'sum', 'descriptor', 'organization', 'descriptor', 'time'], 
    ['descriptor', 'organization', 'descriptor']]


def getDefaultRole(frameCode, elementCode):
    try:
        role = __roleDefaultNETypes__[frameCode][elementCode-1] # -1 jo freimu lomas numurējas no 1
    except IndexError:
        print "Hmm mēģinam dabūt defaulto lomu elementam ar nelabu numuru", elementCode, "freimā", frameCode
        role = ''
    return role
