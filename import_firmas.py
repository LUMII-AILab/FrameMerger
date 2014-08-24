#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys, re
sys.path.append("./src")
from openpyxl import load_workbook
from collections import namedtuple
from db_config import api_conn_info
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection
from DocumentUpload import personAliases, inflectEntity, orgAliases

realUpload = True # Vai lādēt DB pa īstam - lai testu laikā nečakarē DB datus

conn = PostgresConnection(api_conn_info, dataset=3)
api = SemanticApiPostgres(conn)

# Organizāciju nosaukumu tīrīšana, kas nav vispārīga, bet tieši šim dīvainajam excelim
def cleanOrgName(name):
    fixname = name.strip()
    fixname = re.sub(ur'  +', ' ', fixname, re.UNICODE)
    nodarboshanaas = [u'[Vv]eikals ?(-salons|-noliktava|-darbnīca| ?-serviss|-bāze|-kafejnīca|-bārs|-ateljē)?', u'Komisijas veikals', u'Valūtas maiņas punkts', 'Birojs',
        u'Veļas mazgātava', u'Gludinātava', u'Solārijs', u'Solārij[au] studija', u'Fotoskola', u'Studija', 'Arhitektu birojs', u'Projektēšanas birojs', 'Projektu birojs',
        u'Metāllūžņu iepirkšanas punkts', u'Juridiskais birojs', 'Muitas noliktava', u'Birojs-noliktava', u'[rR]eklāmas aģentūra',
        u'[Ll]aikraksts', u'Tipogrāfija', u'Izdevniecība', u'Grāmatu salons', u'Apgāds',
        u'Dekoratīvā apdare', ur'Salons(-veikals|-darbnīca)?', u'Ražotne', u'Galdniecība', u'Kokapstrādes cehs', u'Kokzāģētava', u'Darbnīca(-veikals)?', u'Ateljē(-veikals)?', u'Alus darītava',
        u'Vairumtirdzniecības bāze', u'Noliktava', u'Tirgus', u'Tirdzniecības (komplekss|vieta|centrs)', u'Tirgotava',
        u'Viesu (nams|māja)', u'[Aa]tpūtas (centrs|bāze|komplekss|vieta|māja)', u'(Lauku|[Bb]rīvdienu) (māja|sēta)', u'(Parketa|Aizkaru) salons', u'Mācību centrs', u'Jogas centrs', u'Pilotu skola', 
        u'Interneta portāls', 'Internet(a )?v[ae]ikals', u'Iepirkšanās portāls', u'Interneta klubs',
        u'Aptieka', u'Zāļu lieltirgotava', u'Privātklīnika', u'(Veselības|Medicīnas) centrs', ur'Veterinārā (klīnika|aptieka|klīnika-aptieka|ambulance)', u'Zooveikals',
        u'Klīnika', u'Veselības centrs', u'Zobārstniecības (kabinets|klīnika)', u'Zobu tehniskā laboratorija', 'Zobu tehnika', 'Laboratorija', u'Ārstu prakse', u'Vakcinācijas centrs',
        u'Ambulatorā klīnika', u'Uroloģijas ambulance', u'Cilmes šūnu banka', u'Medicīniskās rehabilitācijas centrs', u'Doktorāts',
        u'Kiosks', u'Degvielas uzpildes stacija( Statoil)?', u'Viesnīca', 'Hostelis', 'Motelis', u'Dienesta viesnīca',
        u'Auto gāzes uzpildes stacija', u'Dienas stacionārs', u'Veļas mazgātava un ķīmiskā tīrītava', u'Ķīmiskā tīrītava', u'Ferma',
        u'Ēdnīca', u'Kafejnīca', u'Tējnīca', u'Restorāns', u'Serviss(-veikals)?', u'Riepu serviss', u'Servisa centrs', u'Autoserviss(-veikals)?', u'Autocentrs', u'Autosalons(-autoserviss)?',
        u'([Tt]ūrisma|Ceļojumu) (aģentūra|birojs|operators|komplekss|firma|klubs)', u'Kokaudzētava', u'Stād(u )?audzētava', u'Dārzniecība',
        u'Taksometru (pakalpojumi|dienests)', u'Kaljana veikals', u'Namu pārvalde', u'Datorserviss', u'Datoru centrs', u'[Aa]viokompānija', u'[Aa]uditorfirma',
        u'Konditoreja(s cehs)?', u'Ceptuve', u'Maizes Ceptuve', u'Beķereja', u'Dzirnavas', 'Kautuve', u'Gaļas kombināts', u'Gaļas pārstrādes cehs', u'Veselīgas vides saimniecība',
        u'Slēpošanas trase', u'Trenažieru zāle', u'Peldbaseins', u'Slidotava', u'Sporta (centrs|klubs)', u'[Peintbola|Atpūtas] parks', 'Tenisa korti', 'Karting[au] (trase|halle)', 'Ledus halle']

    fixname = re.sub(ur', (%s)$' % '|'.join(nodarboshanaas), '', fixname, re.UNICODE)
    fixname = re.sub(ur'(", SIA), [\w\sāčēģīķļņšūžČĒĢĪĶĻŅŠŪŽ\-]+$', ur'\1', fixname, re.UNICODE)  
    fixname = re.sub(ur'[Zz]vejniek[ua] saimniecība', u'Zvejnieka saimniecība', fixname, re.UNICODE)
    fixname = re.sub(ur'([Ll]auksaimniecības pakalpojumu|piensaimnieku|bezpeļņas|zemnieku) kooperatīvā sabiedrība', u'kooperatīvā sabiedrība', fixname, re.UNICODE)
    fixname = re.sub(ur'pilnsabiedrība', u'Pilnsabiedrība', fixname, re.UNICODE)
    fixname = re.sub(ur'individuālais uzņēmums', u'Individuālais uzņēmums', fixname, re.UNICODE)
    fixname = re.sub(ur'", Si[Aa]|" SIA|",SIA', u'", SIA', fixname, re.UNICODE)
    return fixname
    
def validOrg(name):
    if re.search(ur' (individuālā darba veicēj[as]|individuālais darbs|IK|viesu nams|lauku māja|fizioterapeite|eksperts|\.lv|tehniskaisjuriskonsults|juridiskais birojs|administrator[se]|((zob)?ārsta|zobārstniecības|zobu (higiēnistes|tehniķa)|veterinārā|arhitekta|arhitektes|jurista|juristes) (privāt)?prakse|veterinārārst[es]|dziednie(ks|ce))$', name, re.UNICODE):  # Individuālos komersantus neimportējam
        return False
    elif re.search(ur'(zvērināt[as])? (advokāt|notār|mērniek)[aeu]s?( birojs| privātprakse)?$', name, re.UNICODE):
        return False
    elif re.search(ur'ārsta[\w\s\-āčēģīķļņšūž]+prakse[\w\s\-āčēģīķļņšūž]*$', name, re.UNICODE):
        return False
    elif '"' not in name and ('prakse' in name or u'dvokāt' in name or u' mērnie' in name or u'notārs' in name):
        return False
    else:
        return True

# Ielādējam firmu exceli uz dict'u sarakstu
def load_firmas(filename):
    print('Loading excel...')
    book = load_workbook(filename)
    sheet = book.get_sheet_by_name(name = "Sheet1")
    print('Excel in memory')
    rows = sheet.range("A2:E39308")   #FIXME - hardkodēts range, kurā tajā excelī ir dati
    #rows = sheet.range("A2:E13")   #FIXME - hardkodēts range, kurā tajā excelī ir dati

    Firma = namedtuple('Firma', ['uid','name','nozare','person','position'])
    firmas = []
    personas = set()

    for counter, row in enumerate(rows):
        firma = Firma(row[0].value, row[1].value, row[2].value, row[3].value, row[4].value)
        
        name = cleanOrgName(firma.name)
        if not validOrg(name):
            continue

        aliases = orgAliases(name)
        name = aliases[0]

        existing_ids = api.entity_ids_by_name_list(name)
        if existing_ids:
            continue # Šāda organizaacija jau ir DB

        inflections = inflectEntity(name, 'org')
        if realUpload:
            api.insertEntity(name, aliases, category = 2, outerids = [firma.uid], inflections = inflections, commit=False)
        else:
            print('\n\t'.join(aliases))
        if counter % 100 == 99:
            print('%s\n' % (counter+1,))
            conn.commit()

        personas.add(firma.person)

    print "Firmas ieliktas"

    # print(len(personas))
    # print(len(firmas))

    for counter, person in enumerate(list(personas)):
        if not person or person == 'NULL':
            continue

        if '(' in person:
            print "Skipping %s" % person
            continue

        existing_ids = api.entity_ids_by_name_list(person)
        if existing_ids:
            continue # Šāda persona jau ir DB
        
        # print("%s: %d" % (person, len(existing_ids)))
        aliases = personAliases(person)
        inflections = inflectEntity(person, 'person')
        if realUpload:
            api.insertEntity(person, aliases, category = 3, inflections = inflections, commit=False)
        else:
            print(', '.join(aliases))
        if counter % 100 == 99:
            sys.stderr.write('%s\n' % (counter+1,))
            conn.commit()

# pārlaiž jauno (labāko?) aliasu ģenerēšanu
def update_preloaded_org_names():
    q = "select entityid from entities where dataset = 3 and category = 2"
    res = api.api.query(q, None )
    entity_ids = [x[0] for x in res]

    for entity_id in entity_ids:
        entity = api.entity_data_by_id(entity_id)
        name = entity.get(u'Name') 
        aliases = set()
        for othername in entity.get(u'OtherName'):
            aliases.add(othername)
        for newalias in orgAliases(name):
            aliases.add(newalias)
        if realUpload:
            api.updateEntity(entity_id, name, aliases, entity.get('Category'), entity.get('OuterId'), entity.get('NameInflections'))

def update_preloaded_person_names():
    q = "select entityid from entities where dataset = 3 and category = 3"
    res = api.api.query(q, None )
    entity_ids = [x[0] for x in res]

    for counter, entity_id in enumerate(entity_ids):
        entity = api.entity_data_by_id(entity_id)
        name = entity.get(u'Name') 
        aliases = set()
        for othername in entity.get(u'OtherName'):
            aliases.add(othername)
        for newalias in personAliases(name):
            aliases.add(newalias)
        inflections = inflectEntity(name, 'person')
        if realUpload:
            api.updateEntity(entity_id, name, aliases, entity.get('Category'), entity.get('OuterId'), inflections, commit = False)
        if counter % 100 == 99:
            sys.stderr.write('%s\n' % (counter+1,))
            conn.commit()

def main():
    print sys.stdout.encoding
    print 
    #load_firmas('./input/firmu nosaukumi.xlsx')
    #update_preloaded_org_names()
    update_preloaded_person_names()
    print 'Done!'


if __name__ == "__main__":
    main()