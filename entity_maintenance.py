#!/usr/bin/env python
# -*- coding: utf8 -*-
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

import sys, json
from collections import defaultdict

sys.path.append("./src")
from db_config import api_conn_info
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection
from DocumentUpload import inflectEntity, orgAliases
from openpyxl import load_workbook

conn = PostgresConnection(api_conn_info, dataset=2)
api = SemanticApiPostgres(conn)

def fetch_entities():
    sql = "select e.entityid, e.name, e.category, e.dataset, e.nameinflections, array_agg(n.name) aliases, min(i.outerid) ids from entities e \
            left outer join entityothernames n on e.entityid = n.entityid \
            left outer join entityouterids i on e.entityid = i.entityid \
            where e.deleted is false \
            group by e.entityid, e.name, e.category, e.dataset, e.nameinflections\
            order by e.dataset, e.category, e.entityid"
    res = api.api.query(sql, None )

    entities = defaultdict(lambda: defaultdict(list)) # multimaps: dataset->category->list of entities there

    for entity in res:
        entities[entity.dataset][entity.category].append(entity)

    for dataset, dataset_entities in entities.items():
        for category, category_entities in dataset_entities.items():
            with open('./entity_fixtures/%s-%d-%d.json' % (api_conn_info.get('dbname'), dataset, category), 'wt') as file_handle:
                file_handle.write('[\n')
                for entity in category_entities:
                    d = entity.__dict__
                    nameinflections = d.get('nameinflections')
                    if nameinflections:
                        try:
                            nameinflections = json.loads(nameinflections)
                        except Exception as e:
                            nameinflections = None
                    d['nameinflections'] = nameinflections
                    file_handle.write('%s,\n' % json.dumps(d, ensure_ascii=False))
                file_handle.write('{}]')

def create_dates():
    m_names = ['janvāris', 'februāris', 'marts', 'aprīlis', 'maijs', 'jūnijs', 'jūlijs', 'augusts', 'septembris', 'oktobris', 'novembris', 'decembris']
    m_names = ['janvāris', 'februāris', 'marts', 'aprīlis', 'maijs', 'jūnijs', 'jūlijs', 'augusts', 'septembris', 'oktobris', 'novembris', 'decembris']

    # def insertEntity(self, name, othernames, category, outerids=[], inflections = None, hidden = False, commit = True):
    for year in range(1900, 2021):
        name = '%d. gads' % year
        aliases = [name, '%d' % year, '%d.' % year, '%d. g' % year, '%d. g.' % year]        
        inflections = inflectEntity(name, 'date')
        api.insertEntity(name, aliases, 6, inflections = inflections, commit = False)
        print(name, aliases, inflections)

        for month in range(1,13):
            name = '%d. gada %s' % (year, m_names[month-1])
            aliases = [name, '%d.%02d' % (year, month), '%d.%02d.' % (year, month)]
            inflections = inflectEntity(name, 'date')
            if month<10:
                aliases.append('%d.%d' % (year, month))
                aliases.append('%d.%d.' % (year, month))
            api.insertEntity(name, aliases, 6, inflections = inflections, commit = False)
            #print(name, aliases, inflections)

    conn.commit()
    print("Dates loaded!")

def load_education():
    education_xls = "/Users/pet/Dropbox/Resursi/Leksiskie dati/izglītības iestādes14082014.xlsm"
    print('Loading excel...')
    book = load_workbook(education_xls, data_only = True)
    sheet = book.get_sheet_by_name(name = "Nosaukumi un alias")
    print('Excel in memory')
    rows = sheet.range("A2:B2000")   #FIXME - hardkodēts range, kurā tajā excelī ir dati

    entitynames = defaultdict(set) # multimaps: entity canonical name -> set of aliases
    for row in rows:
        representative = row[1].value
        alias = row[0].value
        if representative:
            entitynames[representative].add(representative)
            if alias:
                entitynames[representative].add(alias)
                if 'vidusskola' in alias:
                    entitynames[representative].add(alias.replace('vidusskola', 'vsk.'))
                    entitynames[representative].add(alias.replace('vidusskola', 'vsk'))

    for representative, aliases in entitynames.items():
        inflections = inflectEntity(representative, 'organization')
        api.insertEntity(representative, aliases, 2, inflections = inflections, commit = False)
        # print("%s -> %s" % (representative, aliases))
    conn.commit()
    print("Education institutions loaded!")

# Saģenerē dažādus 'apcirptos' aliasus
def aliasname(name):
    result = set()
    name = name.strip()
    result.add(name)
    if name.startswith('"') and name.endswith('"'):
        result.update(aliasname(name[1:-1]))
    if '"' not in name:
        result.add('"' + name + '"')
    if name.startswith('Partija') or name.startswith('partija'):
        result.update(aliasname(name[8:])) # tas pats tikai bez partija sākumā
    if name.startswith('Politiskā partija'):
        result.update(aliasname(name[10:])) # tas pats tikai bez Politiskā sākumā (partija paliek)
    return result


def load_parties():
    party_xls = "/Users/pet/Dropbox/Resursi/Leksiskie dati/Partiju_lielaako_saraksts.xlsx"
    print('Loading excel...')
    book = load_workbook(party_xls, data_only = True)
    sheet = book.get_sheet_by_name(name = "Sheet1")
    print('Excel in memory')
    rows = sheet.range("A2:D204")   #FIXME - hardkodēts range, kurā tajā excelī ir dati

    entitynames = defaultdict(set) # multimaps: entity canonical name -> set of aliases
    last_representative = None
    for row in rows:
        representative = row[0].value
        alias = row[1].value
        abbr1 = row[2].value
        abbr2 = row[3].value
        if representative:
            last_representative = representative 
        else:
            representative = last_representative # ja A kolonna ir tukša, tad tas nozīmē, ka entītija turpinās

        entitynames[representative].update(aliasname(representative))

        if alias:
            entitynames[representative].update(aliasname(alias))
        if abbr1:
            entitynames[representative].add(abbr1)
        if abbr2:
            entitynames[representative].add(abbr2)

    for representative, aliases in entitynames.items():
        inflections = inflectEntity(representative, 'organization')
        api.insertEntity(representative, aliases, 2, inflections = inflections, commit = False)
        #print("%s %s-> %s" % (representative, inflections, aliases))
    conn.commit()
    print("Parties loaded!")

def load_cv_entities(filename):
    with open(filename) as f:
        cv_entities = json.load(f)

    for entity in cv_entities:
        name = entity.get('representative')
        aliases = entity.get('aliases')
        category = 0
        if entity.get('type') == 'organization':
            category = 2
            aliases = set(orgAliases(name)).union(aliases)
        if entity.get('type') == 'person':
            category = 3
        
        
        api.insertEntity(name, aliases, category, outerids=[entity.get('uqid')], inflections = json.dumps(entity.get('inflections'), ensure_ascii=False), commit = False)
    conn.commit()
    print("Entities from file %s loaded!" % filename)

def load_entities(filename):
    with open(filename) as f:
        entities = json.load(f)

    for counter, entity in enumerate(entities):
        name = entity.get('name')
        aliases = set(entity.get('aliases'))
        category = entity.get('category')
        inflections = json.dumps(entity.get('nameinflections'), ensure_ascii=False)
        outerids=[]
        if entity.get('ids'):
            outerids=[entity.get('ids')]
        api.insertEntity(name, aliases, category, outerids=outerids, inflections = inflections, commit = False)
        # print(name, aliases, category, inflections)
        if counter % 100 == 99:
            print('%s\n' % (counter+1,))
            conn.commit()
    conn.commit()
    print("Entities from file %s loaded!" % filename)

def reinflect_entities():
    print('Re-inflecting all entities!')
    #res = api.api.query("select entityid, name from entities e", None )
    #res = api.api.query("SELECT entityid, NAME, categorynameeng FROM entities e JOIN entitycategories c ON e.category = c.categoryid where name in ('Vjačeslavs Stepaņenko', 'Helga Teni')", None )
    res = api.api.query("SELECT entityid, NAME, categorynameeng FROM entities e JOIN entitycategories c ON e.category = c.categoryid where category = 3", None )

    for counter, entity in enumerate(res):
        inflections = inflectEntity(entity.name, entity.categorynameeng)
        api.api.insert("update entities set nameinflections = %s where entityid = %s", (inflections, entity.entityid))
        if counter % 1000 == 999:
            print('%s' % (counter+1,))
            conn.commit()

    conn.commit()
    print('Entity re-inflection done!')


def main():
    print(sys.version)
    print(sys.stdout.encoding)
    # create_dates()
    # load_education()
    # load_parties()
    # load_cv_entities("entity_fixtures/gold/Organizācijas no LETA.json")
    # load_cv_entities("entity_fixtures/gold/Personas no LETA.json")
    # load_entities("entity_fixtures/gold/Vietas no LĢIS.json")
    # load_entities("entity_fixtures/gold/Personas no firmu exceļa.json")
    # load_entities("entity_fixtures/gold/Organizācijas no firmu exceļa.json")
    # fetch_entities()
    reinflect_entities()
    print('Done!')

if __name__ == "__main__":
    main()