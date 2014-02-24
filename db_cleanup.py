#!/usr/bin/env python
# coding=utf-8

import sys, requests
sys.path.append("./src")
from db_config import api_conn_info, inflection_webservice
from SemanticApiPostgres import PostgresConnection, SemanticApiPostgres


fixes = [  # List of name - query tuples
	('Empty entities',	"delete from entities where name = ''"),
	('Person categories 1', "update entities set category = 2 where (name like '%\"%' or name like 'SIA %' or name like 'AS %' or name like 'A\\S %' or name like '% A\\S'  or name like '% AS') and category = 3"),
	('Orgnames "līdz" -> delete', "delete from entities where category=2 and ( lower(name) like '%līdz %' )"),
	('Orgnames visādi amati utml -> hidden', "update entities en set hidden = True where category=2 and dataset <> 3 and hidden = False and (\
										 lower(name)    like '% valde%'   or lower(EN.name) like '% amat%'    or\
										 lower(EN.name) like '% vadītā%'  or lower(EN.name) like '%direktor%' or lower(EN.name) like '%vietniek%' or\
										 lower(EN.name) like '%veidoj%'   or lower(EN.name) like '%čempion%'  or lower(EN.name) like '%iecelt%' OR\
										 lower(EN.name) like '%iecēla%'   OR lower(EN.name) like '%atstāj%'   or lower(EN.name) like '%strādā%' OR\
										 lower(EN.name) like '%strādāja%' OR lower(EN.name) like '%strādājis%' or lower(EN.name) like 'viens%'  or\
										 lower(EN.name) like '% viens%'   or lower(EN.name) like '%minēts%'   OR lower(EN.name) like '%kļuvis%' OR\
										 lower(EN.name) like '%saistībā%' or lower(EN.name) like '%atsaukts%' OR lower(EN.name) like '%pildīt%' OR\
										 lower(EN.name) like '%paziņo%'   or lower(EN.name) like '%piedal%'   or lower(EN.name) like '%bijusi%' OR\
										 lower(EN.name) like '%bijis%'    OR lower(EN.name) like '%būs %'     OR lower(EN.name) like '% ir %'    )"),
	('Entītijas ar prievārdiem pa vidu -> hidden', "update entities EN set hidden = True where EN.category in (2,3) and dataset <> 3 and hidden = False and ( lower(EN.name) like '% no %' OR\
										 lower(EN.name) like '% uz %' OR lower(EN.name) like '% pie %' OR lower(EN.name) like '% ar %' OR lower(EN.name) like '% ap %' OR\
										 lower(EN.name) like '% par %' OR lower(EN.name) like '% kā %' OR lower(EN.name) like 'no %' OR lower(EN.name) like 'uz %' OR\
										 lower(EN.name) like 'pie %' OR lower(EN.name) like 'ap %' OR lower(EN.name) like 'par %' OR lower(EN.name) like 'kā %')"),
	('SIA -> SIS locījumu gļuks1', "update entityothernames set name = overlay(name placing 'A' from 3 for 1) where name like 'SIS %'"),
	('SIA -> SIS locījumu gļuks2', "update entities set name = overlay(name placing 'A' from 3 for 1), nameinflections = null where name like 'SIS %' and category=2"),
	('Liekās entītijas - CV headeri', "delete from entities where category = 9 and (name = 'Politiskā un sabiedriskā darbība' or name = 'Izglītība' or name = 'Karjera' or name = 'Tiesu procesi un skandāli' or name = 'Īpašumi, amatpersonu deklarācijas' or name = 'Personas dati, ģimenes stāvoklis')")
	]

cleanup = [
	('"deleted" entities',	"delete from entities where deleted = True"),
	('all dirtyentities',	"delete from dirtyentities"),
	('all dirtydocuments',	"delete from dirtydocuments"),
	('all dirtytexts',	"delete from dirtytexts")
]

orphans = [
	('Orphan cdc_wordbags',		"delete from cdc_wordbags where not exists (select * from entities where entities.entityid = cdc_wordbags.entityid)"),
	('Orphan entityouterids',	"delete from entityouterids where not exists (select * from entities where entities.entityid = entityouterids.entityid)"),
	('Orphan entityothernames',	"delete from entityothernames where not exists (select * from entities where entities.entityid = entityothernames.entityid)"),
	('Orphan entitymentions1',	"delete from entitymentions where not exists (select * from entities where entities.entityid = entitymentions.entityid)"),
	('Orphan entitymentions2',	"delete from entitymentions where not exists (select * from documents where documents.documentid = entitymentions.documentid)"),
	('Orphan dirtyentities', 	"delete from dirtyentities where not exists (select * from entities where entities.entityid = dirtyentities.entityid)"),
	('Orphan framedata', 		"delete from framedata where not exists (select * from entities where entities.entityid = framedata.entityid)"),
	('Empty frames', 			"delete from frames where not exists (select * from framedata where framedata.frameid = frames.frameid)"),
	('Orphan summaryframeroles', "delete from summaryframeroledata where not exists (select * from entities where entities.entityid = summaryframeroledata.entityid)"),
	('Orphan summaryframedata1', "delete from summaryframedata where not exists (select * from frames where frames.frameid = summaryframedata.frameid)"),
	('Orphan summaryframedata2', "delete from summaryframedata where not exists (select * from summaryframes where summaryframes.frameid = summaryframedata.summaryframeid)"),
	('Empty summaryframes', 	"delete from summaryframes where not exists (select * from summaryframeroledata where summaryframeroledata.frameid = summaryframes.frameid)")
]

def run_queries(queries):
	for query in queries:
		cursor.execute(query[1])
		sys.stderr.write("%s:\t%d rows processed\n" % (query[0], cursor.rowcount) )

def merge_duplicates():
	sql = "select primes.entityid as prime, dups.entityid as duplicate\
			from (\
				  SELECT entityid, name,\
				  ROW_NUMBER() OVER(PARTITION BY name ORDER BY entityid asc) AS Row\
				  FROM entities where dataset <> 3 and deleted = false \
				) dups\
			join (select min(entityid) as entityid, name from entities where dataset <> 3 and deleted = false group by name) primes on dups.name = primes.name\
			where dups.Row > 1"
		# atrodam pārīšus - id_kurš_paliek + id_kurš_jālikvidē
		# dataset <> 3 filtrs - pieļaujam dublikātus tad, ja LETA profilu 'nosaukumos' sakrita vārdi, tie ir 'whitelist cilvēki kuriem vairākiem vienāds vārds.
	cursor.execute(sql)
	sys.stderr.write( 'Merging %d entities ... ' % (cursor.rowcount, ))
	for counter,dup in enumerate(cursor.fetchall()):
		api.mergeEntities(dup.duplicate, dup.prime, False) # katram pārītim pāradresējam visus freimus utml
		if counter % 100 == 99:
			sys.stderr.write('%s\n' % (counter+1, ))
			conn.commit()

	sys.stderr.write( 'done\n')

namefixes = [   #Gļuki 'gold' CV nosaukumos - daļa jau LETA datos, daļa pie regexp parsēšanas
	('AS "Latvo"', 'AS "Latvo'),
	('SIA "Abava"', 'SIA Abava"'),
	('SIA "Araks R"', 'SIA "Araks R'),
	('SIA "Arhitektu birojs Sīlis, Zābers & Kļava"', 'SIA "Arhitektu birojs Sīlis'),
	('SIA "Brenntag Latvia"', 'SIA "Brenntag Latvia'),
	('SIA "Colgate - Palmolive (Latvia)"', 'SIA "Colgate - Palmolive'),
	('SIA "Fix"', 'SIA "Fix'),
	('SIA "Hill + Knowlton Strategies"', 'SIA "Hill'),
	('SIA "JYSK LINNEN`N Furniture"', 'SIA "JYSK LINNEN'),
	('SIA "Kuehne+Nagel"', 'SIA "Kuehne'),
	('SIA "LSF Holdings"', 'SIA "LSF Holdings'),
	('SIA "MOOZ!"', 'SIA "MOOZ'),
	('SIA "Pēterkoks"', 'SIA Pēterkoks"'),
	('SIA "Poligrāfijas Apgāds (PolAp)"', 'SIA "Poligrāfijas Apgāds'),
	('SIA "Procter & Gamble marketing Latvia, Ltd"', 'SIA "Procter & Gamble marketing Latvia'),
	('SIA "Reaton, Ltd"', 'SIA "Reaton'),
	('SIA "Standartizācijas, akreditācijas un metroloģijas centrs"', 'SIA "Standartizācijas')
	]

def fix_names():
	cursor.executemany("update entities set name = %s where name = %s", namefixes)
	cursor.executemany("update entityothernames set name = %s where name = %s", namefixes)

# Atrodam entītijas kam nav nameinflections un izlokam tās
def missing_inflections():
	cursor.execute("select entityid, name, categorynameeng as category from entities e join entitycategories c on e.category = c.categoryid where nameinflections is null")
	sys.stderr.write( 'Generating inflections for %d entities ... ' % (cursor.rowcount, ))
	for counter, entity in enumerate(cursor.fetchall()):
		r = requests.get('http://%s:%d/inflect_phrase/%s?category=%s' % (inflection_webservice.get('host'), inflection_webservice.get('port'), entity.name, entity.category) ) 
		# http://ezis.ailab.lv:8182/inflect_phrase/SIA%20%22Cirvis%22?category=organization
		cursor.execute("update entities set nameinflections = %s where entityid = %s", (r.text, entity.entityid) )
		if counter % 100 == 99:
			sys.stderr.write('%s\n' % (counter+1,))
			conn.commit()
	sys.stderr.write( 'done\n')

# ------ main ----
sys.stderr.write( 'Starting...\n')

conn = PostgresConnection(api_conn_info)
api = SemanticApiPostgres(conn)
cursor = conn.new_cursor()
run_queries(fixes)
fix_names()
merge_duplicates()
run_queries(cleanup)
missing_inflections()
run_queries(orphans)

cursor.close()
conn.commit()
sys.stderr.write( 'Done!\n')
