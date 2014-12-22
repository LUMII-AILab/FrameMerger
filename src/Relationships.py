#!/usr/bin/env python
#coding=utf-8
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.
from __future__ import unicode_literals
from __future__ import print_function

import sys, json
from DocumentUpload import inflectEntity

inverted_relations_text = {
'brālis' : {'male' : 'brālis', 'female' : 'māsa'},
'māsa' : {'male' : 'brālis', 'female' : 'māsa'},
'dēls' : {'male' : 'tēvs', 'female' : 'māte'},
'meita' : {'male' : 'tēvs', 'female' : 'māte'},
'mazdēls' : {'male' : 'vectēvs', 'female' : 'vecmamma'},
'mazmeita' : {'male' : 'vectēvs', 'female' : 'vecmamma'},
'māte' : {'male' : 'dēls', 'female' : 'meita'},
'tēvs' : {'male' : 'dēls', 'female' : 'meita'},
'precējies' : {'male' : 'precējies', 'female' : 'precējusies'},
'precējusies' : {'male' : 'precējies', 'female' : 'precējusies'},
'vecmamma' : {'male' : 'mazdēls', 'female' : 'mazmeita'},
'vectēvs' : {'male' : 'mazdēls', 'female' : 'mazmeita'},
'vīrs' : {'male' : 'vīrs', 'female' : 'sieva'},
'sieva' : {'male' : 'vīrs', 'female' : 'sieva'},
'šķīries' : {'male' : 'šķīries', 'female' : 'šķīrusies'},
'šķīrusies' : {'male' : 'šķīries', 'female' : 'šķīrusies'}
}

secondary_relations_text = {
'brālis' : {
	'brālis' : {'result' : 'brālis', 'blessed' : True},
	'māsa' : {'result' : 'māsa', 'blessed' : True},
	'dēls' : {'result' : 'brāļadēls', 'blessed' : True},
	'meita' : {'result' : 'brāļameita', 'blessed' : True},
	'precējies' : {'result' : 'brāļasieva', 'blessed' : True},
	'sieva' : {'result' : 'brāļasieva', 'blessed' : True},
	'tēvs' : {'result' : 'tēvs', 'blessed' : False},
	'māte' : {'result' : 'māte', 'blessed' : False},
	'vectēvs' : {'result' : 'vectēvs', 'blessed' : False},
	'vecmamma' : {'result' : 'vecmamma', 'blessed' : False}
},
'māsa' : {
	'brālis' : {'result' : 'brālis', 'blessed' : True},
	'māsa' : {'result' : 'māsa', 'blessed' : True},
	'dēls' : {'result' : 'māsasadēls', 'blessed' : True},
	'meita' : {'result' : 'māsasmeita', 'blessed' : True},
	'precējusies' : {'result' : 'māsasvīrs', 'blessed' : True},
	'vīrs' : {'result' : 'māsasvīrs', 'blessed' : True},
	'tēvs' : {'result' : 'tēvs', 'blessed' : False},
	'māte' : {'result' : 'māte', 'blessed' : False},
	'vectēvs' : {'result' : 'vectēvs', 'blessed' : False},
	'vecmamma' : {'result' : 'vecmamma', 'blessed' : False}
},
'dēls' : {
	'brālis' : {'result' : 'dēls', 'blessed' : False},
	'māsa' : {'result' : 'meita', 'blessed' : False},
	'dēls' : {'result' : 'mazdēls', 'blessed' : True},
	'meita' : {'result' : 'mazmeita', 'blessed' : True},
	'precējies' : {'result' : 'vedekla', 'blessed' : True},
	'sieva' : {'result' : 'vedekla', 'blessed' : True},
	'tēvs' : {'result' : 'vīrs', 'blessed' : False},
	'māte' : {'result' : 'sieva', 'blessed' : False}
},
'meita' : {
	'brālis' : {'result' : 'dēls', 'blessed' : False},
	'māsa' : {'result' : 'meita', 'blessed' : False},
	'dēls' : {'result' : 'mazdēls', 'blessed' : True},
	'meita' : {'result' : 'mazmeita', 'blessed' : True},
	'precējusies' : {'result' : 'znots', 'blessed' : True},
	'vīrs' : {'result' : 'znots', 'blessed' : True},
	'tēvs' : {'result' : 'vīrs', 'blessed' : False},
	'māte' : {'result' : 'sieva', 'blessed' : False}
},
'māte' : {
	'brālis' : {'result' : 'mātesbrālis', 'blessed' : True},
	'māsa' : {'result' : 'mātesmāsa', 'blessed' : True},
	'dēls' : {'result' : 'brālis', 'blessed' : True},
	'meita' : {'result' : 'māsa', 'blessed' : True},
	'precējusies' : {'result' : 'tēvs', 'blessed' : False},
	'vīrs' : {'result' : 'tēvs', 'blessed' : False},
	'tēvs' : {'result' : 'vectēvs', 'blessed' : True},
	'māte' : {'result' : 'vecmamma', 'blessed' : True}
},
'tēvs' : {
	'brālis' : {'result' : 'tēvabrālis', 'blessed' : True},
	'māsa' : {'result' : 'tēvamāsa', 'blessed' : True},
	'dēls' : {'result' : 'brālis', 'blessed' : True},
	'meita' : {'result' : 'māsa', 'blessed' : True},
	'precējies' : {'result' : 'māte', 'blessed' : False},
	'sieva' : {'result' : 'māte', 'blessed' : False},
	'tēvs' : {'result' : 'vectēvs', 'blessed' : True},
	'māte' : {'result' : 'vecmamma', 'blessed' : True}
}
}

__inverted_relations = None
__secondary_relations = None

def inverted_relations(api):
	global __inverted_relations
	if __inverted_relations:
		return __inverted_relations

	names = set()
	for rel, val in inverted_relations_text.items():
		names.add(rel)
		names.add(val['male'])
		names.add(val['female'])
	mapping = {}
	for nameid in api.entity_id_mapping_by_name_list(names):
		if mapping.get(nameid.name):
			raise Exception("Netiekam galā ar attiecību veidiem - '%s' nav viennozīmīga entītija" % (nameid.name, ))
		mapping[nameid.name] = nameid.entityid

	def fetch_id(name):
		if not mapping.get(name):
			raise Exception("Netiekam galā ar attiecību veidiem - '%s' nav atbilstoša entītija" % (name, ))
		return mapping[name]			

	__inverted_relations = {}
	for rel, val in inverted_relations_text.items():
		__inverted_relations[ fetch_id(rel) ] = {'male' : fetch_id(val['male']), 'female' : fetch_id(val['female'])}
		
	return __inverted_relations
		
def secondary_relations(api):
	global __secondary_relations
	if __secondary_relations:
		return __secondary_relations

	names = set()
	for rel, val in secondary_relations_text.items():
		names.add(rel)
		for rel2, val2 in val.items():
			names.add(rel2)
			names.add(val2['result'])

	mapping = {}
	for nameid in api.entity_id_mapping_by_name_list(names):
		if mapping.get(nameid.name):
			raise Exception("Netiekam galā ar attiecību veidiem - '%s' nav viennozīmīga entītija" % (nameid.name, ))
		mapping[nameid.name] = nameid.entityid

	def fetch_id(name):
		if not mapping.get(name):
			print("'%s' nav atbilstoša entītija, insertojam" % (name, ), file=sys.stderr)
			mapping[name] = api.insertEntity(name, [name], 7, outerids=[], inflections = inflectEntity(name, 'relationship'))
		return mapping[name]			

	__secondary_relations = {}
	for rel, val in secondary_relations_text.items():
		new_val = {}
		for rel2, val2 in val.items():
			# new_val[ fetch_id(rel2) ] = {'result' : fetch_id(val2.get('result')), 'blessed' : val2.get('blessed')}
			# FIXME - testēšanas nolūkiem liekam ka nekad nav blessed, pēc tam jānomaina
			new_val[ fetch_id(rel2) ] = {'result' : fetch_id(val2.get('result')), 'blessed' : False}
		__secondary_relations[ fetch_id(rel) ] = new_val
		
	return __secondary_relations

# Ja entītijas dati nav listē, tad viņas dati jāpaņem no DB, lai ir korekta verbalizācija
def ensure_entity(entity_id, mentioned_entities, api):
	if entity_id not in mentioned_entities:
		entity_data = api.entity_data_by_id(entity_id)
		if entity_data:
			mentioned_entities[entity_id] = entity_data
		else: 
			print("Neizdevās iegūt entītijas datus ar id %s" % (entity_id,), file=sys.stderr)
			raise Exception


def build_relations(api, entity_a, frames, mentioned_entities):
	# print(entity, frames, blessed_summary_frames, mentioned_entities)
	# Hipotētiski visus šos api call varētu aizstāt ar singletonu, kas 1x no datubāzes ielādē visus pastarpināto attiecību tripletus, tādu nav nemaz tik daudz

	# FIXME - ja datiem būtu normāla struktūra nevis tas FrameData bullshit, tad kods būtu 5x vienkāršāks
	relations = secondary_relations(api)
	inv_relations = inverted_relations(api)
	result = []
	for frame in frames:
		if frame.get('FrameType') == 3:
			relation_ab = None
			partner1ID = None
			partner2ID = None
			for fd in frame.get('FrameData'):
			    if fd.get('Key') == 4:
			    	relation_ab = fd.get('Value').get('Entity')
			    if fd.get('Key') == 1:
			        partner1ID = fd.get('Value').get('Entity')
			    if fd.get('Key') == 2:
			        partner2ID = fd.get('Value').get('Entity')

			if not partner1ID or not partner2ID:
				continue

			# mums vajag attiecību a -> b  - ja freimā ir b -> a, tad jāinverto
			if partner1ID == entity_a:
				entity_b = partner2ID
			else:
				entity_b = partner1ID
				inverse_relation = inv_relations.get(relation_ab)
				if inverse_relation: # Ja šī relācija ir invertojama, tad mums vajag otrādo
					gender = 'male'
					inflections = json.loads(mentioned_entities[entity_b].get('NameInflections'))
					if inflections.get('Dzimte') == 'Sieviešu':
					    gender = 'female'
					relation_ab = inverse_relation[gender]
					ensure_entity(relation_ab, mentioned_entities, api) # FIXME - šis nepieciešams tikai debugam, ātrdardībai var ņemt ārā

			next_relations = relations.get(relation_ab)
			if next_relations:
				# OK, tagad mums ir freims A kuram ir vērts skatīties sekundārās attiecības B
				frames_b = api.entity_frames_by_id(entity_b)
				for frame_b in frames_b:
					if frame_b.get('FrameType') == 3:
						relation_bc = None
						partner1ID = None
						partner2ID = None
						for fd in frame_b.get('FrameData'):
						    if fd.get('Key') == 4:
						        relation_bc = fd.get('Value').get('Entity')
						    if fd.get('Key') == 1:
						        partner1ID = fd.get('Value').get('Entity')
						    if fd.get('Key') == 2:
						        partner2ID = fd.get('Value').get('Entity')

						# ja tas ir no relācijām kur viens partneris nekonkrēts, tad te nav ko darīt
						if not partner1ID or not partner2ID:
							continue

						# mums vajag attiecību b -> c  - ja freimā ir c -> b, tad jāinverto
						if partner1ID == entity_b:
							entity_c = partner2ID
						else:
							entity_c = partner1ID

						# Ja pievilktā entītija nav listē, tad viņas dati jāpaņem, lai ir korekta verbalizācija
						ensure_entity(entity_c, mentioned_entities, api)

						if entity_c == partner1ID:
							inverse_relation = inv_relations.get(relation_bc)
							if inverse_relation: # Ja šī relācija ir invertojama, tad mums vajag otrādo
								gender = 'male'
								inflections = json.loads(mentioned_entities[entity_c].get('NameInflections'))
								if inflections.get('Dzimte') == 'Sieviešu':
								    gender = 'female'
								relation_bc = inverse_relation[gender]
						ensure_entity(relation_bc, mentioned_entities, api) # FIXME - šis nepieciešams tikai debugam, ātrdardībai var ņemt ārā

						if entity_a == entity_c:
							continue

						result_relation = next_relations.get(relation_bc)
						if result_relation:														
							result.append ( {'FrameData':[
								{'Key':1,'Value': {'Entity': entity_a}},
								{'Key':2,'Value': {'Entity': entity_c}},
								{'Key':4,'Value': {'Entity': result_relation.get('result')}}
								], 'IsBlessed':result_relation.get('blessed'), 'FrameType' : 3, 'SourceId' : 'Pastarpināto attiecību veidošana', 'IsHidden' : False} )
							# Ja pievilktā attiecību veida entītija nav listē, tad viņas dati jāpaņem, lai ir korekta verbalizācija
							ensure_entity(result_relation.get('result'), mentioned_entities, api)

							# print('%s --[%s]--> %s --[%s]--> %s, sanāk %s' % (mentioned_entities[entity_a]['Name'], mentioned_entities[relation_ab]['Name'], mentioned_entities[entity_b]['Name'],mentioned_entities[relation_bc]['Name'], mentioned_entities[entity_c]['Name'], mentioned_entities[result_relation.get('result')]['Name']))
	return result