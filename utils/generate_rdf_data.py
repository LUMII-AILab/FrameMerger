#!/usr/bin/env python
# -*- coding: utf8 -*-
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

# NOTE: don't forget to run ./utils/get_all_data.py before running this file

PICKLE_FILE_WITH_FRAME_DATA = './all_frames.pickle'
PICKLE_FILE_WITH_ENTITY_DATA = './ref_entities_data.pickle'


import sys
sys.path.append("./src")

from FrameTypeInfo import FrameTypeInfo 

from rdflib import Graph, Namespace, URIRef, Literal, BNode, RDF, term
from rdflib.namespace import RDFS, OWL

import cPickle as pickle

import traceback


# FIXME need to get precise entity names from entity type id
def entity_type_uri_ref_by_index(index):
	# type_map = [
	# 	"Person",
	# 	"Event"
	# 	# ...
	# ]
	return URIRef("http://lumii.lv/ontologies/LETA_Frames#Entity")

def entity_uri(local_entity_id):
	# generate entity id
	entity_rdf_id = "entity_" + str(local_entity_id)
	return URIRef("http://lumii.lv/rdf_data/LETA_Frames/" + entity_rdf_id)


def add_entity_to_rdf_store(entity, rdf_store):
	# create rdf node for the entity
	entity_node = entity_uri(entity[u'EntityId'])

	# get entity type ref
	entity_type = entity_type_uri_ref_by_index(entity[u'Category'])
	
	# add type assertion
	rdf_store.add((entity_node, RDF.type, entity_type))

	# add name assertion
	rdf_store.add((entity_node, URIRef("http://lumii.lv/ontologies/LETA_Frames#NameD"), Literal(entity[u'Name'], lang="lv")))

	rdf_store.add((entity_node, RDFS.label, Literal(entity[u'Name'])))


def get_frame_type_uri_ref_by_index(index, frame_type_info):
	return URIRef("http://lumii.lv/ontologies/LETA_Frames#" + frame_type_info.frame_type_en_name_from_id(index))

def get_role_type_uri_ref_by_frame_and_role_index(frame_index, role_index, frame_type_info):
	return URIRef("http://lumii.lv/ontologies/LETA_Frames#" + frame_type_info.frame_role_en_name_from_frame_id_and_role_id(frame_index, role_index))

def add_frame_to_rdf_store(frame, rdf_store, frame_type_info, entity_dict):
	# print(pprint(frame))

	# generate frame id
	frame_rdf_id = "frame_" + str(frame[u'FrameId'])

	# create rdf node for the frame
	frame_node = URIRef("http://lumii.lv/rdf_data/LETA_Frames/" + frame_rdf_id)

	# get frame type ref
	frame_type = get_frame_type_uri_ref_by_index(frame[u'FrameType'], frame_type_info)
	
	# add type assertion
	rdf_store.add((frame_node, RDF.type, frame_type))

	# add frame text
	if u'FrameText' in frame:
		rdf_store.add((frame_node, URIRef("http://lumii.lv/ontologies/LETA_Frames#FrameText"), Literal(frame[u'FrameText'], lang="lv")))

	# add frame source id
	if u'SourceId' in frame:
		rdf_store.add((frame_node, URIRef("http://lumii.lv/ontologies/LETA_Frames#SourceId"), Literal(frame[u'SourceId'])))

	label = frame_type_info.frame_type_en_name_from_id(frame[u'FrameType'])
	roles = []

	# add roles
	for role_link in frame[u'FrameData']:
		role_type = get_role_type_uri_ref_by_frame_and_role_index(frame[u'FrameType'], role_link[u'Key'], frame_type_info)
		entity = entity_uri(role_link[u'Value'][u'Entity'])

		rdf_store.add((frame_node, role_type, entity))

		# dereference entity for label
		entity_name = entity_dict[role_link[u'Value'][u'Entity']].get(u'Name', u'???') or u'???'
		roles.append(frame_type_info.frame_role_en_name_from_frame_id_and_role_id(frame[u'FrameType'], role_link[u'Key']) + u' : ' + entity_name)

	label = label + u'(' + u', '.join(roles) + u')'
	rdf_store.add((frame_node, RDFS.label, Literal(label)))


def main():
	print("create rdf_store")
	rdf_store = Graph()

	rdf_store.add((URIRef("http://lumii.lv/rdf_data/LETA_Frames/"), RDF.type, OWL.Ontology))

	print("load frame info data")
	frame_type_info = FrameTypeInfo()

	print("load frames from pickle")
	frame_list = pickle.load(open(PICKLE_FILE_WITH_FRAME_DATA, 'rb'))

	print("load entities from pickle")
	entity_list = pickle.load(open(PICKLE_FILE_WITH_ENTITY_DATA, 'rb'))

	print("place entities in a dictionary for easy access")
	entity_dict = {}
	for entity_answer in entity_list[u'Answers']:
		entity = entity_answer[u'Entity']
		entity_dict[entity[u'EntityId']] = entity


	try:
		print("convert frames to rdf")
		counter = 0
		for frame in frame_list[u'Answers'][0][u'FrameData']:
			counter = counter + 1
			print("process frame " + str(counter))
			# print(frame)
			# print(pprint(frame))
			add_frame_to_rdf_store(frame, rdf_store, frame_type_info, entity_dict)

		print("convert entitys to rdf")

		counter = 0
		for entity_answer in entity_list[u'Answers']:
			counter = counter + 1
			print("process entity " + str(counter))
			entity = entity_answer[u'Entity']
			# print(frame)
			# print(pprint(frame))
			add_entity_to_rdf_store(entity, rdf_store)

	except: # catch *all* exceptions
		# just in case serialize what we have so far
		print("Error:")
		traceback.print_exc()


	print("save rdf file (may take some minutes)")
	# Serialize the store as RDF/XML to the file frame_data.rdf.
	rdf_store.serialize("./output/frame_and_entity_data.rdf", format="pretty-xml")

if __name__ == "__main__":
    main()
