#!/usr/bin/env python
# -*- coding: utf8 -*-
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

namespaces = """
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix frame_ont: <http://ontologies.lumii.lv/frame_ont/> .
"""

entity_uri = "http://frame_db.lumii.lv/entity/"
frame_uri = "http://frame_db.lumii.lv/frame/"

# VARIANTI:
#  - vai SKOS ir piemērots Entity representēšanai RDFā?
#  - kas (no esošām klasēm) ir piemērots Frame reprezentēšanai RDFā?

frame_tmpl = """
<%(uri)s>
    a   frame_ont:Frame ;
    frame_ont:frame_id "%(id)s" ;
    frame_ont:time <%(time_entity_uri)s> .
"""

entity_tmpl = """
<%(uri)s>
    a   frame_ont:Entity;
    frame_ont:entity_id "%(id)s" ;
    frame_ont:entity_type "%(category)s";
    frame_ont:name "%(primary_name)s" .
"""

class RdfOutput(object):
    def __init__(self):
        pass

    def header(self):
        return namespaces

    def frame_to_RDF(self, f):
        return frame_tmpl % {
            "uri":  frame_uri + str(f["FrameId"]),
            "id":   f["FrameId"],
            "time_entity_uri":  entity_uri + "1111"           # fictional entity ID for the "Time" slot
        }

    def entity_to_RDF(self, e):
        return entity_tmpl % {
            "uri":  entity_uri + str(e["EntityId"]),
            "id":   e["EntityId"],
            "category": e["Category"],   # entity tipiem ar' vajag savu ontoloģiju / URIs
            "primary_name": e["Name"]
        }

    def footer(self):
        return ""

# ---------------------------------------- 

def main():

    test_entity = eval("""
{u'Category': 3,
 u'EntityId': 42,
 u'Name': u'Imants Ziedonis',
 u'NameInflections': u'{"\u0122enit\u012bvs":"Imanta Ziedo\u0146a","Dat\u012bvs":"Imantam Ziedonim","Akuzat\u012bvs":"Imantu Ziedoni","Lokat\u012bvs":"Imant\u0101 Ziedon\u012b","Nominat\u012bvs":"Imants Ziedonis"}',
 u'OtherName': [u'I. Ziedonis'],
 u'OuterId': [u'F6A8C3B7-AC39-11D4-9D85-00A0C9CFC2DB']}
""")

    test_frame = eval("""
{u'DocumentId': u'5D1D9D88-CDBB-4B32-BDE1-CE0DBBC7A18A',
 'FrameCnt': 1,
 u'FrameData': [{u'Key': 4, u'Value': {u'Entity': 46, u'PlaceInSentence': 0}},
                {u'Key': 2, u'Value': {u'Entity': 44, u'PlaceInSentence': 0}},
                {u'Key': 3, u'Value': {u'Entity': 45, u'PlaceInSentence': 0}},
                {u'Key': 1, u'Value': {u'Entity': 42, u'PlaceInSentence': 0}}],
 u'FrameId': 1000017,
 u'FrameType': 0,
 u'IsBlessed': False,
 u'IsDeleted': None,
 u'IsHidden': None,
 u'SentenceId': u'22',
 u'SourceId': u'Manual sample data - kasjauns.lv',
 u'TargetWord': None}
""")

    fmt = RdfOutput()

    print fmt.header()
    print fmt.entity_to_RDF(test_entity)
    print fmt.frame_to_RDF(test_frame)
    print fmt.footer()

if __name__ == "__main__":
    main()

