#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
sys.path.append("../src")

from nose.tools import eq_, ok_, nottest
from pprint import pprint

from SemanticApi import SemanticApi

# ---------------------------------------- 

@nottest
def test_entities_by_id():

    print "Testing GetEntityDataById"
    print

    id_list = [1184, 1191, 4351, 5000]

    api = SemanticApi()
    res = api.entities_by_id(id_list)

    pprint(res)
    print

    print res['Answers'][2]['Entity']['Name'].encode("utf-8")
    print


def test_entity_ids_by_name():

    print "Testing GetEntityIdByName"
    print

    name_list = [
        'Tumanova I.',          # 1184
        '"Infima"',             # 1191
        "Lācīša skola ",        # 4351
        "Lācīša skola"          # not exact match, but similar to 4351 (w/out the trailing " ")
    ]

    api = SemanticApi()
    res = api.entity_ids_by_name(name_list)

    pprint(res)
    print

def try_get_frames_example():

    api = SemanticApi()

    print "Trying GetFrame example (1)"
    print

    res = api.get_frames([13], [0, 1])

    pprint(res)
    print

    print "Trying GetFrame example (1) with all frame types"
    print

    res = api.get_frames([13], [])

    pprint(res)
    print

    print "Trying GetFrame example (2)"
    print

    res = api.get_frames([19], [1])

    pprint(res)
    print


def test_get_frames():

    api = SemanticApi()

    print "Testing GetFrame (1)"
    print

    id_list = [1097,]
    
    res = api.get_frames(id_list)

    pprint(res)
    print

    print "Testing GetFrame (1)"
    print

    id_list = [1191, 1097, 5000]
    f_types = [4, 5, 6, 7]

    res = api.get_frames(id_list, f_types)

    pprint(res)
    print

@nottest
def test_get_all_entities():

    api = SemanticApi()

    print "Trying Get All Entities by ID"
    print

    res = api.entities_by_id([])

    pprint(res)
    print

    print "Trying Get All Entities by Name"
    print

    res = api.entity_ids_by_name([])

    pprint(res)
    print

@nottest
def get_entity_list():

    print "Getting NamedEntity list (0..4999)"
    print

    id_list = range(5000)

    api = SemanticApi()
    res = api.entities_by_id(id_list)

    entity_db = {}

    for item in res["Answers"]:
        item_id = item["Entity"]["EntityId"]

        if item["Answer"] == 0:     # 0 = OK
            entity_db[item_id] = item["Entity"]

    print "Entity list length:", len(entity_db)
    print
    
    out = filter(lambda x: "Tilts" not in x["Name"], entity_db.values())
    pprint(out[:5])
    print
