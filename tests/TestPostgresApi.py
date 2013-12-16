#!/usr/bin/env python
# -*- coding: utf8 -*-

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

logging.getLogger("SemanticApi").level = logging.INFO

import sys
sys.path.append("../src")

from nose.tools import eq_, ok_, nottest
from pprint import pprint


from FrameInfo import FrameInfo

from ConsolidateFrames import Consolidator
import EntityFrames as EF

# ---------------------------------------- 

from SemanticApiPostgres import PostgresConnection, SemanticApiPostgres, default_conn_info
from SemanticApi import SemanticApi

# ---------------------------------------- 

def test_entity_frameids_equal():
    # setup
    api = SemanticApi()
    conn = PostgresConnection(default_conn_info)
    pg_api = SemanticApiPostgres(conn)

    # data example to test
    e_id = 1560092

    # action
    frames_old = api.entity_frames_by_id(e_id)
    frameids_old = set(item["FrameId"] for item in frames_old)
    pprint(frameids_old)

    frameids_new = set(pg_api.frame_ids_by_entity(e_id))

    eq_(frameids_old, frameids_new)

def test_entity_frames_by_id():

    def find_frame_by_id(frames, fr_id):
        res = filter(lambda x: x["FrameId"] == fr_id, frames)[0]
        #del res["FrameMetadata"]

        return res

    # setup
    api = SemanticApi()
    conn = PostgresConnection(default_conn_info)
    pg_api = SemanticApiPostgres(conn)

    # data example to test
    e_id = 1560092

    # action
    frames_old = api.entity_frames_by_id(e_id)
    frames_new = pg_api.entity_frames_by_id(e_id)

    #pprint(frames_old)
    #print
    #pprint(frames_new)

    # check results
    frameids_old = set(item["FrameId"] for item in frames_old)

    ok_(False, "Mismatch in date formats in [FrameMetadata] timestamp - change to match API behaviour?") 
    
    for fr_id in frameids_old:
        eq_(find_frame_by_id(frames_old, fr_id), find_frame_by_id(frames_new, fr_id))


def test_frame_info_equal():
    # setup
    api = SemanticApi()
    conn = PostgresConnection(default_conn_info)
    pg_api = SemanticApiPostgres(conn)

    # data example to test
    e_id = 1560092

    # action
    frames_old = api.entity_frames_by_id(e_id)
    frameids_old = set(item["FrameId"] for item in frames_old)
    min_id = min(frameids_old)

    frame_old = filter(lambda item: item["FrameId"] == min_id, frames_old)[0]
    del frame_old["FrameData"]
    pprint(frame_old)

    frame_new = pg_api.frame_by_id(min_id)
    del frame_new["FrameData"]
    pprint(frame_new)

    # not comparing frame element data
    eq_(frame_old, frame_new)

def test_frame_elements_equal():
    # setup
    api = SemanticApi()
    conn = PostgresConnection(default_conn_info)
    pg_api = SemanticApiPostgres(conn)

    # data example to test
    e_id = 1560092

    # action
    frames_old = api.entity_frames_by_id(e_id)
    frameids_old = set(item["FrameId"] for item in frames_old)
    min_id = min(frameids_old)

    frame_old = filter(lambda item: item["FrameId"] == min_id, frames_old)[0]
    frame_data_old = frame_old["FrameData"]
    pprint(frame_data_old)

    frame_new = pg_api.frame_by_id(min_id)
    frame_data_new = frame_new["FrameData"]
    pprint(frame_data_new)

    # not comparing frame element data
    eq_(frame_data_old, frame_data_new)

def test_entity_info_equal():
    # setup
    api = SemanticApi()
    conn = PostgresConnection(default_conn_info)
    pg_api = SemanticApiPostgres(conn)

    # data example to test
    e_id = 1560092

    # action
    old_info = api.entity_data_by_id(e_id)
    new_info = pg_api.entity_data_by_id(e_id)

    pprint(old_info)
    print
    pprint(new_info)

    # not comparing frame element data
    eq_(old_info, new_info)

def test_entity_ids_by_name():
    # setup
    api = SemanticApi()
    conn = PostgresConnection(default_conn_info)
    pg_api = SemanticApiPostgres(conn)

    # data example to test
    e_name = (u"Imants Ziedonis",)

    # action

    #  - NOTE: funkciju atgrieztās datu strukt. atšķiras
    old_info = api.entity_ids_by_name(e_name)["Answers"][0]["EntityIdList"]
    new_info = pg_api.entity_ids_by_name_list(e_name[0])

    pprint(old_info)
    print
    pprint(new_info)

    # not comparing frame element data
    eq_(old_info, new_info)

def test_api_methods_equal():
    # setup
    api = SemanticApi()
    conn = PostgresConnection(default_conn_info)
    pg_api = SemanticApiPostgres(conn)

    # gather data
    old_info = filter(lambda x: not x.startswith("__"), dir(api))
    old_info = filter(lambda x: x not in ["ServiceURI", "UserID", "api_call", "timeout"], old_info)

    new_info = filter(lambda x: not x.startswith("__"), dir(pg_api))
    new_info = filter(lambda x: x not in ["api"], new_info)

    pprint(old_info)
    print
    pprint(new_info)
    print 
    pprint(set(old_info) - set(new_info))

    # not comparing frame element data
    eq_(old_info, new_info)
# ---------------------------------------- 

def main():
    pass

if __name__ == "__main__":
    main()


