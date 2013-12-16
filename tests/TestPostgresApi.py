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

# ---------------------------------------- 

def main():
    pass

if __name__ == "__main__":
    main()


