#!/usr/bin/env python
# -*- coding: utf8 -*-
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

"""
Note: some tests are silenced using @nottest
"""

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

logging.getLogger("SemanticApi").level = logging.INFO

import sys
sys.path.append("../src")

from nose.tools import eq_, ok_, nottest
from pprint import pprint

# ---------------------------------------- 

# get DB connection config
from db_config import api_conn_info

from SemanticApiPostgres import PostgresConnection, SemanticApiPostgres

# ---------------------------------------- 

t_frame1 = {
    u'DocumentId': u'A36BE0CF-F5D1-4432-AFAE-3FE54D6572A9',
    u'FrameCnt': 5,
    u'FrameData': [{u'Key': 1,
                    u'Value': {u'Entity': 1560495, u'PlaceInSentence': 6}},
                   {u'Key': 3,
                    u'Value': {u'Entity': 1819083, u'PlaceInSentence': 3}}],
    u'FrameId': 2437189,
    u'FrameMetadata': [{u'Key': u'Fdatetime', u'Value': '2007-12-06 00:00:00Z'}],
    u'FrameText': u'Dzintars Za\u0137is bija deput\u0101ta amat\u0101',
    u'FrameType': 9,
    u'IsBlessed': False,
    u'IsDeleted': False,
    u'IsHidden': False,
    u'MergeType': 'M',
    u'SentenceId': u'311',
    u'SourceId': u'Pipeline parse at 2013-11-18T14:13:15.256273',
    u'SummarizedFrames': [2437189, 2406073, 2442501, 2416129, 2636120],
    u'SummaryInfo': 'captsolo | ConsolidateFrames:  BaseConsolidator | 2013_12_17 01:43:00',
    u'TargetWord': u'deput\u0101tu'
 }

@nottest   # NOTE: test changes DB contents - disabled for now
def test_delete_and_insert():
    # setup
    conn = PostgresConnection(api_conn_info)
    pg_api = SemanticApiPostgres(conn)

    # data example to test
    e_id = 1560495

    # delete summary frames
    pg_api.delete_entity_summary_frames(e_id)

    # insert summary frame
    res = pg_api.insert_summary_frame(t_frame1)
    print "Inserted summary frame ID:", res

# ---------------------------------------- 

def main():
    pass

if __name__ == "__main__":
    main()


