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

from TextGenerator import verbalizeframe

# ---------------------------------------- 

# get DB connection config
from db_config import api_conn_info

from SemanticApiPostgres import PostgresConnection, SemanticApiPostgres

# ---------------------------------------- 

# Standarta success case
def test_normal():
    # setup
    conn = PostgresConnection(api_conn_info)
    pg_api = SemanticApiPostgres(conn)

    # data example to test
    verbalization = verbalizeframe(pg_api, 1376556)
    print verbalization
    eq_(verbalization, u'1977 Ludvigs Tribockis mācījās Ogres 1. vidusskolā') # Pati verbalizācija drīkst mainīties atkarībā no vēlmēm, bet jābūt sakarīgam rezultātam

# Freima ID, kurš neeksistē
def test_bad_id():
    conn = PostgresConnection(api_conn_info)
    pg_api = SemanticApiPostgres(conn)

    verbalization = verbalizeframe(pg_api, 1234)
    eq_(verbalization, None)

# Freims ar entīti kurai ir tukšs vārds (vismaz 20.dec dataset)
def test_empty_names():    
    conn = PostgresConnection(api_conn_info)
    pg_api = SemanticApiPostgres(conn)

    verbalization = verbalizeframe(pg_api, 1297495)
    print verbalization
    ok_(verbalization)

def main():
    pass

if __name__ == "__main__":
    main()


