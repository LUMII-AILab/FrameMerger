#!/usr/bin/env python3
# coding=utf-8
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
from db_config import api_conn_info
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection
from TextGenerator import verbalizeframe

from bottle import request, Bottle, abort, static_file, response, run

port = 9000

conn = PostgresConnection(api_conn_info)
api = SemanticApiPostgres(conn)

app = Bottle()

# info par servisu
@app.get('/')
def root():
    response.conent_type = 'text/html; charset=utf-8'
    response.code = 200
    return """<!doctype html>
<html>
<head>
    <title>Frame verbalization service</title>
</head
<body>
API: <br/>
/verbalize/&lt;frameid&gt;
</body>
</html>
"""


@app.get('/verbalize/<frameid:int>')
def verb(frameid):
    response.conent_type = 'text/html; charset=utf-8'
    try:
        result = verbalizeframe(api, frameid)
    except Exception as e:
        response.code = 500
        result = 'Error in verbalizing frame '+str(frameid)+'</br>'+str(e)

    if result:
        response.code = 200
        return result
    else: # result = None - freims nav atrasts
        response.code = 404
        return 'Frame not found'



run(app, host='0.0.0.0', port=port)



# Test:
#
# conn = PostgresConnection(api_conn_info)
# api = SemanticApiPostgres(conn)
# 
# print verbalizeframe(api, 2299369)
# print verbalizeframe(api, 2299370)
# print verbalizeframe(api, '2299371')
#
# Some existing frame ids
# 2299369
# 2299370
# 2299371
# 2299372
# 2299373
# 2299374
# 2299375
# 2299376
# 2299377
# 2299378
# 2299379
# 2299380
# 2299381
# 2299382
# 2299383
# 2299384
# 2299385
# 2299386
# 2299387
# 2299388
