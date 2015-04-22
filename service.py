#!/usr/bin/env python3
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

import sys, os
import signal

pidfile = 'service.pid'
pidfile = os.path.join(os.path.dirname(__file__), pidfile)  # relative to script directory

try:
    # get pid from pid file
    with open(pidfile, 'r') as f:
        pid = int(f.read().strip())

    # kill other service process
    os.kill(pid, signal.SIGKILL)
except:
    pass

# write pid file
with open(pidfile, 'w') as f:
    print(os.getpid(), file=f)



sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
# from db_config import api_conn_info
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection

from consolidate_frames import consolidate_frames, process_entities
from DocumentUpload import upload2db
import json, traceback

from bottle import request, Bottle, abort, static_file, response, run, hook

# JavaScript like dictionary: d.key <=> d[key]
# Elegants risinājums:
# http://stackoverflow.com/a/14620633
class Dict(dict):
    def __init__(self, *args, **kwargs):
        super(Dict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def __getattribute__(self, key):
        try:
            return super(Dict, self).__getattribute__(key)
        except:
            return

    def __delattr__(self, name):
        if name in self:
            del self[name]

# defaults

# host = 'localhost'    # host šeit nav vajadzīgs
port = 9990
listen_host = '0.0.0.0'
out_dir = './output'


# from config import databases
import config

# if hasattr(config, 'host'):
#     host = config.host

if hasattr(config, 'service_port'):
    port = config.service_port

if hasattr(config, 'listen_host'):
    listen_host = config.listen_host

if not hasattr(config, 'databases'):
    print('ERROR: databases not present in configuration file')
    quit(1)


databases = config.databases



# print('Host:', host)
print('Port:', port)
# print('API port:', api_port)
print('Listen address:', listen_host)
# print('Databases:')
database_names = [name for name,database in databases]
# databases = {name:' '.join('%s=%s' % (str(k), str(v)) for k,v in database.items()) for name,database in databases}
databases = {name:database for name,database in databases}
print('Databases:', ', '.join(database_names))
print()




def get_db(name):
    global databases
    if name is None:
        raise Exception("No database specified!")
        return None
    if name not in databases:   # datubāze nav konfigurēta
        raise Exception("Unknown database '"+name+"'")
        return None
    if type(databases[name]) is dict:    # datubāzes objekts vēl nav izveidots
        api = databases[name] = SemanticApiPostgres(PostgresConnection(databases[name]))
    else:
        api = databases[name]

    return api


# conn = PostgresConnection(api_conn_info)
# api = SemanticApiPostgres(conn)




app = Bottle()

# info par servisu
# TODO: vajag labāku noformējumu
@app.get('/')
def root():
    response.conent_type = 'text/html; charset=utf-8'
    response.status = 200
    return """<!doctype html>
<html>
<head>
    <title>Entity consolidation service</title>
</head
<body>
API: <br/>
/databases/&lt;frameid&gt;/consolidate POST list of comma separated entity ids
<br/>
/databases/&lt;frameid&gt;/consolidate/&lt;entityid&gt; GET or POST
</body>
</html>
"""

# # https://gist.github.com/richard-flosi/3789163
# @app.hook('after_request')
# def enable_cors():
#     """
#     You need to add some headers to each request.
#     Don't use the wildcard '*' for Access-Control-Allow-Origin in production.
#     """
#     response.headers['Access-Control-Allow-Origin'] = '*'
#     response.headers['Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
#     response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

# def consolidate_frames(entity_list, api):
# process_entities(entity_list, out_dir, api=api)

@app.get('/databases/<name>/consolidate/<entityid:int>')
@app.post('/databases/<name>/consolidate/<entityid:int>')
def consolidate_entity(name, entityid):
    response.conent_type = 'text/html; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    try:
        api = get_db(name)
        process_entities([entityid], out_dir, api)
    except Exception as e:
        print('Consolidate error:', str(e).strip())
        traceback.print_exc()
        response.status = 500
        result = 'Error in consolidating entity '+str(entityid)+': '+str(e)
        return result
    response.status = 200
    return str(entityid) + ' OK'


@app.post('/databases/<name>/consolidate')
def consolidate_entities(name):
    response.conent_type = 'text/html; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    entityids = request.body.read().decode('utf8', errors='ignore')
    entityids = [int(entityid.strip()) for entityid in entityids.split(',')]
    try:
        api = get_db(name)
        process_entities(entityids, out_dir, api)
    except Exception as e:
        print('Consolidate error:', str(e).strip())
        traceback.print_exc()
        response.status = 500
        result = 'Error in consolidating entities '+','.join(str(x) for x in entityids)+': '+str(e)
        return result

    # if result:
    #     response.code = 200
    #     return result
    # else: # result = None - freims nav atrasts
    #     response.code = 404
    #     return 'Entity not found'
    response.status = 200
    return ','.join(str(x) for x in entityids) + ' OK'



# https://gist.github.com/richard-flosi/3789163
@app.hook('after_request')
def enable_cors():
    """
    You need to add some headers to each request.
    Don't use the wildcard '*' for Access-Control-Allow-Origin in production.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

from datetime import date, datetime

@app.route('/databases/<name>/upload', method=['OPTIONS', 'POST'])
def upload(name):
    if request.method == 'OPTIONS':
        return ''
    response.conent_type = 'text/html; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    # response.add_header('access-control-allow-credentials', 'true')
    # response.add_header('access-control-allow-headers', 'x-prototype-version,x-requested-with')
    response.add_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS, PUT')
    response.add_header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
    document = json.loads(request.body.read().decode('utf8', errors='ignore'), object_hook=Dict)
    try:
        if not document.date:
            document.date = datetime.now().strftime("%Y-%m-%d")
        else:
            document.date = datetime.strptime(document.date, '%Y-%m-%d').date() # atpakaļ no serializētā stringa
        if not document.id:
            raise Exception("No document id")
        api = get_db(name)
        upload2db(document, api)
    except Exception as e:
        print('Upload error:', str(e).strip())
        traceback.print_exc()
        result = 'Upload error: ' + str(e)
        response.status = 500
        return result
    response.status = 200
    return 'OK'

@app.route('/databases/<name>/upload/<id>', method=['OPTIONS', 'POST'])
def upload_id(name, id):
    if request.method == 'OPTIONS':
        return ''
    response.conent_type = 'text/html; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    # response.add_header('access-control-allow-credentials', 'true')
    # response.add_header('access-control-allow-headers', 'x-prototype-version,x-requested-with')
    response.add_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS, PUT')
    response.add_header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
    document = json.loads(request.body.read().decode('utf8', errors='ignore'), object_hook=Dict)
    try:
        if not document.date:
            document.date = datetime.now().strftime("%Y-%m-%d")
        else:
            document.date = datetime.strptime(document.date, '%Y-%m-%d').date() # atpakaļ no serializētā stringa
        document.id = id
        # if not document.id:
        #     raise Exception("No document id")
        api = get_db(name)
        upload2db(document, api)
    except Exception as e:
        print('Upload error:', str(e).strip())
        traceback.print_exc()
        result = 'Upload error: ' + str(e)
        response.status = 500
        # result = traceback.format_exc()
        return result
    response.status = 200
    return 'OK'




run(app, host=listen_host, port=port)
