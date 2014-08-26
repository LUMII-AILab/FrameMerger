#!/usr/bin/env python3

import sys

sys.path.append("./src")
# from db_config import api_conn_info
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection

from consolidate_frames import consolidate_frames, process_entities
from DocumentUpload import upload2db

from bottle import request, Bottle, abort, static_file, response, run


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
    response.code = 200
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
        # import traceback
        # traceback.print_exc()
        response.code = 500
        result = 'Error in consolidating entity '+str(entityid)+'</br>'+str(e)
        return result
    response.code = 200
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
        response.code = 500
        result = 'Error in consolidating entities '+','.join(str(x) for x in entityids)+'</br>'+str(e)
        return result

    # if result:
    #     response.code = 200
    #     return result
    # else: # result = None - freims nav atrasts
    #     response.code = 404
    #     return 'Entity not found'
    response.code = 200
    return ','.join(str(x) for x in entityids) + ' OK'



from datetime import date, datetime

@app.post('/databases/<name>/upload')
def upload(name):
    response.conent_type = 'text/html; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    entityids = request.body.read().decode('utf8', errors='ignore')
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
        response.code = 500
        result = 'Upload error: ' + str(e)
        return result
    response.code = 200
    return 'OK'

@app.post('/databases/<name>/upload/<id>')
def upload_id(name, id):
    response.conent_type = 'text/html; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    entityids = request.body.read().decode('utf8', errors='ignore')
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
        response.code = 500
        result = 'Upload error: ' + str(e)
        return result
    response.code = 200
    return 'OK'




run(app, host=listen_host, port=port)
