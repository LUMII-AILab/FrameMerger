#!/usr/bin/env python3
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

import sys, os, gzip
import signal

from datetime import date, datetime


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

from consolidate_frames import consolidate_frames, process_entities, start_logging, log as consolidate_log
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


log_dir = 'log' # default

if hasattr(config, 'log_dir'):
    log_dir = config.log_dir

if log_dir is not None:
    # make it relative to script dir
    if not log_dir.startswith('./') and not log_dir.startswith('../') and not log_dir.startswith('/'):
        log_dir = os.path.join(os.path.dirname(__file__), log_dir)

    if log_dir and not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    print('Log dir:', log_dir)

    log_filename = os.path.join(log_dir, os.path.splitext(os.path.basename(__file__))[0]+datetime.now().strftime("-%Y_%m_%d-%H_%M.log"))

    print('Log filename:', log_filename)

    logf = open(log_filename, 'wt')
else:
    print('Logging disabled')
    logf = open(os.devnull, 'wt')

def now():
    return datetime.now().strftime("[%Y-%m-%d %H:%M.%f]")

def log(*args, **kargs):
    print(*args, file=logf, **kargs)

def log_flush():
    print(end='', file=logf, flush=True)


log(now(), "Starting...", flush=True)

start_logging(consolidate_log.DEBUG)


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
    response.content_type = 'text/html; charset=utf-8'
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
    log(now(), request.method, request.path)
    response.content_type = 'text/html; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    try:
        api = get_db(name)
        process_entities([entityid], out_dir, api)
    except KeyboardInterrupt:
        print('interrupted')
        quit()
    except Exception as e:
        print(now(), 'Consolidate error:', str(e).strip())
        traceback.print_exc()
        print(now(), 'Consolidate error:', str(e).strip(), file=logf)
        traceback.print_exc(file=logf)
        log_flush()
        response.status = 500
        result = 'Error in consolidating entity '+str(entityid)+': '+str(e)
        return result
    log_flush()
    response.status = 200
    return str(entityid) + ' OK'


@app.post('/databases/<name>/consolidate')
def consolidate_entities(name):
    log(now(), request.method, request.path)
    response.content_type = 'text/html; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    entityids = request.body.read().decode('utf8', errors='ignore')
    entityids = [int(entityid.strip()) for entityid in entityids.split(',')]
    try:
        api = get_db(name)
        print('consolidating', len(entityids), 'entities')
        process_entities(entityids, out_dir, api)
    except KeyboardInterrupt:
        print('interrupted')
        quit()
    except Exception as e:
        print(now(), 'Consolidate error:', str(e).strip())
        traceback.print_exc()
        print(now(), 'Consolidate error:', str(e).strip(), file=logf)
        traceback.print_exc(file=logf)
        log_flush()
        response.status = 500
        result = 'Error in consolidating entities '+','.join(str(x) for x in entityids)+': '+str(e)
        return result

    # if result:
    #     response.code = 200
    #     return result
    # else: # result = None - freims nav atrasts
    #     response.code = 404
    #     return 'Entity not found'
    log_flush()
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

@app.route('/databases/<name>/upload', method=['OPTIONS', 'POST'])
def upload(name):
    if request.method == 'OPTIONS':
        return ''
    log(now(), request.method, request.path)
    consolidate = request.query.get('consolidate', '').lower().strip() in ['true', '1', 't', 'y', 'yes']
    # response.content_type = 'text/html; charset=utf-8'
    response.content_type = 'application/json; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    # response.add_header('access-control-allow-credentials', 'true')
    # response.add_header('access-control-allow-headers', 'x-prototype-version,x-requested-with')
    response.add_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS, PUT')
    response.add_header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
    data = request.body.read()
    try:
        data = gzip.decompress(data)
    except:
        pass
    data = data.decode('utf8', errors='ignore')
    document = json.loads(data, object_hook=Dict)
    # document = json.loads(request.body.read().decode('utf8', errors='ignore'), object_hook=Dict)
    try:
        if not document.date:
            document.date = datetime.now().strftime("%Y-%m-%d")
        else:
            document.date = datetime.strptime(document.date, '%Y-%m-%d').date() # atpakaļ no serializētā stringa
        if not document.id:
            raise Exception("No document id")
        api = get_db(name)
        print('uploading document', id)
        dirtyEntities = upload2db(document, api)
        if consolidate:
            print('consolidating document', id, 'entities (%i dirty entities)' % len(dirtyEntities))
            process_entities(dirtyEntities, out_dir, api)
    except KeyboardInterrupt:
        print('interrupted')
        quit()
    except Exception as e:
        print(now(), 'Upload error:', str(e).strip())
        traceback.print_exc()
        print(now(), 'Upload error:', str(e).strip(), file=logf)
        traceback.print_exc(file=logf)
        log_flush()
        result = 'Upload error: ' + str(e)
        response.status = 500
        return result
    log_flush()
    response.status = 200
    return json.dumps(dirtyEntities)

@app.route('/databases/<name>/upload/<id>', method=['OPTIONS', 'POST'])
def upload_id(name, id):
    if request.method == 'OPTIONS':
        return ''
    log(now(), request.method, request.path)
    consolidate = request.query.get('consolidate', '').lower().strip() in ['true', '1', 't', 'y', 'yes']
    # response.content_type = 'text/html; charset=utf-8'
    response.content_type = 'application/json; charset=utf-8'
    response.add_header('Access-Control-Allow-Origin', '*')
    # response.add_header('access-control-allow-credentials', 'true')
    # response.add_header('access-control-allow-headers', 'x-prototype-version,x-requested-with')
    response.add_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS, PUT')
    response.add_header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
    data = request.body.read()
    try:
        data = gzip.decompress(data)
    except:
        pass
    data = data.decode('utf8', errors='ignore')
    document = json.loads(data, object_hook=Dict)
    # document = json.loads(request.body.read().decode('utf8', errors='ignore'), object_hook=Dict)
    try:
        if not document.date:
            document.date = datetime.now().strftime("%Y-%m-%d")
        else:
            document.date = datetime.strptime(document.date, '%Y-%m-%d').date() # atpakaļ no serializētā stringa
        document.id = id
        # if not document.id:
        #     raise Exception("No document id")
        api = get_db(name)
        print('uploading document', id)
        dirtyEntities = upload2db(document, api)
        if consolidate:
            print('consolidating document', id, 'entities (%i dirty entities)' % len(dirtyEntities))
            process_entities(dirtyEntities, out_dir, api)
    except KeyboardInterrupt:
        print('interrupted')
        quit()
    except Exception as e:
        print(now(), 'Upload error:', str(e).strip())
        traceback.print_exc()
        print(now(), 'Upload error:', str(e).strip(), file=logf)
        traceback.print_exc(file=logf)
        log_flush()
        result = 'Upload error: ' + str(e)
        response.status = 500
        # result = traceback.format_exc()
        return result
    log_flush()
    response.status = 200
    return json.dumps(dirtyEntities)




run(app, host=listen_host, port=port)
