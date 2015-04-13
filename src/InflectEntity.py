#!/usr/bin/env python
# coding=utf-8
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU agentura "Latvijas Universitates Matematikas un informatikas instituts")
#
# All rights reserved.

from __future__ import print_function
from __future__ import unicode_literals

import requests, re
try:    
    from urllib.request import quote # For Python 3.0 and later
except ImportError:    
    from urllib import quote # Fall back to Python 2's urllib

from db_config import inflection_webservice


def inflectEntity(name, category):
    # Nekonkretajam personu entitijam forma "Arvids (Petera Vaska tevs)" locijumos liekam tikai to dalu, kas ir arpus iekavam
    if category == 'person':
        match = re.match(r'([A-ZACEGIKLNŠUŽ]\w+) \(.*\)', name, re.UNICODE)
        if match:
            name = match.group(1)

    query = 'http://%s:%d/inflect_phrase/%s?category=%s' % (inflection_webservice.get('host'), inflection_webservice.get('port'), quote(name.encode('utf8')).replace('/','%2F'), category) 
    r = requests.get(query) 
    if r.status_code != 200:
        log.info("Error when calling %s -> code %s, message %s", query, r.status_code, r.text)
        return '{"Nominativs":%s}' % json.dumps(name, ensure_ascii=False)
    return r.text # TODO - check if valid JSON ?

def normalizeEntity(name, category):
    # Nekonkretajam personu entitijam forma "Arvids (Petera Vaska tevs)" locijumos liekam tikai to dalu, kas ir arpus iekavam
    if category == 'person':
        match = re.match(r'([A-ZACEGIKLNŠUŽ]\w+) \(.*\)', name, re.UNICODE)
        if match:
            name = match.group(1)

    query = 'http://%s:%d/normalize_phrase/%s?category=%s' % (inflection_webservice.get('host'), inflection_webservice.get('port'), quote(name.encode('utf8')).replace('/','%2F'), category) 
    r = requests.get(query) 
    if r.status_code != 200:
        log.info("Error when calling %s -> code %s, message %s", query, r.status_code, r.text)
        return name
    return r.text 

