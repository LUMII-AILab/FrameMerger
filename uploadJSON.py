#!/usr/bin/env python
# coding=utf-8

import sys, os, glob, fnmatch, json, codecs
from datetime import date, datetime

sys.path.append("./src")
from DocumentUpload import upload2db

basedir = os.path.dirname(os.path.realpath(__file__))

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
    
# komandrindas apstrāde
args = list(sys.argv[1:])

for arg in args:
    if arg == '--help':
        print 'JSON document upload to semantic DB'
        print
        print 'Usage: pass filenames to be processed through stdin, one filename per line'
        print
        quit()

sys.stderr.write( 'Starting...\n')
sys.stderr.write( 'Pass filenames to be processed through stdin, one filename per line\n')

try:
    for filename in sys.stdin:
        filename = filename.strip()
        if filename.endswith('.DS_Store'): # ar Mac menedžējot testadatus, vislaik ievazājas :(
            continue

        if not os.path.isabs(filename):
            filename = os.path.join(basedir, filename)
        basename = os.path.basename(filename)

        # sys.stderr.write('Input file: '+filename+' ... \n')

        try:
            if filename.endswith('.json'):
                with open(filename) as f:
                    document = json.load(f, object_hook=Dict)

                document.id = os.path.splitext(basename)[0]
                document.date = datetime.strptime(document.date, '%Y-%m-%d').date() # atpakaļ no serializētā stringa

                upload2db(document)
                sys.stdout.write(filename + "\tOK\n") # Feedback par veiksmīgi apstrādātajiem dokumentiem
        except Exception as e:
            print filename, '\tFail:\t', e

    sys.stderr.flush()

except KeyboardInterrupt:
    print 'Interrupted!'

