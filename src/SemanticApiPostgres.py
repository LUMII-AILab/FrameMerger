#!/usr/bin/env python
#coding=utf-8

import psycopg2, psycopg2.extensions
import atexit

# no FAQ - lai simbolus atgriež kā python unicode tipu
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

# FIXME - hardcodēta konekcijas info
default_conn_info = {
        "host":"alnis.ailab.lv", "port": 5555, "dbname":"semantica", "user":"martins", "password":"parole",
    }

class SemanticDbApi(object):

    def __init__(self, conn_info=None, dataset=None):
        # FIXME
        if conn_info is None:
            conn_info = default_conn_info

        if dataset is None:
            dataset = 0 # FIXME - te vajadzētu norādīt info par datu avotu, web-API ņēma lietotāja vārdu kā parametru

        self.conn = psycopg2.connect(
                host = conn_info["host"],
                port = conn_info["port"],
                dbname = conn_info["dbname"],
                user = conn_info["user"],
                password = conn_info["password"],
            )

        self.dataset = dataset

        self.cSearchByName = 0  # counteri DB operācijām
        self.cInsertEntity = 0  
        self.cInsertFrame = 0  

        atexit.register(self.finalize)


    # Saņem vārdu, atgriež sarakstu ar ID kas tiem vārdiem atbilst
    def searchByName(self, name):
        cursor = self.conn.cursor()
        cursor.execute("select entityid from entityothernames where name = %s", (name,) )
        r = cursor.fetchall()
        cursor.close()

        self.cSearchByName += 1

        return map(lambda x: x[0], r) # kursors iedod sarakstu ar tuplēm, mums vajag sarakstu ar tīriem elementiem

	# Inserto jaunu entītiju datubāzē
	# name - unicode string
	# othernames - list of unicode strings
	# category - integer code
	# outerids - list of unicode strings
	# inflections - unicode string
    def insertEntity(self, name, othernames, category, outerids=[], inflections = None):
        cursor = self.conn.cursor()

        cursor.execute(
            "INSERT INTO Entities(Name, OtherNames, OuterID, category, DataSet, NameInflections) VALUES (%s, %s, %s, %s, %s, %s) RETURNING EntityID;" ,
            (name, bool(othernames), bool(outerids), category, dataset, inflections) )
        entityid = cursor.fetchone()[0] # izņemam insertotās rindas id no tuples

        for othername in othernames:
            cursor.execute("INSERT INTO EntityOtherNames(EntityID, Name) VALUES (%s, %s)", (entityid, othername) )
        for outerid in outerids:
            cursor.execute("INSERT INTO EntityOuterIDs(EntityID, OuterID) VALUES (%s, %s)", (entityid, outerid) )       
        self.conn.commit()
        cursor.close()

        self.cInsertEntity += 1

        return entityid

	# Inserto jaunu freimu datubāzē
	# frametype - freima tipa kods kā int
	# elements - dict no freima-elementa koda uz entityID
	# document - dokumenta guid kā unicode string
	# sentenceId - teikuma id
	# targetword - unicode string
	# date - freima datums - string ISO datumformātā 
    def insertFrame(self, frametype, elements, document, source=None, sentenceId=None, targetword = None, date=None):
        cursor = self.conn.cursor()

        cursor.execute( "INSERT INTO Frames(FrameTypeID, SourceID, SentenceID, DocumentID, TargetWord, ApprowedTypeID, DataSet, Blessed, Hidden, Fdatetime)\
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING FrameID;" ,
                        (frametype, source, sentenceId, document, targetword, 0, dataset, None, False, date) )
        frameid = cursor.fetchone()[0] # izņemam insertotās freima id no tuples

        for element, entityid in elements.iteritems():
            cursor.execute("INSERT INTO FrameData(FrameID, EntityID, RoleID) VALUES (%s, %s, %s)", (frameid, entityid, element) )       
            # NB! Te nav validācijas; te arī neuzstāda wordindex lauku

        self.conn.commit()
        cursor.close()

        self.cInsertFrame += 1    

        return frameid

    # Statistika par DB requestiem to profilēšanai
    def finalize(self):
        print 'Kopā Postgres DB darbības:'
        print self.cSearchByName, 'searchByName'
        print self.cInsertEntity, 'insertEntity'
        print self.cInsertFrame, 'insertFrame'
        self.conn.commit()
        self.conn.close()

