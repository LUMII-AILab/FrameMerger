#!/usr/bin/env python
#coding=utf-8

import psycopg2, psycopg2.extensions, psycopg2.extras
import atexit

from pprint import pprint

# no FAQ - lai simbolus atgriež kā python unicode tipu
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

# FIXME - hardcodēta konekcijas info
default_conn_info = {
        "host":"alnis.ailab.lv", "port": 5555, "dbname":"semantica", "user":"martins", "password":"parole",
    }

class PostgresConnection(object):

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

        # FIXME - XXX
        #  - statistika jāpārtaisa - nestrādās kad API operācijas ir iznestas atsevišķā klasē
        self.cSearchByName = 0  # counteri DB operācijām
        self.cInsertEntity = 0  
        self.cInsertFrame = 0  

        atexit.register(self.finalize)

    # Statistika par DB requestiem to profilēšanai
    def finalize(self):
        print 'Kopā Postgres DB darbības:'
        print self.cSearchByName, 'searchByName'
        print self.cInsertEntity, 'insertEntity'
        print self.cInsertFrame, 'insertFrame'
        self.conn.commit()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def new_cursor(self):
        # using DictCursor factory
        return self.conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) #DictCursor)

    def query(self, sql, parameters):
        cursor = self.new_cursor()
        cursor.execute(sql, parameters)
        r = cursor.fetchall()
        cursor.close()

        return r

    def insert(self, sql, parameters, returning=False, commit=False):
        cursor = self.new_cursor()

        cursor.execute(sql, parameters)
        if returning:
            result = cursor.fetchone()[0] # izņemam insertotā objekta id no tuples
        else:
            result = None

        if commit:
            self.commit()
        cursor.close()

        #self.cInsertEntity += 1

        return result


def first_col(seq):
    # FIXME change to generators if needed
    return [x[0] for x in seq]

class SemanticApiPostgres(object):
    def __init__(self, api):
        self.api = api

    def frame_ids_by_entity(self, e_id):
        sql = "select frameid from framedata where entityid = %s"
        res = self.api.query(sql, (e_id,) )
        #self.cSearchByName += 1

        frame_ids = first_col(res)
        return frame_ids

    def entity_frames_by_id(self, e_id):
        res = []

        for fr_id in self.frame_ids_by_entity(e_id):
            res.append(self.frame_by_id(fr_id))

        return res

    def frame_by_id(self, fr_id):
        """
   frameid          integer DEFAULT nextval ('frames_frameid_seq'::regclass),
   frametypeid      integer DEFAULT 0,
   sourceid         text,
   documentid       text,
   sentenceid       text,
   approwedtypeid   integer,
   weight           integer,
   deleted          boolean DEFAULT FALSE,
   dataset          integer DEFAULT 0,
   blessed          boolean,
   hidden           boolean DEFAULT FALSE,
   targetword       text,
   fdatetime        timestamp (6) WITHOUT TIME ZONE
"""
        sql = "select * from frames where frameid = %s"
        res = self.api.query(sql, (fr_id,) )
        frame = res[0]
        #self.cSearchByName += 1

        frame_info = {
             u'DocumentId': frame.documentid,   
             u'FrameData':  None,        # no frame element info queried yet
             u'FrameId':    frame.frameid,
             u'FrameMetadata': [{u'Key': u'Fdatetime', u'Value': frame.fdatetime}],
             u'FrameType':  frame.frametypeid,
             u'IsBlessed':  frame.blessed,
             u'IsDeleted':  frame.deleted,
             u'IsHidden':   frame.hidden,
             u'SentenceId': frame.sentenceid,
             u'SourceId':   frame.sourceid,
             u'TargetWord': frame.targetword,
        }

        frame_info[u"FrameData"] = self.frame_elements_by_id(fr_id)

        return frame_info

    def frame_elements_by_id(self, fr_id):
        """
   frameid      integer,
   entityid     integer,
   roleid       integer,
   roletypeid   integer DEFAULT 0,
   wordindex    integer
"""
        sql = "select * from framedata where frameid = %s"
        res = self.api.query(sql, (fr_id,) )
        #self.cSearchByName += 1

        elem_list = []

        for item in res:
            elem_list.append({
                u'Key': item.roleid, 
                u'Value': {u'Entity': item.entityid, u'PlaceInSentence': item.wordindex}
            })

        return elem_list

    def entity_data_by_id(self, e_id):
        """
   entityid          integer DEFAULT nextval ('entities_entityid_seq'::regclass),
   "name"            text,
   othernames        boolean DEFAULT FALSE,
   outerid           boolean DEFAULT FALSE,
   category          integer,
   deleted           boolean DEFAULT FALSE,
   blessed           boolean DEFAULT FALSE,
   dataset           integer DEFAULT 0,
   framecount        integer DEFAULT 0,
   hidden            boolean DEFAULT FALSE,
   nameinflections   text
"""
        sql = "select * from entities where entityid = %s"
        res = self.api.query(sql, (e_id,) )
        res = res[0]
        #self.cSearchByName += 1

        entity_info = {
            u'Category': res.category,
            u'EntityId': res.entityid,
            u'Name': res.name,
            u'NameInflections': res.nameinflections,
            u'OtherName': res.othernames,
            u'OuterId': res.outerid,
        }

        if res.othernames:
            entity_info["OtherName"] = \
                [item[0] for item in 
                    self.api.query("select name from entityothernames where entityid = %s", (e_id,))
                    if item[0] != res.name
                ]
        else:
            entity_info["OtherName"] = None

        if res.outerid:
            entity_info["OuterId"] = \
                [item[0] for item in 
                    self.api.query("select outerid from entityouterids where entityid = %s", (e_id,))
                ]
        else:
            entity_info["OuterId"] = None

        return entity_info 


    # Saņem vārdu, atgriež sarakstu ar ID kas tiem vārdiem atbilst
    def entity_ids_by_name_list(self, name):
        # atšķiras no SemanticApi.entity_ids_by_name ar to, ka šis atgriež
        # tikai entīšu sarakstu (kamēr SemanticApi.* atgriež JSON struktūru
        # kur ir "iepakots" ID saraksts)

        sql = "select entityid from entityothernames where name = %s"
        res = self.api.query(sql, (name,) )
        #self.cSearchByName += 1

        return map(lambda x: x[0], res) # kursors iedod sarakstu ar tuplēm, mums vajag sarakstu ar tīriem elementiem

	# Inserto jaunu entītiju datubāzē
	# name - unicode string
	# othernames - list of unicode strings
	# category - integer code
	# outerids - list of unicode strings
	# inflections - unicode string
    def insertEntity(self, name, othernames, category, outerids=[], inflections = None):
        main_sql = "INSERT INTO Entities(Name, OtherNames, OuterID, category, DataSet, NameInflections) VALUES (%s, %s, %s, %s, %s, %s) RETURNING EntityID;"

        res = self.api.insert(main_sql, (name, bool(othernames), bool(outerids), category, dataset, inflections),
                returning = True,
                commit = False)
        entityid = res # insertotās rindas id

        names_sql = "INSERT INTO EntityOtherNames(EntityID, Name) VALUES (%s, %s)"
        for othername in othernames:
            self.api.insert(names_sql, (entityid, othername) )

        outerid_sql = "INSERT INTO EntityOuterIDs(EntityID, OuterID) VALUES (%s, %s)"
        for outerid in outerids:
            self.api.insert(outerid_sql, (entityid, outerid) )       

        self.api.commit()

        #self.cInsertEntity += 1

        return entityid

	# Inserto jaunu freimu datubāzē
	# frametype - freima tipa kods kā int
	# elements - dict no freima-elementa koda uz entityID
	# document - dokumenta guid kā unicode string
	# sentenceId - teikuma id
	# targetword - unicode string
	# date - freima datums - string ISO datumformātā 
    def insertFrame(self, frametype, elements, document, source=None, sentenceId=None, targetword = None, date=None):
        main_sql = "INSERT INTO Frames(FrameTypeID, SourceID, SentenceID, DocumentID, TargetWord, ApprowedTypeID, DataSet, Blessed, Hidden, Fdatetime)\
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING FrameID;"

        res = self.api.insert(main_sql,
                (frametype, source, sentenceId, document, targetword, 0, dataset, None, False, date),
                returning = True,
                commit = False)
        frameid = res # insertotā freima id

        element_sql = "INSERT INTO FrameData(FrameID, EntityID, RoleID) VALUES (%s, %s, %s)"
        for element, entityid in elements.iteritems():
            self.api.insert(element_sql, (frameid, entityid, element) )       
            # NB! Te nav validācijas; te arī neuzstāda wordindex lauku

        self.api.commit()

        #self.cInsertFrame += 1    

        return frameid


# ------------------------------------------------------------  

def test_entity_select():
    conn = PostgresConnection(default_conn_info)
    api = SemanticApiPostgres(conn)

    id_list = api.searchByName("Imants Ziedonis")
    pprint(id_list)

def main():
    test_entity_select()

if __name__ == "__main__":
    main()
