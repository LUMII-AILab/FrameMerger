#!/usr/bin/env python
#coding=utf-8

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import psycopg2, psycopg2.extensions, psycopg2.extras
from psycopg2.extras import Json
import atexit

from pprint import pprint

# no FAQ - lai simbolus atgriež kā python unicode tipu
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

class PostgresConnection(object):

    def __init__(self, conn_info=None, dataset=None):
        if conn_info is None or conn_info["host"] is None or len(conn_info["host"])==0:
            print "Postgres connection error: connection information must be supplied in <conn_info>"
            raise Exception("Postgres connection error: connection information must be supplied in <conn_info>")

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
        #print 'Kopā Postgres DB darbības:'
        #print self.cSearchByName, 'searchByName'
        #print self.cInsertEntity, 'insertEntity'
        #print self.cInsertFrame, 'insertFrame'
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

    def summary_frame_ids_by_entity(self, e_id):
        sql = "select frameid from SummaryFrameRoleData where entityid = %s"
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

        fdatetime = frame.fdatetime
        if fdatetime is not None:
            fdatetime = fdatetime.isoformat(" ") + "Z"      # fix date format to match JSON API

        frame_info = {
             u'DocumentId': frame.documentid,   
             u'FrameData':  None,        # no frame element info queried yet
             u'FrameId':    frame.frameid,
             u'FrameMetadata': [{u'Key': u'Fdatetime', u'Value': fdatetime}],
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

    def entity_data_by_id(self, e_id, alldata = True):
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
        if not alldata:
            sql = "select entityid, name, category from entities where deleted is false and entityid = %s"
        else:
            sql = "select e.entityid, e.name, e.category, e.nameinflections, array_agg(n.name) aliases, min(i.outerid) ids from entities e \
                        left outer join entityothernames n on e.entityid = n.entityid \
                        left outer join entityouterids i on e.entityid = i.entityid \
                        where e.entityid = %s and e.deleted is false \
                        group by e.entityid, e.name, e.category, e.nameinflections"

        res = self.api.query(sql, (e_id,) )
        if len(res) == 1:
            res = res[0]
        else:
            log.error('Entity ID '+str(e_id)+'not found in entity_data_by_id')
            return None

        entity_info = {
            u'Category': res.category,
            u'EntityId': res.entityid,
            u'Name': res.name,
        }

        if alldata:
            entity_info[u'NameInflections'] = res.nameinflections
            entity_info[u'OtherName'] = [item for item in res.aliases if item != res.name]
            entity_info[u'OuterId'] = [res.ids,]    # FIXME: vai entītei var būt vairāki ["OuterId"] ? ja jā, change the query

        return entity_info 


    # Saņem vārdu, atgriež sarakstu ar ID kas tiem vārdiem atbilst
    def entity_ids_by_name_list(self, name):
        # atšķiras no SemanticApi.entity_ids_by_name ar to, ka šis atgriež
        # tikai entīšu sarakstu (kamēr SemanticApi.* atgriež JSON struktūru
        # kur ir "iepakots" ID saraksts)
        # sql = "select entityid from entityothernames where lower(name) = %s"
        sql = "select distinct e.entityid from entityothernames n join entities e on n.entityid = e.entityid where lower(n.name) = %s and e.deleted is false"
        res = self.api.query(sql, (name.lower(),) )

        return map(lambda x: x[0], res) # kursors iedod sarakstu ar tuplēm, mums vajag sarakstu ar tīriem elementiem

	# Inserto jaunu entītiju datubāzē
	# name - unicode string
	# othernames - list of unicode strings
	# category - integer code
	# outerids - list of unicode strings
	# inflections - unicode string
    def insertEntity(self, name, othernames, category, outerids=[], inflections = None):
        main_sql = "INSERT INTO Entities(Name, OtherNames, OuterID, category, DataSet, NameInflections) VALUES (%s, %s, %s, %s, %s, %s) RETURNING EntityID;"

        res = self.api.insert(main_sql, (name, bool(othernames), bool(outerids), category, self.api.dataset, inflections),
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
                (frametype, source, sentenceId, document, targetword, 0, self.api.dataset, None, False, date),
                returning = True,
                commit = False)
        frameid = res # insertotā freima id

        element_sql = "INSERT INTO FrameData(FrameID, EntityID, RoleID) VALUES (%s, %s, %s)"
        for element, entityid in elements.iteritems():
            self.api.insert(element_sql, (frameid, entityid, element) )       
            # NB! Te nav validācijas; te arī neuzstāda wordindex lauku

        self.api.commit()

        return frameid

    def delete_entity_summary_frames(self, e_id):
        """
        Delete all summary frames referring to a given entity ID.

        Parameters:
         - e_id (int) = entity ID
        """
        frame_ids = self.summary_frame_ids_by_entity(e_id)

        return self.delete_summary_frames(frame_ids)

    def delete_summary_frames(self, fr_id_list):
    # Iztīra summary freimus no DB
        cursor = self.api.new_cursor()

        log.debug("Deleting summary frames with IDs %r.", fr_id_list)

        for fr_id in fr_id_list:
            cursor.execute("delete from SummaryFrameData where summaryframeid = %s", (fr_id,))
            cursor.execute("delete from SummaryFrameRoleData where frameid = %s", (fr_id,))
            cursor.execute("delete from SummaryFrames where frameid = %s", (fr_id,))

        self.api.commit()
        cursor.close()

    def entities_by_id(self):
        raise NotImplementedError("method not implemented")

    def entity_ids_by_name(self):
        # replaced by entity_ids_by_name_list()
        raise NotImplementedError("method not implemented")

    def entity_summary_frames_by_id(self):
        raise NotImplementedError("method not implemented")

    def get_frames(self):
        raise NotImplementedError("method not implemented")

    def get_summary_frames(self):
        raise NotImplementedError("method not implemented")

	# Inserto jaunu summary freimu datubāzē
	# frametype - freima tipa kods kā int
	# elements - dict no freima-elementa koda uz entityID
	# document - dokumenta guid kā unicode string
	# sentenceId - teikuma id
	# targetword - unicode string
	# date - freima datums - string ISO datumformātā 
    def insert_summary_frame(self, frame):
        main_sql = "INSERT INTO SummaryFrames(FrameTypeID, SourceID, SentenceID, DocumentID, TargetWord, SummaryTypeID, DataSet, Blessed, Hidden,\
                         FrameCnt, FrameText, SummaryInfo, Deleted)\
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING FrameID;"

        log.debug("Inserting summary frame:\n %r", frame)

        # fix MergeType codes (!) as the API requires Int for now
        merge_type_map = {"O": 0, "M": 2, "E": 1}

        if frame["MergeType"] in merge_type_map:
            merge_type = merge_type_map[frame["MergeType"]]
        else:
            merge_type = None

        if frame["FrameCnt"]<2 and not "LETA CV" in frame["SourceId"]:
            frame["IsHidden"] = True

        res = self.api.insert(main_sql,
                (frame["FrameType"], frame["SourceId"], frame["SentenceId"], frame["DocumentId"], frame["TargetWord"], 
                    merge_type, self.api.dataset, frame["IsBlessed"], frame["IsHidden"], frame["FrameCnt"], frame["FrameText"],
                    frame["SummaryInfo"], frame["IsDeleted"]),
                returning = True,
                commit = False)
        frameid = res # insertotā freima id

        element_sql = "INSERT INTO SummaryFrameRoleData(FrameID, EntityID, RoleID, WordIndex) VALUES (%s, %s, %s, %s)"

        # insert frame elements
        for entry in frame["FrameData"]:
            i_entity = entry["Value"]["Entity"]
            i_word_index = entry["Value"]["PlaceInSentence"]
            i_element = entry["Key"]

            self.api.insert(element_sql, (frameid, i_entity, i_element, i_word_index) )       
            # NB! Te nav validācijas

        relation_sql = "INSERT INTO SummaryFrameData(SummaryFrameID, FrameID) VALUES (%s, %s)"

        # record info about Summarized raw frames
        for raw_frame_id in frame[u"SummarizedFrames"]:
            self.api.insert(relation_sql, (frameid, raw_frame_id) )       

        self.api.commit()

        # form result report
        report = {"Answers":[
            {   
                "Answer": 0,
                "FrameId": frameid,
            },
        ]}

        return report

    # Pēc entītijas ID, atgriež visus summary freimus par viņu. 
    def summary_frame_data_by_id(self, entityID):
        cursor = self.api.new_cursor()
        main_sql = "select blessed, sourceid, frametypeid, json_agg(r) as elements from SummaryFrames f\
                    join (select frameid, roleid, entityid from SummaryFrameRoleData) r on r.frameid = f.frameid\
                    where f.frameid in (select frameid from SummaryFrameRoleData where entityid = %s)\
                    group by blessed, sourceid, frametypeid"
        cursor.execute(main_sql, (entityID,))
        r = []

        for frame in cursor.fetchall():            

            frame_info = {
                 # u'DocumentId': frame.documentid,   
                 u'FrameData':  frame.elements,
                 # u'FrameId':    frame.frameid,
                 # u'FrameMetadata': [{u'Key': u'Fdatetime', u'Value': fdatetime}],
                 u'FrameType':  frame.frametypeid,
                 u'Blessed':  frame.blessed,
                 # u'IsDeleted':  frame.deleted,
                 # u'IsHidden':   frame.hidden,
                 # u'SentenceId': frame.sentenceid,
                 u'SourceId':   frame.sourceid,
                 # u'TargetWord': frame.targetword,
            }
            r.append(frame_info)
        cursor.close()
        return r

    def insertMention(self, entityID, documentID, chosen=True, cos_similarity=None, blessed=False, unclear=False):
        cursor = self.api.new_cursor()
        cursor.execute("delete from entitymentions where entityid = %s and documentid = %s", (entityID, documentID) )
        cursor.execute("insert into entitymentions values (%s, %s, %s, %s, %s, %s)", (entityID, documentID, chosen, cos_similarity, blessed, unclear) )
        # TODO - validācija rezultātiem
        self.api.commit()
        cursor.close()

    # kādas ir apstiprinātās entītijas attiecīgajam dokumentam
    def getBlessedEntityMentions(self, documentID):
        cursor = self.api.new_cursor()
        cursor.execute("select entityid from entitymentions where blessed is true and documentid = %s", (documentID, ) )
        r = cursor.fetchall()
        cursor.close()        
        return map(lambda x: x[0], r) # kursors iedod sarakstu ar tuplēm, mums vajag sarakstu ar tīriem elementiem

    # Atgriež entītijas crossdocumentcoreference wordbagus pēc padotā entītijas ID
    def getCDCWordBags(self, entityid):
        cursor = self.api.new_cursor()
        cursor.execute("select wordbags from cdc_wordbags where entityid = %s", (entityid,) )
        r = cursor.fetchone()
        cursor.close()

        if r is None: # Ja ir rezultāts, izņemam no tuples, ja ieraksts nav atrasts, atgriežam None
            return None
        else:
            return r[0] 

    # Saglabā cross document coreference wordbagus attiecīgajai entītijai
    def putCDCWordBags(self, entityid, wordbags):
        cursor = self.api.new_cursor()
        cursor.execute("delete from cdc_wordbags where entityid = %s", (entityid,) )
        cursor.execute("insert into cdc_wordbags values (%s, %s)", (entityid, Json(wordbags)) )
        # TODO - validācija rezultātiem
        self.api.commit()
        cursor.close()

    # Iztīra rawfreimus no DB, kas atbilst šim dokumenta ID - lai atkārtoti laižot nekrājas dublicēti freimi; un lai laižot pēc uzlabojumiem iztīrās iepriekšējās versijas kļūdas
    def cleanupDB(self, documentID):
        cursor = self.api.new_cursor()
        cursor.execute("delete from framedata where frameid in \
                            (select A.frameid from frames as A where A.documentid = %s)", (documentID, ))
        cursor.execute("delete from frames where documentid = %s;", (documentID, ))
        self.api.commit()
        cursor.close()

    # Atzīmē, ka entītei ar šo ID ir papildinājušies dati un pie izdevības būtu jāpārlaiž summarizācija 
    def dirtyEntity(self, entityID):
        if entityID == 0: return
        
        cursor = self.api.new_cursor()
        # Paskatamies, vai entītija jau nav rindā (kas būtu ļoti iespējams)
        cursor.execute("select entityid from dirtyentities where status = 1 and entityid = %s;", (entityID, ))
        r = cursor.fetchone()
        if not r: # ja nav tāds atrasts
            cursor.execute("insert into dirtyentities values (%s, 1, 0, 'now', null);", (entityID, ))
        self.api.commit()
        cursor.close()

    # Ja šāds dokuments nav dokumentu tabulā, tad to pievieno
    def insertDocument(self, documentID, timestamp = None):
        cursor = self.api.new_cursor()
        cursor.execute("select documentid from documents where documentid = %s;", (documentID, ))
        r = cursor.fetchone()
        if not r: # ja nav tāds atrasts
            cursor.execute("insert into documents (documentid, i_time) values (%s, %s);", (documentID, timestamp))
        self.api.commit()
        cursor.close()

# ------------------------------------------------------------  

def test_entity_select(conn_info):

    conn = PostgresConnection(conn_info)
    api = SemanticApiPostgres(conn)

    id_list = api.entity_ids_by_name_list("Imants Ziedonis")
    pprint(id_list)

def main():
    import sys
    sys.path.append("..")

    from db_config import api_conn_info

    test_entity_select(api_conn_info)

if __name__ == "__main__":
    main()
