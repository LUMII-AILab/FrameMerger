#!/usr/bin/env python
#coding=utf-8

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import psycopg2, psycopg2.extensions, psycopg2.extras, json
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

    def insert(self, sql, parameters, returning=False, commit=False): # TODO - šo bieži lieto arī update / delete saukšanai - maldinošs nosaukums
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
        log.info("starting SemanticApiPostgres instance")
        self.api = api

    def frame_ids_by_entity(self, e_id):
        sql = "select frameid from framedata where entityid = %s"
        res = self.api.query(sql, (e_id,) )
        #self.cSearchByName += 1

        frame_ids = first_col(res)
        return frame_ids

    def summary_frame_ids_by_entity(self, e_id):
        sql = "select distinct frameid from SummaryFrameRoleData where entityid = %s"
        res = self.api.query(sql, (e_id,) )

        frame_ids = first_col(res)
        return frame_ids

    def entity_frames_by_id(self, e_id):
        # TODO - te sanāk vairāki nestēti DB izsaukumi, visticamāk šeit ir consolidate_frames lēnākā daļa
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
        frame = res[0]  #FIXME - te nav pārbaudes vai rezultāts ir atrasts
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
    # name - unicode string
    # šī meklēšana ir case insensitive, un meklē arī alternatīvajos vārdos
    # LETA powerpointā gribēja saukt šo metodi 'ListEntity'
    def entity_ids_by_name_list(self, name):
        # atšķiras no SemanticApi.entity_ids_by_name ar to, ka šis atgriež
        # tikai entīšu sarakstu (kamēr SemanticApi.* atgriež JSON struktūru
        # kur ir "iepakots" ID saraksts)
        # sql = "select entityid from entityothernames where lower(name) = %s"
        sql = "select distinct e.entityid from entityothernames n join entities e on n.entityid = e.entityid where lower(n.name) = %s and e.deleted is false"
        res = self.api.query(sql, (name.lower().strip(),) )

        return map(lambda x: x[0], res) # kursors iedod sarakstu ar tuplēm, mums vajag sarakstu ar tīriem elementiem

	# Inserto jaunu entītiju datubāzē
	# name - unicode string
	# othernames - list of unicode strings
	# category - integer code
	# outerids - list of unicode strings
	# inflections - unicode string
    def insertEntity(self, name, othernames, category, outerids=[], inflections = None, hidden = False, commit = True):
        # debuginfo
        # if category == 2:
        #     print 'insertojam', category, name
        main_sql = "INSERT INTO Entities(Name, OtherNames, OuterID, category, DataSet, NameInflections, Hidden) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING EntityID;"

        res = self.api.insert(main_sql, (name, bool(othernames), bool(outerids), category, self.api.dataset, inflections, hidden),
                returning = True,
                commit = False)
        entityid = res # insertotās rindas id

        names_sql = "INSERT INTO EntityOtherNames(EntityID, Name) VALUES (%s, %s)"
        for othername in othernames:
            self.api.insert(names_sql, (entityid, othername) )


        outerid_sql = "INSERT INTO EntityOuterIDs(EntityID, OuterID) VALUES (%s, %s)"
        for outerid in outerids:
            self.api.insert(outerid_sql, (entityid, outerid) )       

        if commit:
            self.api.commit()

        return entityid

    # Atjauno entītijas datus, pārrakstot visus iepriekšējos
    # entityid - entītijas id, integer
    # name - unicode string
    # othernames - list of unicode strings
    # category - integer code
    # outerids - list of unicode strings
    # inflections - unicode string
    def updateEntity(self, entityid, name, othernames, category, outerids=[], inflections = None, commit = True):
        main_sql = "UPDATE Entities SET name = %s, OtherNames = %s, OuterID = %s, category = %s, DataSet = %s, NameInflections = %s where entityid = %s"
        self.api.insert(main_sql, (name, bool(othernames), bool(outerids), category, self.api.dataset, inflections, entityid))

        self.api.insert("DELETE FROM EntityOtherNames where entityid = %s", (entityid,) )
        names_sql = "INSERT INTO EntityOtherNames(EntityID, Name) VALUES (%s, %s)"
        for othername in othernames:
            self.api.insert(names_sql, (entityid, othername) )

        self.api.insert("DELETE FROM EntityOuterIDs where entityid = %s", (entityid,) )
        outerid_sql = "INSERT INTO EntityOuterIDs(EntityID, OuterID) VALUES (%s, %s)"
        for outerid in outerids:
            if outerid:
                self.api.insert(outerid_sql, (entityid, outerid) )       

        if commit:
            self.api.commit()


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
            # NB! Te ātrdarbības dēļ nav validācijas par to, vai entītiju un lomu id ir korekti; te arī nevar uzstādīt wordindex lauku (kuru gan šobrīd nekur nelieto)

        self.api.commit() # TODO - priekš dokumentu upload būtu efektīvāk necommitot pēc katras entītijas

        return frameid

    # Apdeito summary freima datus datubāzē, pārrakstot visus iepriekšējos
    # frametype - freima tipa kods kā int
    # elements - dict no freima-elementa koda uz entityID
    # document - dokumenta guid kā unicode string
    # sentenceId - teikuma id
    # targetword - unicode string
    # date - freima datums - string ISO datumformātā 
    # LETA powerpoint gribēja šo f-ju saukt 'UpdateFrame', taču pēc komentāra skaidrs ka runa iet par apkopotajiem freimiem
    def updateSummaryFrame(self, frameid, frametype, elements, document, frameText = None, summarizedFrames = [], source=None, sentenceId=None, targetword = None, date=None, blessed = None, hidden = False):
        main_sql = "UPDATE SummaryFrames SET FrameTypeID = %s, SourceID = %s, SentenceID = %s, DocumentID = %s, TargetWord = %s,\
                     Blessed = %s, Hidden = %s, Fdatetime = %s, FrameText = %s where frameid = %s"

        self.api.insert(main_sql,
                (frametype, source, sentenceId, document, targetword, blessed, hidden, date, frameText, frameid))

        self.api.insert("DELETE FROM SummaryFrameRoleData where FrameID = %s", (frameid, ))
        element_sql = "INSERT INTO SummaryFrameRoleData(FrameID, EntityID, RoleID) VALUES (%s, %s, %s)"
        for element, entityid in elements.iteritems():
            self.api.insert(element_sql, (frameid, entityid, element) )       
            # NB! Te ātrdarbības dēļ nav validācijas par to, vai entītiju un lomu id ir korekti; te arī nevar uzstādīt wordindex lauku (kuru gan šobrīd nekur nelieto)

        self.api.insert("DELETE FROM SummaryFrameData where SummaryFrameID = %s", (frameid, ))
        frame_sql = "INSERT INTO SummaryFrameData(SummaryFrameID, FrameID) VALUES (%s, %s)"
        for summarizedFrame in summarizedFrames:
            self.api.insert(frame_sql, (frameid, summarizedFrame) )       

        self.api.commit()

    #Apvieno summarizētajam freimam tikai 'apakšfreimu' sarakstu - lieto pie apvienošanas, blesotiem summaryfreimiem
    def updateSummaryFrameRawFrames(self, frameid, summarizedFrames, commit=True):
        self.api.insert("DELETE FROM SummaryFrameData where SummaryFrameID = %s", (frameid, ))
        frame_sql = "INSERT INTO SummaryFrameData(SummaryFrameID, FrameID) VALUES (%s, %s)"
        for summarizedFrame in summarizedFrames:
            self.api.insert(frame_sql, (frameid, summarizedFrame) )       

        if commit:
            self.api.commit()

    # Apvieno 2 entītijas
    # from - integer, entītijas ID, kuras freimi u.c. tiks pievienota otrai entītei un pati entīte izdzēsta
    # to - integer, entītijas ID uz kuru tas tiks pāradresēta
    def mergeEntities(self, entityFrom, entityTo, commit = True):
        self.api.insert("UPDATE FrameData set EntityID = %s where EntityID = %s", (entityTo, entityFrom))
        self.api.insert("UPDATE SummaryFrameRoleData set EntityID = %s where EntityID = %s", (entityTo, entityFrom))
        self.api.insert("UPDATE EntityMentions set EntityID = %s where EntityID = %s", (entityTo, entityFrom))
        # TODO - ja ieviesīs 'locations' lauku pie EntityMentions, tad tas šeit būtu jāapvieno vienā

        self.api.insert("UPDATE Entities set Deleted = True where EntityID = %s", (entityFrom, ))

        self.dirtyEntity(entityTo) #pēc šādas 'pāradresācijas' summaryFrames vajag pilnībā pārrēķināt nevis tikai samest pie vienas entity
        if commit:
            self.api.commit()

    # Izdzēš entītiju
    # EntityID - integer, entītija kuru izdzēst
    # fullDelete - boolean, vai neatgriezeniski izdzēst visus datus, vai arī tikai atzīmēt entītiju kā izdzēstu
    def deleteEntity(self, entityID, fullDelete = False):
        if fullDelete:
            self.api.insert("DELETE FROM FrameData where EntityID = %s", (entityID, ))
            self.api.insert("DELETE FROM SummaryFrameRoleData where EntityID = %s", (entityID, ))
            self.api.insert("DELETE FROM EntityMentions where EntityID = %s", (entityID, ))
            self.api.insert("DELETE FROM EntityOtherNames where EntityID = %s", (entityID, ))
            self.api.insert("DELETE FROM EntityOuterIDs where EntityID = %s", (entityID, ))

            self.api.insert("DELETE FROM Entities where EntityID = %s", (entityID, ))

        else:
            self.api.insert("UPDATE Entities set Deleted = True where EntityID = %s", (entityID, ))
        self.api.commit()

    def delete_entity_summary_frames_except_blessed(self, e_id, commit=True):
        """
        Delete summary frames [except blessed/anti-blessed] referring to a given entity ID.

        Parameters:
         - e_id (int) = entity ID
         - commit (bool) = if commit should be called (default: True)
        """

        sql = "select distinct fr.frameid from SummaryFrameRoleData fr_data \
join SummaryFrames fr on fr.frameid = fr_data.frameid \
where fr_data.entityid = %s and fr.blessed is null;"

        res = self.api.query(sql, (e_id,) )
        frame_ids = first_col(res)

        return self.delete_summary_frames(frame_ids, commit)

    def delete_all_entity_summary_frames(self, e_id, commit=True):
        """
        Delete all summary frames referring to a given entity ID.

        Parameters:
         - e_id (int) = entity ID
         - commit (bool) = if commit should be called (default: True)
        """

        frame_ids = self.summary_frame_ids_by_entity(e_id)

        return self.delete_summary_frames(frame_ids, commit)

    def delete_summary_frames(self, fr_id_list, commit=True):
        """
        Delete all summary frames with supplied <frame_IDs>.

        Parameters:
         - fr_id_list (list) = a list of summary frame IDs
         - commit (bool) = if commit should be called (default: True)
        """
        cursor = self.api.new_cursor()

        log.debug("Deleting summary frames with IDs %r.", fr_id_list)

        for fr_id in fr_id_list:
            cursor.execute("delete from SummaryFrameData where summaryframeid = %s", (fr_id,))
            cursor.execute("delete from SummaryFrameRoleData where frameid = %s", (fr_id,))
            cursor.execute("delete from SummaryFrames where frameid = %s", (fr_id,))

        if commit:
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
    def insert_summary_frame(self, frame, commit):
        main_sql = "INSERT INTO SummaryFrames(FrameTypeID, SourceID, SentenceID, DocumentID, TargetWord, SummaryTypeID, DataSet, Blessed, Hidden,\
                         FrameCnt, FrameText, SummaryInfo, Deleted)\
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING FrameID;"

        # fix MergeType codes (!) as the API requires Int for now
        merge_type_map = {"O": 0, "M": 2, "E": 1}

        if frame["MergeType"] in merge_type_map:
            merge_type = merge_type_map[frame["MergeType"]]
        else:
            merge_type = None

        if frame["IsBlessed"] is None:
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

        if commit:
            self.api.commit()

        return frameid

    # Pēc entītijas ID, atgriež visus summary freimus par viņu. 
    def summary_frame_data_by_id(self, entityID):
        cursor = self.api.new_cursor()
        main_sql = "select f.frameid, blessed, sourceid, frametypeid, summaryinfo, framecnt, json_agg(r) as elements from SummaryFrames f\
                    join (select frameid, roleid, entityid from SummaryFrameRoleData) r on r.frameid = f.frameid\
                    where f.frameid in (select frameid from SummaryFrameRoleData where entityid = %s)\
                    group by f.frameid, blessed, sourceid, frametypeid, summaryinfo, framecnt"
        cursor.execute(main_sql, (entityID,))
        r = []

        for frame in cursor.fetchall():            

            frame_info = {
                 # u'DocumentId': frame.documentid,   
                 u'FrameData':  frame.elements,
                 u'FrameId':    frame.frameid,
                 # u'FrameMetadata': [{u'Key': u'Fdatetime', u'Value': fdatetime}],
                 u'FrameType':  frame.frametypeid,
                 u'Blessed':  frame.blessed,
                 # u'IsDeleted':  frame.deleted,
                 # u'IsHidden':   frame.hidden,
                 # u'SentenceId': frame.sentenceid,
                 u'SourceId':   frame.sourceid,
                 # u'TargetWord': frame.targetword,
                 u'SummaryInfo': frame.summaryinfo,
                 u'FrameCnt': frame.framecnt,
            }
            r.append(frame_info)
        cursor.close()
        return r

    def summary_frame_by_id(self, fr_id):
        cursor = self.api.new_cursor()
        main_sql = "select f.frameid, blessed, sourceid, frametypeid, summaryinfo, framecnt, json_agg(r) as elements from SummaryFrames f\
                    join (select frameid, roleid, entityid from SummaryFrameRoleData) r on r.frameid = f.frameid\
                    where f.frameid = %s\
                    group by f.frameid, blessed, sourceid, frametypeid, summaryinfo, framecnt"
        cursor.execute(main_sql, (fr_id,))
        frame = cursor.fetchone()
        cursor.close()
        if not frame:
            return None # Ja nav atrasts šāds freims

        # Sakropļojam freima elementu info lai atbilst vecajam API
        # TODO - pārrakstīt patērētājfunkcijas, lai visur lieto normālo formu
        elem_list = []
        for item in frame.elements:
            elem_list.append({
                u'Key': item.get(u'roleid'), 
                u'Value': {u'Entity': item.get(u'entityid'), u'PlaceInSentence': item.get(u'wordindex')}
            })  

        return {
             # u'DocumentId': frame.documentid,   
             u'FrameData':  elem_list,
             u'FrameId':    frame.frameid,
             # u'FrameMetadata': [{u'Key': u'Fdatetime', u'Value': fdatetime}],
             u'FrameType':  frame.frametypeid,
             u'Blessed':  frame.blessed,
             # u'IsDeleted':  frame.deleted,
             # u'IsHidden':   frame.hidden,
             # u'SentenceId': frame.sentenceid,
             u'SourceId':   frame.sourceid,
             # u'TargetWord': frame.targetword,
             u'SummaryInfo': frame.summaryinfo,
             u'FrameCnt': frame.framecnt,
        }


    # Pēc entītijas ID, atgriež visus blessed/anti-blessed summary freimus par viņu. 
    def blessed_summary_frame_data_by_entity_id(self, entityID):
        cursor = self.api.new_cursor()
        main_sql = "select f.frameid, blessed, sourceid, frametypeid, summaryinfo, framecnt, json_agg(r) as elements from SummaryFrames f\
                    join (select frameid, roleid, entityid from SummaryFrameRoleData) r on r.frameid = f.frameid\
                    where f.frameid in (select frameid from SummaryFrameRoleData where entityid = %s)\
                      and blessed is not NULL\
                    group by f.frameid, blessed, sourceid, frametypeid, summaryinfo, framecnt"
        cursor.execute(main_sql, (entityID,))
        r = []

        for frame in cursor.fetchall():            

            frame_info = {
                 # u'DocumentId': frame.documentid,   
                 u'FrameData':  frame.elements,
                 u'FrameId':    frame.frameid,
                 # u'FrameMetadata': [{u'Key': u'Fdatetime', u'Value': fdatetime}],
                 u'FrameType':  frame.frametypeid,
                 u'Blessed':  frame.blessed,
                 # u'IsDeleted':  frame.deleted,
                 # u'IsHidden':   frame.hidden,
                 # u'SentenceId': frame.sentenceid,
                 u'SourceId':   frame.sourceid,
                 # u'TargetWord': frame.targetword,
                 u'SummaryInfo': frame.summaryinfo,
                 u'FrameCnt': frame.framecnt,
            }
            r.append(frame_info)
        cursor.close()
        return r

    # Pēc entītijas ID atrod tai atbilstošos nestrukturētos/plain text faktus
    def entity_text_facts(self, entity_id):
        cursor = self.api.new_cursor()
        main_sql = "select id, text from entitytextfacts where entityid = %s"
        cursor.execute(main_sql, (entity_id,))
        r = []
        for fact in cursor.fetchall():
            r.append(fact.text)
        cursor.close()
        return r

    def insertMention(self, entityID, documentID, chosen=True, cos_similarity=None, blessed=False, unclear=False, locations=None):
        cursor = self.api.new_cursor()
        cursor.execute("delete from entitymentions where entityid = %s and documentid = %s", (entityID, documentID) )
        locationsstr = None
        if locations:
            locationsstr = json.dumps(locations, separators=(',', ':'))
        # print entityID, '->', locationsstr
        cursor.execute("insert into entitymentions (entityid, documentid, chosen, cos_similarity, blessed, unclear, locations)\
                        values (%s, %s, %s, %s, %s, %s, %s)", (entityID, documentID, chosen, cos_similarity, blessed, unclear, locationsstr) )
        self.api.commit()
        cursor.close()

    # Kādas ir apstiprinātās entītijas attiecīgajam dokumentam
    def getBlessedEntityMentions(self, documentID):
        cursor = self.api.new_cursor()
        cursor.execute("select entityid from entitymentions where blessed is true and documentid = %s", (documentID, ) )
        r = cursor.fetchall()
        cursor.close()        
        return map(lambda x: x[0], r) # kursors iedod sarakstu ar tuplēm, mums vajag sarakstu ar tīriem elementiem

    # Kādos dokumentos šī entītija ir pieminēta
    def getDocsForEntity(self, entityID):
        cursor = self.api.new_cursor()
        cursor.execute("select documentid from entitymentions where chosen is true and entityid = %s", (entityID, ) )
        r = cursor.fetchall()
        cursor.close()        
        return map(lambda x: x[0], r) # kursors iedod sarakstu ar tuplēm, mums vajag sarakstu ar tīriem elementiem

    # Kādas entītijas ir pieminētas šajā dokumentā
    def getEntitiesForDoc(self, entityID):
        cursor = self.api.new_cursor()
        cursor.execute("select entityid from entitymentions where chosen is true and documentid = %s", (documentID, ) )
        r = cursor.fetchall()
        cursor.close()        
        return map(lambda x: x[0], r) # kursors iedod sarakstu ar tuplēm, mums vajag sarakstu ar tīriem elementiem

    # Pāradresē dokumentos minēto personu uz citu entītiju
    # fromID, toID - integer, entītiju ID
    # docs - list no dokumentu ID, kuros šo pāredresēt (paredzētais usecase - lietotājs ir izvēlējies 1+ dokumentus, kuros ir jālabo entītijas ID)
    def redirectDocsToEntity(self, fromID, toID, docs):
        cursor = self.api.new_cursor()
        cursor.execute("UPDATE EntityMentions SET chosen = FALSE, blessed = FALSE, unclear = FALSE where entityID = %s and documentID in %s", (fromID, tuple(docs) ) )
        cursor.execute("DELETE EntityMentions where entityID = %s and documentID in %s", (toID, tuple(docs) ) ) 
        for doc in docs:            
            cursor.execute("INSERT INTO EntityMentions (entityID, documentID, chosen, blessed, unclear) VALUES (%s, %s, TRUE, TRUE, FALSE)", (toID, doc ) ) 
            # Reinsertojam tāpēc, ka iespējams ka toID entītija pirms tam entitymentions nebija piemināta
        self.api.commit()
        cursor.close()

    # Atgriež sarakstu ar dokumentiem, kur šādu entītiju atzīmēja, ka nesanāk īsti izšķirt
    # name - unicode string
    def getUndecidedEntity(self, name):
        entityIDs = entity_ids_by_name_list(name) # entītijas, kas atbilst tam vārdam
        return api.query("SELECT documentID, array_agg(entityID) FROM entitymentions where unclear = TRUE and entityID in %s group by documentid", (tuple(entityIDs), ))

    # Norādīt redzamību summāram freimam
    # frameID - integer, summarizētā freimaID
    # visibility - boolean
    def changeVisible(self, frameID, visibility):
        self.api.insert("UPDATE SummaryFrames SET hidden = %s where frameid = %s", (not visibility, frameID) )
        self.api.commit()

    def blessEntity(self, entityID):
        self.api.insert("UPDATE Entities SET blessed = TRUE where entityid = %s", (entityID, ) )
        self.api.commit()

    def denyEntity(self, entityID):
        self.api.insert("UPDATE Entities SET blessed = FALSE where entityid = %s", (entityID, ) )
        self.api.commit()

    def blessSummaryFact(self, frameID):
        self.api.insert("UPDATE SummaryFrames SET blessed = TRUE where frameid = %s", (frameID, ) )
        self.api.commit()

    def denySummaryFact(self, frameID):
        self.api.insert("UPDATE SummaryFrames SET blessed = FALSE where frameid = %s", (frameID, ) )
        self.api.commit()

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
    # LETA powerpoint gribēja saukt par 'ReprocessSummary'
    def dirtyEntity(self, entityID, processID = None):
        if entityID == 0: return
        
        cursor = self.api.new_cursor()
        # Paskatamies, vai entītija jau nav rindā (kas būtu ļoti iespējams)
        cursor.execute("select entityid from dirtyentities where entityid = %s;", (entityID, ))
        r = cursor.fetchone()
        if not r: # ja nav tāds atrasts
            cursor.execute("insert into dirtyentities values (%s, 1, 0, 'now', %s);", (entityID, processID))
        else:
            cursor.execute("update dirtyentities set status = 1, updated = 'now', process_id = %s where entityid = %s", (processID, entityID))
        self.api.commit()
        cursor.close()

    # Atzīmē, ka dokumentu ar šādu ID pie izdevības vajadzētu paņemt un noprocesēt 
    # docs - list no dokumentu ID
    # priority - integer, prioritātes kods, lai nu kā LETA fabrika tos lietos
    def reprocessDoc(self, docs, priority = 0):
        cursor = self.api.new_cursor()
        for docID in docs:
            # Paskatamies, vai dokuments jau nav rindā
            cursor.execute("select documentid from dirtydocuments where documentid = %s;", (docID, ))
            r = cursor.fetchone()
            if not r: # ja nav tāds atrasts
                cursor.execute("insert into dirtydocuments values (%s, 1, %s, 'now', null);", (docID, priority))

        self.api.commit()
        cursor.close()

    # Uzstāda dokumenta apstrādes ierakstam (dirtydocuments) norādīto statusa kodu
    def setDocProcessingStatus(self, documentID, processID, status):
        cursor = self.api.new_cursor()
        cursor.execute("update dirtydocuments set status = %s, process_id = %s where documentid = %s", (status, processID, documentID))
        cursor.close()
        self.api.commit()

    # Uzstāda entītiju sarakstam (katrai entītijai) norādīto statusa kodu tabulā dirtyentities
    def setEntityProcessingStatus(self, entities, processID, status):
        cursor = self.api.new_cursor()
        cursor.execute("update dirtyentities set status = %s, process_id = %s where entityid = ANY(%s)", (status, processID, entities))
        cursor.close()
        self.api.commit()        

    # Ja šāds dokuments nav dokumentu tabulā, tad to pievieno
    def insertDocument(self, documentID, timestamp, compactText):
        cursor = self.api.new_cursor()
        cursor.execute("select documentid from documents where documentid = %s", (documentID, ))
        r = cursor.fetchone()
        if not r: # ja nav tāds atrasts
            cursor.execute("insert into documents (documentid, i_time, content) values (%s, %s, %s)", (documentID, timestamp, compactText))
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
