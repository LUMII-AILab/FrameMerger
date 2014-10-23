#!/usr/bin/env python
# -*- coding: utf8 -*-
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

import json
import operator
import requests     # http://docs.python-requests.org/en/latest/index.html

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import shelve
from pprint import pprint, pformat

from EntityFrames import EntityFrames

class EntityDb(object):
    def __init__(self, api):
        FNAME = "entity_db"

        self._api = api
        log.info("EntityDb: opening persistent DB @ file [%s]", FNAME)
        self.db = shelve.open(FNAME)

    def add_entity(self, e_id):
        e_id = str(e_id)

        if e_id in self.db:
            log.warning("Entity ID %s already present in the EntityDb", e_id)

        self.db[e_id] = EntityFrames(self._api, e_id)
        log.info("EntityDb: entity %s added: %s", e_id, self.db[e_id].entity_name())

    def sync(self):
        log.info("EntityDb: syncing persistent DB")
        self.db.sync()

    def close(self):
        log.info("EntityDb: closing persistent DB")
        self.db.close()
        self.db = None

    def __del__(self):
        if self.db is not None:
            self.close()

class EntityFrames(object):
    def __init__(self, api, entity_id):
        self.entity_id = entity_id
        self._api = api

        self.load_data()

    def load_data(self):
        # load entity info
        try:
            self.entity = self._api.entity_data_by_id(self.entity_id)
        except requests.exceptions.ConnectionError, e:
            log.error("Error retrieving entity info for entity %s", self.entity_id)
            self.entity = None

        # load frames info
        if self.entity is not None:
            try:
                self.frames = self._api.entity_frames_by_id(self.entity_id)
            except requests.exceptions.ConnectionError, e:
                log.error("Error retrieving frames for entity %s", self.entity_id)
                self.frames = None

        else:
            self.frames = None

    def entity_name(self):
        # if cases when entity is not found, entity_data_by_id returns None

        if self.entity is not None:
            name = self.entity["Name"]
        else:
            name = "<ENF: Entity not found>"

        return name

class SemanticApi(object):

    ServiceURI = "http://sps.inside.lv:789/Rest/Semantic/"
    UserID = "UldisB"

    def __init__(self, timeout = 60):
        self.timeout = timeout

    def api_call(self, method, data, debug=False):

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        # set up POST req
        data_out = json.dumps(data)
        req_uri = SemanticApi.ServiceURI + method + "/" + SemanticApi.UserID

        try:
            # make POST req
            r = requests.post(req_uri, data_out, headers=headers, timeout = self.timeout)
            log.debug("API call: %s\n%s", req_uri, data_out) 
            #print r.text
        except requests.exceptions.Timeout, e:
            print "Timeout at API when posting", data_out
            raise
        except requests.exceptions.ConnectionError, e:
            log.exception("HTTP error in SemanticApi.api_call():\n\t%s", data_out)
            raise

        # parse returned result
        #  - note: will throw an exception if JSON result can not be parsed

        try:
            output = r.json()
        except ValueError, e:
            log.error("Can not decode JSON response from the API. Fatal error report from the API?")
            log.info("HTTP POST response:\n%s", r.text)
            raise

        if debug:
            log.debug("HTTP POST response:\n%s", r.text)

        return r.json()

    def entity_data_by_id(self, e_id):
        data = self.entities_by_id([e_id])["Answers"]

        if data[0]["Answer"] == 0:
            entity = data[0]["Entity"]

        else:
            log.warning("Entity %s not found. Error code [%s], message: %s" % (e_id, data[0]["Answer"], data[0]["AnswerTypeString"]))
            entity = None

        return entity

    def entity_frames_by_id(self, e_id):
        data = self.get_frames([e_id])["Answers"]

        if data[0]["Answer"] == 0:
            frames = data[0]["FrameData"]

        else:
            log.warning("No frames found for entity %s. Error code [%s], message: %s" % (e_id, data[0]["Answer"], data[0]["AnswerTypeString"]))
            frames = []

        return frames

    def entity_summary_frames_by_id(self, e_id):
        data = self.get_summary_frames([e_id])["Answers"]

        if data[0]["Answer"] == 0:
            frames = data[0]["FrameData"]

        else:
            log.warning("No summary frames found for entity %s. Error code [%s], message: %s" % (e_id, data[0]["Answer"], data[0]["AnswerTypeString"]))
            frames = []

        return frames

    def entities_by_id(self, e_id_list):
        method = "GetEntityDataByIdPg"

        data = \
        { 
            "entityIdList":
            {
                "DataSet": ["AllUsers"],
                "SearchType": "AllData",
                "EntityIdList": e_id_list
            }
        }

        res = self.api_call(method, data)
        log.debug("Received entity info for entities %s:\n%s", repr(e_id_list), repr(res))

        return res

    def summary_frame_data_by_id(self, fr_id_list):
        method = "GetSummaryFrameDataByIdPg"

        data = \
        {
            "parameterList": 
            {
                "FrameIdList": [fr_id_list],
                "DataSet": ["AllData" ]
            }
        }

        res = self.api_call(method, data)
        log.debug("Received summary frame data for IDs %r:\n%r", fr_id_list, res)

        return res

    def delete_entity_summary_frames(self, e_id):
        """
        Delete all summary frames referring to a given entity ID.

        Parameters:
         - e_id (int) = entity ID
        """

        # FIXME: we only need frame_ids here, but are retrieving whole frame info packets
        entity_frames = self.entity_summary_frames_by_id(e_id)

        frame_ids = map(operator.itemgetter("FrameId"), entity_frames)

        return self.delete_summary_frames(frame_ids)

    def delete_summary_frames(self, fr_id_list):
        method = "DeleteSummaryFramePg"

        data = \
        {
            "parameterList": 
            {
                "FrameIdList": fr_id_list,
                "DataSet": ["AllData" ]
            }
        }

        res = self.api_call(method, data)
        log.debug("Deleting summary frames with IDs %r:\n%r", fr_id_list, res)

        return res

    def entity_ids_by_name(self, e_name_list):
        method = "GetEntityIdByNamePg"

        data = \
        { 
            "entityNameList":
            {
                "EntityNameList": e_name_list
            }
        }

        return self.api_call(method, data)

    def get_frames(self, e_id_list, f_types_list=None):
        method = "GetFramePg"

        if f_types_list is None:
            f_types_list = []

        data = \
        { "parameterList": 
            { "QueryParameters":
                [{
                    "EntityIdList": e_id_list,
                    "FrameTypes": f_types_list
                }]
            }
        }

        res = self.api_call(method, data)
        log.debug("Received frames for entity %s:\n%s", repr(e_id_list), repr(res))

        return res

    def get_summary_frames(self, e_id_list, f_types_list=None):
        method = "GetSummaryFramePg"

        if f_types_list is None:
            f_types_list = []

        data = \
        { "parameterList": 
            { "QueryParameters":
                [{
                    "EntityIdList": e_id_list,
                    "FrameTypes": f_types_list
                }]
            }
        }

        res = self.api_call(method, data)
        log.debug("Received summary frames for entity %s:\n%s", repr(e_id_list), repr(res))

        return res

    def insert_summary_frame(self, frame, data_set="",):

        def fix_frame_content(frame):
            # XXX changes frame contents (passed by reference), use carefully !

            if frame.get("TargetWord") is None:
                frame["TargetWord"] = ""

            if frame.get("FrameText") is None:
                frame["FrameText"] = ""
            
            # fix MergeType codes (!) as the API requires Int for now
            merge_type_map = {"O": 0, "M": 2, "E": 1}

            if frame["MergeType"] in merge_type_map:
                frame["MergeType"] = merge_type_map[frame["MergeType"]]

            if frame["FrameCnt"]<2 and not "LETA CV" in frame["SourceId"]:
                frame["IsHidden"] = True

            # frame.pop("IsDeleted", False)

            return frame

        method = "InsertSummaryFramePg"

        frame = fix_frame_content(frame)

        data = { "frameList": 
                    {"DataSet":[data_set], "FrameList": [frame]},
               }

        return self.api_call(method, data)
