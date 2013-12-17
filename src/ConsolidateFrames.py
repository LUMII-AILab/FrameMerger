#!/usr/bin/env python
# -*- coding: utf8 -*-

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import os
from datetime import datetime

import EntityFrames as EF

from copy import deepcopy
from pprint import pprint

# ---------------------------------------- 

INFO_STR = os.getenv("LOGNAME") + " | " + __name__ + ": "

def get_info_str(cons_obj):
    """
    Format info string to add to summary frames. Includes class name of the Consolidator used.
    """

    timestamp = datetime.now().strftime("%Y_%m_%d %H:%M:%S")
    cl_name = cons_obj.__class__.__name__

    return "%s %s | %s" % (INFO_STR, cl_name, timestamp)

# ---------------------------------------- 

class DummyConsolidator(object):
    """
    Return all incoming frames as-is.
    """
    
    def __init__(self):
        self.info_str = INFO_STR + self.__class__.__name__

    def apply(self, frame_list):
        # simple case - return frames as-is
        #  - add "O" (original frame) as frame status 

        res = []

        for f in frame_list:

            item = deepcopy(f)

            item["MergeType"] = "O"

            # "O" frames have only 1 source frame
            #  - setting this field anyway (or the API will complain)
            item["SummarizedFrames"] = [item["FrameId"]]

            item["SummaryInfo"] = get_info_str(self)

            res.append(item)

        return res


# TODO
#  - add tests for BaseConsolidator behaviour

class BaseConsolidator(object):
    """
    Consolidate together identical frames (duplicates).
    """

    def __init__(self):
        self.info_str = INFO_STR + self.__class__.__name__

    def apply(self, frame_list):
        # simple case - return frames as-is
        #  - add "O" (original frame) as frame status 

        res = []

        # put identical frames together
        res_cnt = {}
        res_buf = {}
        for f in frame_list:
            key = EF.frame_key(f)       # frame_key = (all frame elements)
            res_cnt[key] = res_cnt.get(key, 0) + 1
            res_buf.setdefault(key, []).append(f)

        # re-assemble result list
        res = []
        for key in res_cnt:

            item = deepcopy(res_buf[key][0])

            item["SummarizedFrames"] = [f["FrameId"] for f in res_buf[key]]

            if res_cnt[key] > 1:
                item["MergeType"] = "M"

                # TODO: remove elements that do not make sense in a summary

                all_docs = set(f["DocumentId"] for f in res_buf[key])
                # PP - vajag lai ir vismaz kautkāds links tur ir uz pirmavotu, tas ļoti palīdz freimera debugam
                # if len(all_docs) > 1:
                #     item["DocumentId"] = ""
                #item["SentenceId"] = ""

                all_docs = set(f["SourceId"] for f in res_buf[key])
                # if len(all_docs) > 1:
                #     item["SourceId"] = ""
                pass

            else:
                item["MergeType"] = "O"

            item["FrameCnt"] = res_cnt[key]

            item["SummaryInfo"] = get_info_str(self)

            res.append(item)

        return res

class Consolidator(object):

    def __init__(self):
        self.info_str = INFO_STR + __name__

    def apply(self, frame_list):
        # > simple / default case - return frames as-is
        if EF.all_frame_cores_unique(frame_list): 

            # optional: can add frame status "O" (original frame)
            return frame_list

        # create a copy of frames # <- will need a core-dict data struct here
        res_cnt = {}
        res_buf = {}
        for f in frame_list:
            key = EF.frame_core(f)
            res_cnt[key] = res_cnt.get(key, 0) + 1
            res_buf.setdefault(key, []).append(f)

        # re-assemble result list
        res = []
        for key in res_cnt:

            try:        # normal handling

            # ja izmanto frame_core() tad vairs nevar tālāk sūtīt
            # pirmo freimu, kas pagadās (!!!) - XXX

                item = deepcopy(res_buf[key][0])

                item = fill_other_elements(item, res_buf[key])     # fills in item fields from other frames

                # assumes all frames participated in merging - XXX
                #  - otherwise an exception would have been raised
                item["SummarizedFrames"] = [f["FrameId"] for f in res_buf[key]]

                # set "Merged" flag
                item["MergeType"] = "M"
                item["SummaryInfo"] = get_info_str(self)

                item["FrameCnt"] = res_cnt[key]

                res.append(item)

            except EF.ConflictingFramesError:

                for f in res_buf[key]:

                    item = deepcopy(f)

                    # set "Merged" flag
                    item["MergeType"] = "E"

                    # "E" frames have only 1 source frame
                    #  - setting this field anyway (or the API will complain)
                    item["SummarizedFrames"] = [item["FrameId"]]

                    item["SummaryInfo"] = get_info_str(self)

                    res.append(item)

        #pprint(res)

        return res

def fill_other_elements(item, frame_list):

    # leave equal elements as-is
    # merge those where one frame has a value more specific than the other

    for f in frame_list:
        key = EF.frame_key(f)

        for e_id, value in key[1]:
            # f = frame, e_id = all elements in frame
            # proceed if no conflicting value filled in <item> yet
            
            # throw an exception if there are conflicting values
            try:
                EF.update_element_if_not_conflicting(item, e_id, value)
            except EF.ConflictingFramesError, e:
                item_value = [v["Value"]["Entity"] for v in item["FrameData"] if v["Key"] == e_id][0]
                log.error("Conflicting frame element values when merging:\n  frame %s (%s: %s) vs. (%s: %s) [in previous frames]",  
                    f["FrameId"], e_id, value, e_id, item_value)
                raise


    return item
# ---------------------------------------- 

def main():
    pass

if __name__ == "__main__":
    main()

