#!/usr/bin/env python
# -*- coding: utf8 -*-

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import shelve
from pprint import pprint

import requests

from FrameInfo import FrameInfo

# TODO:
#  - pass the filename in as a parameter

f_info = FrameInfo("input/frames-new-modified.xlsx")

class EntityFrames(object):
    def __init__(self, api, entity_id):
        self.entity_id = entity_id

        self.load_data(api)

    def load_data(self, api):
        # load entity info
        try:
            self.entity = api.entity_data_by_id(self.entity_id)
        except requests.exceptions.ConnectionError, e:
            log.error("Error retrieving entity info for entity %s", self.entity_id)
            self.entity = None

        # load frames info
        if self.entity is not None:
            try:
                self.frames = api.entity_frames_by_id(self.entity_id)
            except requests.exceptions.ConnectionError, e:
                log.error("Error retrieving frames for entity %s", self.entity_id)
                self.frames = None

        else:
            self.frames = None

        # add frame counts
        if self.frames is not None:
            for item in self.frames:
                if item.get("FrameCnt") is None:
                    item["FrameCnt"] = 1

    def entity_name(self):
        # if cases when entity is not found, entity_data_by_id returns None

        if self.entity is not None:
            name = self.entity["Name"]
        else:
            name = "<ENF: Entity not found>"

        return name

    def set_consolidated_frames(self, frames):
        log.info("Setting consolidated frames for entity %s (%s frames).", self.entity_id, len(frames))
        self.cons_frames = frames

class EntitySummaryFrames(object):
    def __init__(self, api, entity_id):
        self.entity_id = entity_id

        self.load_data(api)

    def load_data(self, api):
        # load entity info
        try:
            self.entity = api.entity_data_by_id(self.entity_id)
        except requests.exceptions.ConnectionError, e:
            log.error("Error retrieving entity info for entity %s", self.entity_id)
            self.entity = None

        # load frames info
        if self.entity is not None:
            try:
                self.frames = api.entity_frames_by_id(self.entity_id)
            except requests.exceptions.ConnectionError, e:
                log.error("Error retrieving frames for entity %s", self.entity_id)
                self.frames = None

            self.summary_frames = api.summary_frame_data_by_id(self.entity_id)

        else:
            self.frames = None
            self.summary_frames = None

        # add frame counts
        if self.frames is not None:
            for item in self.frames:
                if item.get("FrameCnt") is None:
                    item["FrameCnt"] = 1


def frame_key(frame):

    def frame_data_set(frame):
        data = frozenset((item["Key"], item["Value"]["Entity"]) for item in frame["FrameData"])
        return data

    # XXX - what happens if one of the frames is missing an element that the other has?
    #  - it will be treated as different frame in comparisons

    # Q: is it safer to use tuples and not set() for frame element values?
    #  - A: tuple(..., set()) comparison should work as expected

    return (frame["FrameType"], frame_data_set(frame))

def summary_frame_key(frame):

    def summary_frame_data_set(frame):
        data = frozenset((item["roleid"], item["entityid"]) for item in frame["FrameData"])
        return data

    # XXX - what happens if one of the frames is missing an element that the other has?
    #  - it will be treated as different frame in comparisons

    # Q: is it safer to use tuples and not set() for frame element values?
    #  - A: tuple(..., set()) comparison should work as expected

    return (frame["FrameType"], summary_frame_data_set(frame))

class ConflictingFramesError(Exception):
    """
    Exception raised when element value conflicts are detected when merging frames.
    """
    pass

def update_element_if_not_conflicting(frame, e_id, value):
    # if element not present in frame:
    #  - set element
    # if value same as 

    # XXX: need a more efficient way to manipulate elements in FrameData (!)
    #  - when updating, need to scan frame["FrameData"] = BAD (!!!)

    # make frame elements dict
    fr_elements = dict(frame_key(frame)[1])

    # if element not present in frame, set it
    if e_id not in fr_elements:
        frame["FrameData"].append(
            gen_element(e_id, value)
        )

    # if equal do nothing
    elif value == fr_elements[e_id]:
        pass

    else:
        raise ConflictingFramesError("Conflicting frame element values detected when merging!")

    return frame

def frame_core(frame):

    def frame_core_set(frame):
        core_elements = f_info.get_core_elements(frame["FrameType"])

        data = frozenset((item["Key"], item["Value"]["Entity"]) for item in frame["FrameData"]
                    if item["Key"] in core_elements)
        return data

    return (frame["FrameType"], frame_core_set(frame))

def frames_equal(fr1, fr2):
    return frame_key(fr1) == frame_key(fr2)

def frame_lists_are_equal(list1, list2):
    key_set1 = set(frame_key(x) for x in list1)
    key_set2 = set(frame_key(x) for x in list2)

    return key_set1 == key_set2

def all_frames_unique(frame_list):
    # no duplicate frames (by value) present in the list
    frame_key_set = set(frame_key(x) for x in frame_list)
    return len(frame_key_set) == len(frame_list)

def all_frame_cores_unique(frame_list):
    # no duplicate frame cores present in the list
    frame_key_set = set(frame_core(x) for x in frame_list)
    return len(frame_key_set) == len(frame_list)

def frame_in_set_by_value(frame_list, frame):
    frame_key_set = set(frame_key(x) for x in frame_list)
    return frame_key(frame) in frame_key_set

def frame_in_set_by_id(frame_list, fr_id):
    # params:
    #  - fr_id = int
    return fr_id in set(frame["FrameId"] for frame in frame_list)

def get_frames_by_value(frame_list, sample_frame):
    sample = frame_key(sample_frame)
    return filter(lambda x: sample == frame_key(x), frame_list)

def get_frame_cnt(frame):
    if frame is None:
        return 0

    return frame["FrameCnt"]

def frame_cnt_by_value(frame_list, sample_frame):
    # NOTES: assumes there is only one matching frame ???
    #  - but do we need this assumption here?
    #  - other options:
    #     - return the sum of all frame counts
    #     - return a list of all frame counts (does not make sense, of what value is it?)
    #     - return a tuple: (sum_of_counts, cnt_of_counts)

    matching = get_frames_by_value(frame_list, sample_frame)

    # assume at most one matching frame (but see the NOTES above)
    assert(len(matching) <= 1)
   
    # return 0 if no matching frames (= makes sense)
    if len(matching) == 0:
        return 0

    # TEST all cases:
    #  - when no matching found
    #  - when 1 matching frame
    #  - when many matching frames
    return get_frame_cnt(matching[0])

def gen_element(k, v):
    return {
               u"Key": k,
               u"Value": {
                   u"Entity": v,
                   u"PlaceInSentence": 0
               },
           }

def create_frame(fr_type, fr_dict, fr_id=0):
    frame_data = []
    for k, v in fr_dict.viewitems():
        frame_data.append(
            gen_element(k, v)
        )

    frame = {
        "FrameData": frame_data,
        "FrameId": 0,
        "FrameType": fr_type,
        "FrameCnt": 1,
    }

    return frame

def test_frame_fns():

    fr1 = create_frame(1, {1: 2444, 3: 222, 4: 99})
    fr2 = create_frame(1, {4: 99, 1: 2444, 3: 222})
    fr3 = create_frame(1, {4: 99, 1: 2444, 3: 222, 5:444})
    assert frames_equal(fr1, fr2)
    assert not frames_equal(fr2, fr3)
        
def main():
    test_frame_fns()

if __name__ == "__main__":
    main()


