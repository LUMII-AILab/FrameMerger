#!/usr/bin/env python
# -*- coding: utf8 -*-

# enable logging, but default to null logger (no output)
import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import sys
sys.path.append("../src")

from nose.tools import eq_, ok_, nottest
from pprint import pprint

from FrameInfo import FrameInfo

from ConsolidateFrames import Consolidator
import EntityFrames as EF

# ---------------------------------------- 


def test_init_consolidator():
    """
    Test that Consolidator service can be instantiated.
    """

    c = Consolidator()
    assert c is not None

def test_copy_unique_core():
    """
    Test that frames with unique (different) core elements are not merged together.
    """

    c = Consolidator()

    fr1 = EF.create_frame(6, {1: 10, 3: 20, 4: 30})
    fr2 = EF.create_frame(6, {1: 110, 3: 120, 4: 130})
    fr3 = EF.create_frame(6, {1: 210, 2: 220, 3: 230})

    # ensure we use only core attributes in test data (fr1..fr3) here
    data_in = (fr1, fr2, fr3)
    data_out = c.apply(data_in)

    # preconditions
    assert EF.all_frame_cores_unique(data_in)
    assert all(EF.get_frame_cnt(x)==1 for x in data_in)

    # expect
    assert EF.frame_lists_are_equal(data_in, data_out)
    assert all(EF.get_frame_cnt(x)==1 for x in data_out)

def test_copy_non_unique_core():
    """ 
    Test that frames with same core elements are merged together.
    (assuming that all frames are merged = are not conflicting)
    """

    c = Consolidator()

    t = 6
    fr1 = EF.create_frame(t, {1: 10, 3: 20, 4: 30})
    fr2 = EF.create_frame(t, {1: 10})

    # ensure we use only core attributes in test data (fr1..fr3) here
    data_in = (fr1, fr2)
    data_out = c.apply(data_in)

    #f = FrameInfo("frames-new (1).xlsx")
    #print "Core:", f.get_core_elements(t)
    #print "Other:", f.get_other_elements(t)

    #raise 

    # preconditions
    assert not EF.all_frame_cores_unique(data_in)
    assert all(EF.get_frame_cnt(x)==1 for x in data_in)

    # just check that frames are consolidated
    #  - actual content is checked in other tests

    # expect
    assert len(data_out) == 1
    assert not all(EF.get_frame_cnt(x)==1 for x in data_out)
    eq_(EF.get_frame_cnt(data_out[0]), 2)

def test_copy_non_unique_core_preserves_details():
    """
    Test that more specific elements overwrite empty fields (in non-core positions) when merging.
    """

    c = Consolidator()

    t = 6
    fr1 = EF.create_frame(t, {1: 10})
    fr2 = EF.create_frame(t, {1: 10, 3: 20, 4: 30})

    # ensure we use only core attributes in test data (fr1..fr3) here
    data_in = (fr1, fr2)

    # preconditions
    assert not EF.all_frame_cores_unique(data_in)
    assert all(EF.get_frame_cnt(x)==1 for x in data_in)

    # check that the most detailed frame is preserved
    #  = the most specific frame gets carried on

    # TEST twice here to check with both combinations of frame order

    # expect #1
    data_out = c.apply(data_in)
    ok_(EF.frames_equal(data_out[0], fr2), "Most specific frame must be preserved (details not be lost)")
    eq_(EF.get_frame_cnt(data_out[0]), 2)

    # expect #2
    data_out = c.apply(data_in[::-1])
    ok_(EF.frames_equal(data_out[0], fr2), "Most specific frame must be preserved (details not be lost)")
    eq_(EF.get_frame_cnt(data_out[0]), 2)

def test_merge_exact_copies():
    """
    Test that exact copies are merged into 1 frame.
    """

    # Izglītība(6)	Core	Core	Core	Extra-Thematic	Peripheral	Peripheral
    t = 6

    fr1_1 = EF.create_frame(t, {1: 10, 2: 20, 4: 30, 5: 60})
    fr1_2 = EF.create_frame(t, {1: 10, 2: 20, 4: 30, 5: 60})
    fr1_3 = EF.create_frame(t, {1: 10, 2: 20, 4: 30, 5: 60})
    fr2 = EF.create_frame(t, {1: 110, 3: 120, 4: 130})

    c = Consolidator()
    data_in = (fr1_1, fr1_2, fr1_3, fr2)
    data_out = c.apply(data_in)

    # assert_equals
    eq_(EF.frame_cnt_by_value(data_out, fr1_1), 3)
    eq_(EF.frame_cnt_by_value(data_out, fr2), 1)

def test_merge_preserves_frame_ids():
    """
    Test that merged frame contains a list of original frames used in merging.
    """
        
    # Izglītība(6)	Core	Core	Core	Extra-Thematic	Peripheral	Peripheral
    t = 6

    fr1_1 = EF.create_frame(t, {1: 10, 2: 20, 4: 30, 5: 60}, fr_id=1001)
    fr1_2 = EF.create_frame(t, {1: 10, 2: 20, 4: 30, 5: 60}, fr_id=1002)
    fr1_3 = EF.create_frame(t, {1: 10, 2: 20, 4: 30, 5: 60}, fr_id=1003)
    fr2 = EF.create_frame(t, {1: 110, 3: 120, 4: 130}, fr_id=2001)

    c = Consolidator()
    data_in = (fr1_1, fr1_2, fr1_3, fr2)
    data_out = c.apply(data_in)
    res = EF.get_frames_by_value(data_out, fr1_1)

    # assert
    eq_(len(res), 1, "Only 1 merged frame must be returned matching given value")
    ok_("SummarizedFrames" in res[0])
    eq_(set(res[0]["SummarizedFrames"]), set(f["FrameId"] for f in (fr1_1, fr1_2, fr1_3))) 

def test_merge_add_frame_status_flags_ok():
    """
    Test that Consolidator() adds frame status flags ("Merged")
    """

    # Izglītība(6)	Core	Core	Core	Extra-Thematic	Peripheral	Peripheral
    t = 6

    fr1_1 = EF.create_frame(t, {1: 10, 2: 20, 4: 30, 5: 60}, fr_id=1001)
    fr1_2 = EF.create_frame(t, {1: 10, 2: 20, 4: 30, 5: 60}, fr_id=1002)
    fr1_3 = EF.create_frame(t, {1: 10, 2: 20, 4: 30, 5: 60}, fr_id=1003)

    c = Consolidator()
    data_in = (fr1_1, fr1_2, fr1_3)
    data_out = c.apply(data_in)
    res = EF.get_frames_by_value(data_out, fr1_1)

    pprint(res)

    # assert
    ok_("MergeType" in res[0])
    eq_(res[0]["MergeType"], "M", "MergeType of merged frames must be 'M'.")

### NOTE: not setting "Original" flag for now. - TEST IGNORED -

@nottest
def test_merge_add_frame_status_flags_original():
    """
    Test that Consolidator() adds frame status flags ("Original")
    """

    # Izglītība(6)	Core	Core	Core	Extra-Thematic	Peripheral	Peripheral
    t = 6

    fr1 = EF.create_frame(t, {1: 10, 3: 20, 4: 30})
    fr2 = EF.create_frame(t, {1: 110, 3: 120, 4: 130})
    fr3 = EF.create_frame(t, {1: 210, 2: 220, 3: 230})

    c = Consolidator()
    data_in = (fr1, fr2, fr3)
    data_out = c.apply(data_in)
    res = EF.get_frames_by_value(data_out, fr1)

    pprint(res)

    # assert
    ok_("MergeType" in res[0])

    # Q: do we want unchanged frames to be of status "Original"? 
    #    - or rather use "Merged" for all frames that did not have conflicts?
    eq_(res[0]["MergeType"], "O", "MergeType of unchanged frames must be 'O'.")
    
def test_merge_add_frame_status_flags_error():
    """
    Test that Consolidator() adds frame status flags ("Error")
    """

    # Izglītība(6)	Core	Core	Core	Extra-Thematic	Peripheral	Peripheral
    t = 6

    # 2 conflicting frames

    fr1 = EF.create_frame(t, {
            0: 10, 1: 11, 2: 12,        # Core
            3: 20, 4: 30,               # Other
        })
    fr2 = EF.create_frame(t, {
            0: 10, 1: 11, 2: 12,        # Core
            3: 60, 4: 70,               # Other
        })

    c = Consolidator()
    data_in = (fr1, fr2)
    data_out = c.apply(data_in)

    # assert
    assert all(EF.get_frame_cnt(x)==1 for x in data_out)

    # assert 1
    res = EF.get_frames_by_value(data_out, fr1)
    ok_("MergeType" in res[0])
    eq_(res[0]["MergeType"], "E", "MergeType of conflicting frames must be 'E'.")

    # assert 2
    res = EF.get_frames_by_value(data_out, fr2)
    ok_("MergeType" in res[0])
    eq_(res[0]["MergeType"], "E", "MergeType of conflicting frames must be 'E'.")

# ---------------------------------------- 

def main():
    pass

if __name__ == "__main__":
    main()


