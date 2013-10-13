#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
sys.path.append("./src")

import cPickle as pickle
from pprint import pprint

import EntityFrames as EF


def print_frames(e_id):
    """
    Print all frames (original and consolidated) for this entity, from the .pickle file.
    """

    fname = "output/%s" % (e_id)

    with open(fname + ".pickle", "rb") as f:
        data = pickle.load(f)

        # only 1 entity in the pickled object (for now)
        entity = data[0]

    # [i for i in fr_o.frames if i["FrameId"] == 1264893]

    pprint(entity.entity)
    print

    print "Saving frame data to files (<id>.orig and <id>.cons)"

    def sort_key_orig(f):
        return (f["FrameType"], sorted(k["Key"] for k in f["FrameData"]), f["FrameId"])

    def sort_key_cons(f):
        return (f["FrameType"], sorted(k["Key"] for k in f["FrameData"]), min(f["SummarizedFrames"]))

    def print_items(items, stream):
        before = None
        for item in items:
            if item["FrameType"] != before:
                # draw line separator
                before = item["FrameType"]
                print >>stream, "-"*40
                print >>stream

            pprint(item, stream = stream)

    with open(fname + ".orig", "w") as outf1:
        fr_orig = sorted(entity.frames, key = sort_key_orig)

        print_items(fr_orig, outf1)

    with open(fname + ".cons", "w") as outf2:
        fr_cons = sorted(entity.cons_frames, key = sort_key_cons)

        print_items(fr_cons, outf2)

    print " ... completed."

    print
    print "> Run:"
    print "vimdiff %s %s" % (fname + ".orig", fname + ".cons")

def main():
    try:
        e_id = sys.argv[1]
        e_id = int(e_id)

    except IndexError, e:
        usage()
        sys.exit(-1)

    print_frames(e_id)

# ---------------------------------------- 

def usage():
    print """
Usage:
    %s <entity_id>

Print all frames (original and consolidated) for this entity, from the .pickle file.
""" % (sys.argv[0],)

# ---------------------------------------- 

if __name__ == "__main__":
    main()



