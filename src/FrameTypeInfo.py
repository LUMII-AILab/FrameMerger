#!/usr/bin/env python
# -*- coding: utf8 -*-
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

from openpyxl import load_workbook
import re
from pprint import pprint


FRAME_INFO_XLS_FILE_PATH = "./input/frames.xlsx"
FRAME_INFO_SHEET_NAME = "frames-new"

# column number where row type value is stored
ROW_ENTRY_TYPE__COLUMN = 2


#
# keys for decoding of the frame definiton rows 
#

# row type value for frame definition rows
ENTRY_TYPE_VALUE_FOR_FRAME_DEFINITION_ROW = "aFR"

# columns with lv and en names of a frame
LV_FRAME_NAME__COLUMN = 0
EN_FRAME_NAME__COLUMN = 1


#
# keys for decoding of frame roles
#

# row type value for frame role difinition rows
ENTRY_TYPE_VALUE_FOR_ROLE_DEFINITION_ROW = "mFE"

# columns with lv and en names of a frome role
LV_ROLE_NAME__COLUMN = 4
EN_ROLE_NAME__COLUMN = 5

# column with a class restrition for a role
ROLE_CLASS_RESTICTION__COLUMN = 6

# replace spaces with "-"
def make_class_name(name):
    return re.sub(r"\s+", "-", name)

class FrameTypeInfo(object):
    def __init__(self):
        """
        Loads FrameTypeInfo from an XLSX file.
        """

        # accumulator for frames
        self.frames = []

        # role names encountered in frame definitions
        self.roles = set()

        # entity names encountered in class restrictions inside role definitions
        self.entities = set()

        # load workbook with info about frames
        wb = load_workbook(FRAME_INFO_XLS_FILE_PATH)

        # load worksheet from the workbook that contains info about frames
        ws = wb.get_sheet_by_name(name = FRAME_INFO_SHEET_NAME)


        # current frame; used to accumulate roles
        current_frame = None

        # iterate ower each row in the worksheet
        for row in ws.rows:
            # if row type cell says that it is a frame definition then start new frame info accumulation
            if row[ROW_ENTRY_TYPE__COLUMN].value == ENTRY_TYPE_VALUE_FOR_FRAME_DEFINITION_ROW:
                # append previous frame to frames list
                if current_frame:
                    self.frames.append(current_frame)

                current_frame = {
                    'name_lv' : row[LV_FRAME_NAME__COLUMN].value,
                    'name_en' : row[EN_FRAME_NAME__COLUMN].value,
                    'roles' : []
                }

                # print row[LV_FRAME_NAME__COLUMN].value

            # if row type cell says that it is a role definition then store role info in current frame
            if row[ROW_ENTRY_TYPE__COLUMN].value == ENTRY_TYPE_VALUE_FOR_ROLE_DEFINITION_ROW:
                current_frame['roles'].append({
                    'name_lv' : row[LV_ROLE_NAME__COLUMN].value,
                    'name_en' : row[EN_ROLE_NAME__COLUMN].value,
                    'class_restriction' : make_class_name(row[ROLE_CLASS_RESTICTION__COLUMN].value) if row[ROLE_CLASS_RESTICTION__COLUMN].value else None
                })

                self.roles.add(row[EN_ROLE_NAME__COLUMN].value)

                if row[ROLE_CLASS_RESTICTION__COLUMN].value:
                    self.entities.add(make_class_name(row[ROLE_CLASS_RESTICTION__COLUMN].value)) 

                # print "\t" + row[LV_ROLE_NAME__COLUMN].value + " - " + (row[ROLE_CLASS_RESTICTION__COLUMN].value or u'')

        # add last frame to the frame list
        if current_frame:
            self.frames.append(current_frame)

    def frame_type_from_id(self, fr_type_id):
        return self.frames[fr_type_id]

    def frame_type_en_name_from_id(self, fr_type_id):
        return self.frame_type_from_id(fr_type_id)['name_en']

    def frame_role_from_frame_id_and_role_id(self, fr_type_id, role_type_id):
        # note that "role_type_id - 1", because in data it starts with 1
        return self.frame_type_from_id(fr_type_id)['roles'][role_type_id - 1]

    def frame_role_en_name_from_frame_id_and_role_id(self, fr_type_id, role_type_id):
        return self.frame_role_from_frame_id_and_role_id(fr_type_id, role_type_id)['name_en']

    def get_entities(self):
        return list(self.entities)




import datetime

DEFAULT_PREFIX = "http://lumii.lv/ontologies/LETA_Frames"
ONTOLOGY_VERSION = '0.1'

def frame_role_target_restrictions(frame_name, role_data):
    return """
        SubClassOf(Annotation(rdfs:comment "%(frame_name)s -- %(role_name_en)s --> %(role_target_class_restrictien_name)s") ObjectSomeValuesFrom(ObjectInverseOf(:%(role_name_en)s) :%(frame_name)s) :%(role_target_class_restrictien_name)s)
""" % {'role_name_en' : role_data['name_en'],
       'frame_name' : frame_name,
       'role_target_class_restrictien_name' : role_data['class_restriction']}


def frame_to_class(frame):
    return """
    Declaration(Class(:%(name_en)s))
        AnnotationAssertion(rdfs:label :%(name_en)s "%(name_en)s"@en)
        AnnotationAssertion(rdfs:label :%(name_en)s "%(name_lv)s"@lv)
        SubClassOf(:%(name_en)s :Frame)

        // role target class restrictions
        %(role_target_restrictions)s

""" % dict(frame.items() + ({'role_target_restrictions' : "".join([frame_role_target_restrictions(frame['name_en'], role) for role in frame['roles'] if role['class_restriction']])}).items())

# FIXME handle latvian entity name "Attiecību veids"
def entity_to_class(entity_name):
    return """
    Declaration(Class(:%(entity_name)s))
        SubClassOf(:%(entity_name)s :Entity)
""" % {'entity_name' : entity_name}


def owl_manchester_form(frames):
    print """
Prefix(:=<%(ontology_uri)s#>)
Prefix(xsd:=<http://www.w3.org/2001/XMLSchema#>)
Prefix(rdfs:=<http://www.w3.org/2000/01/rdf-schema#>)

Ontology(<%(ontology_uri)s>
<%(ontology_uri)s/%(ontology_version)s>
    Annotation(owl:versionInfo "autogenerated from frames.xslx on %(current_time)s"^^xsd:string)

    //
    // top level classes
    //

    Declaration(Class(:Entity))

    Declaration(Class(:Frame))

    DisjointClasses(:Entity :Frame)

    //
    // frames
    //

    %(frames)s

    DisjointClasses(%(frame_names)s)


    //
    // entities
    //

    %(entities)s

    //
    // roles
    //

)

""" % {
    'ontology_uri':DEFAULT_PREFIX,
    'ontology_version':ONTOLOGY_VERSION,
    'current_time': str(datetime.datetime.now()),
    'frames': "\n".join([frame_to_class(f) for f in frames['frames']]),
    'frame_names': " ".join([":" + f['name_en'] for f in frames['frames']]),
    'entities': "\n ".join([entity_to_class(entity) for entity in frames['entities']])
    }


def main():
    frame_type_info = FrameTypeInfo()

    pprint(frame_type_info.frame_type_from_id(0))

    print(frame_type_info.frame_type_en_name_from_id(0))
    print(frame_type_info.frame_role_en_name_from_frame_id_and_role_id(0, 1))

    pprint(frame_type_info.get_entities())

    # pprint(data)

    # owl_manchester_form(data)

if __name__ == "__main__":
    main()
