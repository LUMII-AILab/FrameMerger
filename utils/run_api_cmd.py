#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
sys.path.append("./src")
sys.path.append(".")

from pprint import pprint

from db_config import api_conn_info
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection

def print_entity_frames(fr_id, api):

    data = api.frame_by_id(fr_id)

    pprint(data)

def print_entity_summary_frames(e_id, api):

#    data = api.summary_frame_data_by_id(e_id)
    data = api.blessed_summary_frame_data_by_entity_id(e_id)

#    data = filter(lambda x: x["Blessed"] is not None, data)

    pprint(data)


def main():

    conn = PostgresConnection(api_conn_info)
    api = SemanticApiPostgres(conn)
    
    entity_list = (1575689,)

    for e_id in entity_list:
        print_entity_summary_frames(e_id, api=api)

    frame_id = 2323056
    print_entity_frames(frame_id, api=api)

# ---------------------------------------- 

if __name__ == "__main__":
    main()

