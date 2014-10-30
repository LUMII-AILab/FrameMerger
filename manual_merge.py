#!/usr/bin/env python
# coding=utf-8
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.

import sys, readline

sys.path.append("./src")
from SemanticApiPostgres import SemanticApiPostgres, PostgresConnection
from db_config import api_conn_info

def main():
	conn = PostgresConnection(api_conn_info)
	api = SemanticApiPostgres(conn)

	print('Entītiju apvienošana tagad notiks !')
	entity_to = raw_input('Entītijas ID, kurai pievienos (šī entītija paliks) : ')
	e_to = api.entity_data_by_id(entity_to)
	if not e_to:
		raise Exception('Entītija #%s nav atrasta :(' % entity_to)
	print()

	entity_from = raw_input('Entītijas ID, kuru pievienos (šī entītija tiks dzēsta) : ')
	e_from = api.entity_data_by_id(entity_from)
	if not e_from:
		raise Exception('Entītija #%s nav atrasta :(' % entity_from)
	print()

	print(u"Entītija '%s' tiks likvidēta un pievienota entītijai '%s' %s" % (e_from.get('Name'), e_to.get('Name'), e_to.get('OtherName')))
	verify = raw_input('Vai tiešām apvienot? (y/N) : ')
	if verify in ['y','Y']:
		api.mergeEntities(entity_from, entity_to)
		print('Darīts!')
	else:
		print('Atcelts!')


# ---------------------------------------- 

if __name__ == "__main__":
    main()