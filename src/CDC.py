#!/usr/bin/env python
# -*- coding: utf8 -*-
# 
# © 2013-2014 Institute of Mathematics and Computer Science, University of Latvia
# (LU aģentūra "Latvijas Universitātes Matemātikas un informātikas institūts")
#
# All rights reserved.
from __future__ import unicode_literals

from collections import Counter
import math

def namebag(entity):
    bag = Counter()
    for alias in entity.get('aliases'):
        bag.update(alias.split()) # vairākvārdu aliasiem ieliekam katru atsevišķo vārdu
    for pamatvaards in entity.get('representative').split():
    	bag[pamatvaards] = 0 # Izņemam uzvārdu, jo savādāk rezultātu var dominēt tīri tas vai uzvārda biežuma proporcija dokumentā/datos sakrīt ar 'etalonu', nevis reālie atribūti
    return bag

def mentionbag(entities):
	bag = Counter()
	for entity in entities:
		if (entity.get('type') == 'person') or (entity.get('type') == 'organization') or (entity.get('type') == 'location'):
			# bag.add(entity.get('GlobalID')) # šis ir korektāk, vienkārši uz debug laiku būs vārdi
			bag[entity.get('representative')] += 1
	return bag

def contextbag(entity, sentences): #TODO - šeit principā varētu ņemt nevis tikai 'tuvo' kontekstu, bet no visa dokumenta izvilkt vārdus kas raksturo tā tēmu (sports/politika/utml), bet tad vajag izdomāt kā sašķirot vārdus 'tematiskajos' un pārējos
	bag = Counter() 
	entityid = entity.get('id')
	for sentence in sentences:
		tokens = sentence['tokens']
		for netoken in tokens: # netoken - token containing the entity head
			if netoken.get('namedEntityID') == entityid: # if it's the correct entity
				headindex = netoken.get('index')
				for token in tokens[max(0,headindex-5) : headindex]:  # 5 word context before entity head
					bag[token['lemma']] += 1 # TODO - te vajag no features izvilkto leta_lemma lauku
				for token in tokens[headindex : headindex + 5]:  # 5 word context after entity head
					bag[token['lemma']] += 1 # TODO - te vajag no features izvilkto leta_lemma lauku
	return bag

def cosineSimilarity(vec1, vec2):
	intersection = set(vec1.keys()) & set(vec2.keys())
	numerator = sum([vec1[x] * vec2[x] for x in intersection])

	sum1 = sum([vec1[x]**2 for x in vec1.keys()])
	sum2 = sum([vec2[x]**2 for x in vec2.keys()])
	denominator = math.sqrt(sum1) * math.sqrt(sum2)

	if not denominator:
		return 0.0
	else:
		return float(numerator) / denominator