# -*- coding: utf-8 -*-

import fileinput
from lxml import etree
import os

from geodata.address_formatting.formatter import AddressFormatter

#class AddressFormatter(object):
#	CATEGORY = 'category'
#	NEAR = 'near'
#	ATTENTION = 'attention'
#	CARE_OF = 'care_of'
#	HOUSE = 'house'
#	HOUSE_NUMBER = 'house_number'
#	PO_BOX = 'po_box'
#	ROAD = 'road'
#	BUILDING = 'building'
#	ENTRANCE = 'entrance'
#	STAIRCASE = 'staircase'
#	LEVEL = 'level'
#	UNIT = 'unit'
#	INTERSECTION = 'intersection'
#	SUBDIVISION = 'subdivision'
#	METRO_STATION = 'metro_station'
#	SUBURB = 'suburb'
#	CITY_DISTRICT = 'city_district'
#	CITY = 'city'
#	ISLAND = 'island'
#	STATE = 'state'
#	STATE_DISTRICT = 'state_district'
#	POSTCODE = 'postcode'
#	COUNTRY_REGION = 'country_region'
#	COUNTRY = 'country'
#	WORLD_REGION = 'world_region'


def license_xml_gz_reader(gz_filename):

	with fileinput.hook_compressed(gz_filename, "rb") as fi:
		parser = etree.iterparse(fi, events=("start", "end"))

		match_tag = license_xml_gz_header()
		is_address_place_begin = False

		for (event, elem) in parser:
			#print(event, elem.tag)
			if event == 'start' and elem.tag == 'address_place':
				is_address_place_begin = True
				record = {}
				continue

			if event == 'end' and elem.tag == 'address_place':
				is_address_place_begin = False
				empty = len(record)
				for val in record.values():
					if val == None:
						empty  -= 1

				if empty > 0:
					yield record
				continue

			if event == 'end':
				continue

			if is_address_place_begin and elem.tag in match_tag:
				idx = match_tag.index(elem.tag)
				record[idx] = elem.text
				continue


			"""
			http://lxml.de/parsing.html#modifying-the-tree
			Based on Liza Daly's fast_iter
			http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
			See also http://effbot.org/zone/element-iterparse.htm
			"""
			elem.clear()
			# Also eliminate now-empty references from the root node to elem
			for ancestor in elem.xpath('ancestor-or-self::*'):
				while ancestor.getprevious() is not None:
					del ancestor.getparent()[0]


def license_xml_gz_header():
	return ["index", "region", "city", "street"]