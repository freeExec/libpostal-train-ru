# -*- coding: utf-8 -*-

import fileinput
from lxml import etree
import os
import csv
import argparse
import six
import ftfy

from collections import OrderedDict

#from geodata.address_formatting.formatter import AddressFormatter
from geodata.encoding import safe_decode
from geodata.csv_utils import tsv_string

from fed_nadzor_zdrav_reestr_license_reader import license_xml_gz_header, license_xml_gz_reader


FED_ZGDAR_SEPARATE_DATA_FILENAME = 'license_separate_addresses.tsv'

COUNTRY = "ru"
LANGUAGE = "ru"


class AddressFormatter(object):
    CATEGORY = 'category'
    NEAR = 'near'
    ATTENTION = 'attention'
    CARE_OF = 'care_of'
    HOUSE = 'house'
    HOUSE_NUMBER = 'house_number'
    PO_BOX = 'po_box'
    ROAD = 'road'
    BUILDING = 'building'
    ENTRANCE = 'entrance'
    STAIRCASE = 'staircase'
    LEVEL = 'level'
    UNIT = 'unit'
    INTERSECTION = 'intersection'
    SUBDIVISION = 'subdivision'
    METRO_STATION = 'metro_station'
    SUBURB = 'suburb'
    CITY_DISTRICT = 'city_district'
    CITY = 'city'
    ISLAND = 'island'
    STATE = 'state'
    STATE_DISTRICT = 'state_district'
    POSTCODE = 'postcode'
    COUNTRY_REGION = 'country_region'
    COUNTRY = 'country'
    WORLD_REGION = 'world_region'


class HealthcareLicensesRUFormatter(object):

    field_map = OrderedDict([
        ('index', AddressFormatter.POSTCODE),
        ('region', AddressFormatter.STATE),
        ('district', AddressFormatter.STATE_DISTRICT),
        ('city', AddressFormatter.CITY),
        ('suburb', AddressFormatter.CITY_DISTRICT),
        ('street', AddressFormatter.ROAD),
        ('house_number', AddressFormatter.HOUSE_NUMBER),
        ('unit', AddressFormatter.UNIT),
    ])

    district_tokens = [
        u'район',
        u'с/с',
    ]

    city_tokens = [
        u'г.',
        u'город',
        u'пгт',
        u'п.г.т.',
        u'с.',
    ]

    suburb_tokens = [
        u'мкр',
        u'р-н',
        u'р-он',
        u'район',
        u'микрорайон',
    ]

    unit_tokens = [
        u'помещени',
        u'комнат',
        u'этаж',
        u'кв.',
        u'к.',
        u'пом',
        u'нп',
    ]

    MIN_TOKEN_START = 65535

    def fix_component_encodings(self, components):
        return {k: ftfy.fix_encoding(safe_decode(v)) for k, v in six.iteritems(components)}

    def split_composite_street_house(self, composite_street_house):
        if not composite_street_house:
            return (None, None)

        idx_first_sep = composite_street_house.find(",")
        if idx_first_sep == -1:
            return (composite_street_house, None)

        return (composite_street_house[:idx_first_sep], composite_street_house[idx_first_sep + 1:].strip())

    def try_move_city_from_street(self, components):
        composite_street = components.get(AddressFormatter.ROAD, None)
        if not composite_street:
            return components

        idx_first_sep = composite_street.find(",")
        if idx_first_sep == -1:
            return components

        city = composite_street[:idx_first_sep]
        for token in self.city_tokens:
            if city.find(token) != -1:
                components[AddressFormatter.ROAD] = composite_street[idx_first_sep + 1:].strip()
                components[AddressFormatter.CITY] = city
                break

        return components

    def try_move_district_from_street(self, components):
        composite_street = components.get(AddressFormatter.ROAD, None)
        if not composite_street:
            return components

        idx_first_sep = composite_street.find(",")
        if idx_first_sep == -1:
            return components

        disctrict = composite_street[:idx_first_sep]
        for token in self.district_tokens:
            if disctrict.find(token) != -1:
                components[AddressFormatter.ROAD] = composite_street[idx_first_sep + 1:].strip()
                components[AddressFormatter.STATE_DISTRICT] = disctrict
                break

        return components

    def try_move_suburb_from_street(self, components):
        composite_street = components.get(AddressFormatter.ROAD, None)
        if not composite_street:
            return components

        idx_first_sep = composite_street.find(",")
        if idx_first_sep == -1:
            return components

        suburb = composite_street[:idx_first_sep]
        for token in self.suburb_tokens:
            if suburb.find(token) != -1:
                components[AddressFormatter.ROAD] = composite_street[idx_first_sep + 1:].strip()
                components[AddressFormatter.CITY_DISTRICT] = suburb
                break

        return components

    def try_move_unit_from_house_number(self, components):
        composite_house_number = components.get(AddressFormatter.HOUSE_NUMBER, None)
        if not composite_house_number:
            return components

        if composite_house_number.find(",") == -1:
            return components

        house_number_lower = composite_house_number.lower()
        min_token_pos = self.MIN_TOKEN_START
        for token in self.unit_tokens:
            idx_sep = house_number_lower.find(token)
            if idx_sep >= 0 and idx_sep < min_token_pos:
                min_token_pos = idx_sep

        if min_token_pos == self.MIN_TOKEN_START:
            return components

        idx_sep_found = -1
        while(True):
            idx_sep = composite_house_number.find(",", idx_sep_found + 1)
            if idx_sep == -1:
                break
            if  idx_sep >= min_token_pos:
                break

            if idx_sep < min_token_pos:
                idx_sep_found = idx_sep

        if idx_sep_found != -1:
            components[AddressFormatter.HOUSE_NUMBER] = composite_house_number[:idx_sep_found]
            components[AddressFormatter.UNIT] = composite_house_number[idx_sep_found + 1:].strip()

        return components

    def formatted_addresses(self, path):
        reader = license_xml_gz_reader(path)
        headers = license_xml_gz_header()

        header_indices = {i: self.field_map[k] for i, k in enumerate(headers) if k in self.field_map}

        for row in reader:
            components = {}

            for i, key in six.iteritems(header_indices):
                value = row[i]
                if not value:
                    continue
                value = row[i].strip()
                if not value:
                    continue

                #if not_applicable_regex.match(value) or null_regex.match(value) or unknown_regex.match(value):
                #    continue

                value = value.strip(', -')

                #validator = self.component_validators.get(key, None)

                #if validator is not None and not validator(value):
                #    continue

                if value:
                    components[key] = value


            if components:
                components = self.fix_component_encodings(components)

                index = components.get(AddressFormatter.POSTCODE, None)
                if index == "0":
                    components.pop(AddressFormatter.POSTCODE)

                city = components.get(AddressFormatter.CITY, None)
                if city is None:
                    components = self.try_move_district_from_street(components)
                    components = self.try_move_city_from_street(components)

                city = components.get(AddressFormatter.CITY, None)
                if city:
                    components = self.try_move_suburb_from_street(components)

                composite = components.get(AddressFormatter.ROAD, None)
                if composite:
                    (street, house_number) = self.split_composite_street_house(composite)
                    if street:
                        components[AddressFormatter.ROAD] = street
                    if house_number:
                        components[AddressFormatter.HOUSE_NUMBER] = house_number
                        components = self.try_move_unit_from_house_number(components)

                if len(components) == 1 and AddressFormatter.STATE in components.keys():
                    continue

                yield tuple(components.get(v, '') for v in self.field_map.values())


    def build_prepare_csv_data(self, infile, out_dir):
        formatted_tagged_file = open(os.path.join(out_dir, FED_ZGDAR_SEPARATE_DATA_FILENAME), 'wb')
        writer = csv.writer(formatted_tagged_file, 'tsv_no_quote')

        writer.writerow(self.field_map.keys())

        i = 0
        #for post_code, region, district, city, suburb, street, house_number, unit in self.formatted_addresses(infile):
        for columns in self.formatted_addresses(infile):
            #if not formatted_address or not formatted_address.strip():
            #    continue

            row = []
            for col in columns:
                col = col.replace('"', '')
                col = tsv_string(col)
                row.append(col)

            writer.writerow(row)
            i += 1
            if i % 1000 == 0 and i > 0:
                formatted_tagged_file.flush()
                print('did {} formatted addresses'.format(i))



if __name__ == '__main__':
    # Handle argument parsing here
    parser = argparse.ArgumentParser()

    parser.add_argument('sources', nargs='*')
    parser.add_argument('-i', '--healthcare-licenses-ru-file',
                        help='Path to RU Goverment Healthcare Reestr Licenses.xml file')

    parser.add_argument('-o', '--out-dir',
                        default=os.getcwd(),
                        help='Output directory')

    args = parser.parse_args()

    if args.healthcare_licenses_ru_file:
        hl_formatter = HealthcareLicensesRUFormatter()
        hl_formatter.build_prepare_csv_data(args.healthcare_licenses_ru_file, args.out_dir)
    else:
        print(parser.format_usage())