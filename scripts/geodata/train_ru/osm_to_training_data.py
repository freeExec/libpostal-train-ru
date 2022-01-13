# -*- coding: utf-8 -*-

import argparse
import os

import csv
import six
import ftfy
import yaml

from collections import OrderedDict

from geodata.address_formatting.formatter import AddressFormatter
from geodata.openaddresses.formatter import OpenAddressesFormatter

from geodata.openaddresses.formatter import *

from geodata.addresses.components import AddressComponents

from geodata.address_formatting.aliases import Aliases

from geodata.address_expansions.gazetteers import *
from geodata.address_expansions.abbreviations import abbreviate

from geodata.encoding import safe_decode
from geodata.countries.constants import Countries
from geodata.csv_utils import tsv_string, unicode_csv_reader

from geodata.configs.utils import nested_get

from geodata.osm.extract import *

from geodata.i18n.languages import get_country_languages

FORMAT_DATA_TAGGED_FILENAME = "osm_formatted_addresses_tagged.tsv"
FORMAT_DATA_FILENAME = "osm_formatted_addresses.tsv"

this_dir = os.path.realpath(os.path.dirname(__file__))

csv.register_dialect('csv_no_quote', delimiter=';', quoting=csv.QUOTE_NONE, quotechar='')

OSM_PARSER_DATA_DEFAULT_CONFIG = os.path.join(this_dir, os.pardir, os.pardir, os.pardir,
                                              'resources', 'parser', 'data_sets', 'osm.yaml')

NAME_KEYS = (
    'name',
    'addr:housename',
)

HOUSE_NUMBER_KEYS = (
    'addr:house_number',
    'addr:housenumber',
    'house_number'
)

COUNTRY_KEYS = (
    'country',
    'country_name',
    'addr:country',
    'is_in:country',
    'addr:country_code',
    'country_code',
    'is_in:country_code'
)

POSTAL_KEYS = (
    'postcode',
    'postal_code',
    'addr:postcode',
    'addr:postal_code',
)

#class AddressComponentsSimple(object):
    #def __init__(self):
        #self.setup_component_dependencies()

        #self.setup_valid_scripts()


class OSMAddressRUFormatter(object):
    aliases = Aliases(
        OrderedDict([
            ('name', AddressFormatter.HOUSE),
            ('addr:housename', AddressFormatter.HOUSE),
            ('addr:housenumber', AddressFormatter.HOUSE_NUMBER),
            ('addr:house_number', AddressFormatter.HOUSE_NUMBER),
            ('addr:street', AddressFormatter.ROAD),
            ('addr:suburb', AddressFormatter.SUBURB),
            ('is_in:suburb', AddressFormatter.SUBURB),
            ('addr:neighbourhood', AddressFormatter.SUBURB),
            ('is_in:neighbourhood', AddressFormatter.SUBURB),
            ('addr:neighborhood', AddressFormatter.SUBURB),
            ('is_in:neighborhood', AddressFormatter.SUBURB),
            ('addr:barangay', AddressFormatter.SUBURB),
            # Used in the UK for civil parishes, sometimes others
            ('addr:locality', AddressFormatter.SUBURB),
            # This is actually used for suburb
            ('suburb', AddressFormatter.SUBURB),
            ('addr:city', AddressFormatter.CITY),
            ('is_in:city', AddressFormatter.CITY),
            ('addr:locality', AddressFormatter.CITY),
            ('is_in:locality', AddressFormatter.CITY),
            ('addr:municipality', AddressFormatter.CITY),
            ('is_in:municipality', AddressFormatter.CITY),
            ('addr:hamlet', AddressFormatter.CITY),
            ('is_in:hamlet', AddressFormatter.CITY),
            ('addr:quarter', AddressFormatter.CITY_DISTRICT),
            ('addr:county', AddressFormatter.STATE_DISTRICT),
            ('addr:district', AddressFormatter.STATE_DISTRICT),
            ('is_in:district', AddressFormatter.STATE_DISTRICT),
            ('addr:state', AddressFormatter.STATE),
            ('is_in:state', AddressFormatter.STATE),
            ('addr:province', AddressFormatter.STATE),
            ('is_in:province', AddressFormatter.STATE),
            ('addr:region', AddressFormatter.STATE),
            ('is_in:region', AddressFormatter.STATE),
            # Used in Tunisia
            ('addr:governorate', AddressFormatter.STATE),
            ('addr:postcode', AddressFormatter.POSTCODE),
            ('addr:postal_code', AddressFormatter.POSTCODE),
            ('addr:zipcode', AddressFormatter.POSTCODE),
            ('postal_code', AddressFormatter.POSTCODE),
            ('addr:country', AddressFormatter.COUNTRY),
            ('addr:country_code', AddressFormatter.COUNTRY),
            ('country_code', AddressFormatter.COUNTRY),
            ('is_in:country_code', AddressFormatter.COUNTRY),
            ('is_in:country', AddressFormatter.COUNTRY),
            #Used in Russian
            ('addr:place', AddressFormatter.SUBURB),
        ])
    )

    sub_building_aliases = Aliases(
        OrderedDict([
            ('level', AddressFormatter.LEVEL),
            ('addr:floor', AddressFormatter.LEVEL),
            ('addr:unit', AddressFormatter.UNIT),
            ('addr:flats', AddressFormatter.UNIT),
            ('addr:door', AddressFormatter.UNIT),
            ('addr:suite', AddressFormatter.UNIT),
        ])
    )

    def __init__(self):
        self.config = yaml.safe_load(open(OSM_PARSER_DATA_DEFAULT_CONFIG))
        self.formatter = AddressFormatter(scratch_dir=os.environ['TEMP'])
        #self.components = AddressComponentsSimple()
        self.components = AddressComponents(None, None, None)

    component_validators = {
        AddressFormatter.HOUSE_NUMBER: OpenAddressesFormatter.validators.validate_house_number,
        AddressFormatter.ROAD: OpenAddressesFormatter.validators.validate_street,
        AddressFormatter.POSTCODE: OpenAddressesFormatter.validators.validate_postcode,
    }

    cldr_country_probability = 0.3
    address_only_probability = 0.4
    drop_address_probability = 0.6
    drop_address_and_postcode_probability = 0.1

    abbreviate_city_probability = 0.3
    abbreviate_state_district_probability = 0.4

    exchange_type_position_probability = 0.25

    @classmethod
    def cleanup_number(cls, num, strip_commas=False):
        num = num.strip()
        if strip_commas:
            num = num.replace(six.u(','), six.u(''))
        try:
            num_int = int(num)
        except (ValueError, TypeError):
            try:
                num_float = float(num)
                leading_zeros = 0
                for c in num:
                    if c == six.u('0'):
                        leading_zeros += 1
                    else:
                        break
                num = safe_decode(int(num_float))
                if leading_zeros:
                    num = six.u('{}{}').format(six.u('0') * leading_zeros, num)
            except (ValueError, TypeError):
                pass
        return num

    def normalize_address_components(self, tags):
        address_components = {k: v for k, v in six.iteritems(tags) if self.aliases.get(k)}
        self.aliases.replace(address_components)
        address_components = {k: v for k, v in six.iteritems(address_components) if k in AddressFormatter.address_formatter_fields}
        return address_components

    def normalize_sub_building_components(self, tags):
        sub_building_components = {k: v for k, v in six.iteritems(tags) if self.sub_building_aliases.get(k) and is_numeric(v)}
        self.aliases.replace(sub_building_components)
        sub_building_components = {k: v for k, v in six.iteritems(sub_building_components) if k in AddressFormatter.address_formatter_fields}
        return sub_building_components

    def normalized_street_name(self, address_components, country=None, language=None):
        street = address_components.get(AddressFormatter.ROAD)
        if street and ',' in street:
            street_parts = [part.strip() for part in street.split(',')]

            if len(street_parts) > 1 and (street_parts[-1].lower() == address_components.get(AddressFormatter.HOUSE_NUMBER, '').lower()) and self.formatter.house_number_before_road(country, language):
                street = street_parts[0]
                return street

        return None

    def normalize_city_name(self, city_name, tags):
        official_status = tags.get('addr:city_official_status')
        if official_status and official_status[:3] == 'ru:':
            official_status = official_status[3:]
            city_name = official_status + ' ' + city_name

        return city_name


    def abbreviated_street(self, street, language):
        '''
        Street abbreviations
        --------------------

        Use street and unit type dictionaries to probabilistically abbreviate
        phrases. Because the abbreviation is picked at random, this should
        help bridge the gap between OSM addresses and user input, in addition
        to capturing some non-standard abbreviations/surface forms which may be
        missing or sparse in OSM.
        '''
        abbreviate_prob = float(nested_get(self.config, ('streets', 'abbreviate_probability'), default=0.0))
        separate_prob = float(nested_get(self.config, ('streets', 'separate_probability'), default=0.0))

        return abbreviate(street_and_synonyms_gazetteer, street, language,
                          abbreviate_prob=abbreviate_prob, separate_prob=separate_prob)

    def fix_component_encodings(self, tags):
        return {k: ftfy.fix_encoding(safe_decode(v)) for k, v in six.iteritems(tags)}

    def num_floors(self, buildings, key='building:levels'):
        max_floors = None
        for b in buildings:
            num_floors = b.get(key)
            if num_floors is not None:
                try:
                    num_floors = int(num_floors)
                except (ValueError, TypeError):
                    try:
                        num_floors = int(float(num_floors))
                    except (ValueError, TypeError):
                        continue

                if max_floors is None or num_floors > max_floors:
                    max_floors = num_floors
        return max_floors

    def formatted_places(self, address_components, country, language, tag_components=True):
        formatted_addresses = []

        place_components = self.components.drop_address(address_components)
        formatted_address = self.formatter.format_address(place_components, country, language=language,
                                                          tag_components=tag_components, minimal_only=False)
        formatted_addresses.append(formatted_address)

        if AddressFormatter.POSTCODE in address_components:
            drop_postcode_prob = float(nested_get(self.config, ('places', 'drop_postcode_probability'), default=0.0))
            if random.random() < drop_postcode_prob:
                place_components = self.components.drop_postcode(place_components)
                formatted_address = self.formatter.format_address(place_components, country, language=language,
                                                                  tag_components=tag_components, minimal_only=False)
                formatted_addresses.append(formatted_address)
        return formatted_addresses

    def formatted_addresses_with_venue_names(self, address_components, venue_names, country, language=None,
                                             tag_components=True, minimal_only=False):
        # Since venue names are only one-per-record, this wrapper will try them all (name, alt_name, etc.)
        formatted_addresses = []

        if not venue_names:
            address_components = {c: v for c, v in six.iteritems(address_components) if c != AddressFormatter.HOUSE}
            return [self.formatter.format_address(address_components, country, language=language,
                                                  tag_components=tag_components, minimal_only=minimal_only)]

        raise NotImplementedError("skip code")

    def formatted_addresses(self, tags, tag_components=True):

        #osm_components = self.components.osm_reverse_geocoded_components(latitude, longitude)
        osm_components = tags
        #country, candidate_languages = self.components.osm_country_and_languages(osm_components)
        country = Countries.RUSSIA
        candidate_languages = get_country_languages(country).items()

        all_local_languages = set([l for l, d in candidate_languages])

        # In the UK sometimes streets have "parent" streets and
        #combined_street = self.combine_street_name(tags)

        # random other language
        #namespaced_language = self.namespaced_language(tags, candidate_languages)

        language = None

        # railway station or postoffice
        #is_generic_place = self.is_generic_place(tags)
        # amenity shop airoway
        #is_known_venue_type = self.is_known_venue_type(tags)

        revised_tags = self.normalize_address_components(tags)

        #sub_building_tags = self.normalize_sub_building_components(tags)

        num_floors = None
        num_basements = None
        zone = None

        #postal_code = revised_tags.get(AddressFormatter.POSTCODE, None)

        #building_venue_names = []

        # get outside building tags -> fake
        #building_components = {}
        #if "building" in tags.keys():
        #    building_components = [tags] #self.building_components(latitude, longitude)

        #if building_components:
        #    num_floors = self.num_floors(building_components)
        #    num_basements = self.num_floors(building_components, key='building:levels:underground')

        #    for building_tags in building_components:
        #        building_tags = self.normalize_address_components(building_tags)

        #        #building_is_generic_place = building_is_generic_place or self.is_generic_place(building_tags)
        #        #building_is_known_venue_type = building_is_known_venue_type or self.is_known_venue_type(building_tags)

        #        for k, v in six.iteritems(building_tags):
        #            if k not in revised_tags and k in (AddressFormatter.HOUSE_NUMBER, AddressFormatter.ROAD):
        #                revised_tags[k] = v
        #            #elif k not in revised_tags and k == AddressFormatter.POSTCODE:
        #            #    expanded_postal_codes = self.expand_postal_codes(v, country, all_local_languages | random_languages, osm_components)

        #            #    if len(expanded_postal_codes) == 1:
        #            #        revised_tags[AddressFormatter.POSTCODE] = expanded_postal_codes[0]
        #            elif k == AddressFormatter.HOUSE:
        #                building_venue_names.append((v, building_is_generic_place, building_is_known_venue_type))


        #subdivision_components = self.subdivision_components(latitude, longitude)
        #if subdivision_components:
        #    zone = self.zone(subdivision_components)



        #venue_sub_building_prob = float(nested_get(self.config, ('venues', 'sub_building_probability'), default=0.0))
        #add_sub_building_components = AddressFormatter.HOUSE_NUMBER in revised_tags and (AddressFormatter.HOUSE not in revised_tags or random.random() < venue_sub_building_prob)

        revised_tags = self.fix_component_encodings(revised_tags)

        #address_components, country, language = self.components.expanded(revised_tags, 90, 180, language=language,
        #                                                            num_floors=num_floors, num_basements=num_basements,
        #                                                            add_sub_building_components=add_sub_building_components,
        #                                                            population_from_city=True, check_city_wikipedia=True, osm_components=osm_components)

        address_components = revised_tags
        language = candidate_languages[0][0]

        #languages = list(country_languages[country])
        #venue_names = self.venue_names(tags, languages) or []

        #conscription_number = self.conscription_number(tags, language, country)
        #austro_hungarian_street_number = self.austro_hungarian_street_number(tags, language, country)

        # Abbreviate the street name with random probability
        street_name = address_components.get(AddressFormatter.ROAD)

        if street_name:
            normalized_street_name = self.normalized_street_name(address_components, country, language)
            if normalized_street_name:
                street_name = normalized_street_name
                address_components[AddressFormatter.ROAD] = street_name

            address_components[AddressFormatter.ROAD] = self.abbreviated_street(street_name, language)

        city_name = address_components.get(AddressFormatter.CITY)
        if city_name:
            if city_name == address_components.get(AddressFormatter.SUBURB):
                address_components.pop(AddressFormatter.SUBURB)
            city_name = self.normalize_city_name(city_name, tags)
            address_components[AddressFormatter.CITY] = abbreviate(toponym_abbreviations_gazetteer, city_name, language,
                                                                    abbreviate_prob=self.abbreviate_city_probability)

        state_district = address_components.get(AddressFormatter.STATE_DISTRICT)
        if state_district:
            address_components[AddressFormatter.STATE_DISTRICT] = abbreviate(toponym_abbreviations_gazetteer, state_district, language,
                                                        abbreviate_prob=self.abbreviate_state_district_probability)

        if not address_components:
            return None, None, None

        #venue tra-la-la
        reduced_venue_names = []

        formatted_addresses = self.formatted_addresses_with_venue_names(address_components, reduced_venue_names, country, language=language,
                                                                tag_components=tag_components, minimal_only=not tag_components)

        formatted_addresses.extend(self.formatted_places(address_components, country, language))

        if tag_components and address_components:
            # Pick a random dropout order
            dropout_order = self.components.address_level_dropout_order(address_components, country)

            for component in dropout_order:
                address_components.pop(component, None)

                dropout_venue_names = []
                #dropout_venue_names = venue_names
                #if not address_components or (len(address_components) == 1 and list(address_components)[0] == AddressFormatter.HOUSE):
                #    dropout_venue_names = [venue_name for venue_name in venue_names if self.valid_venue_name(venue_name, address_components, street_languages)]

                formatted_addresses.extend(self.formatted_addresses_with_venue_names(address_components, dropout_venue_names, country, language=language,
                                                                                     tag_components=tag_components, minimal_only=False))

        return OrderedDict.fromkeys(formatted_addresses).keys(), country, language

    def build_training_data(self, infile, out_dir, tag_components=True):
        '''
        Creates formatted address training data for supervised sequence labeling (or potentially
        for unsupervised learning e.g. for word vectors) using addr:* tags in OSM.

        Example:

        cs  cz  Gorkého/road ev.2459/house_number | 40004/postcode Trmice/city | CZ/country

        The field structure is similar to other training data created by this script i.e.
        {language, country, data}. The data field here is a sequence of labeled tokens similar
        to what we might see in part-of-speech tagging.


        This format uses a special character "|" to denote possible breaks in the input (comma, newline).

        Note that for the address parser, we'd like it to be robust to many different types
        of input, so we may selectively eleminate components

        This information can potentially be used downstream by the sequence model as these
        breaks may be present at prediction time.

        Example:

        sr      rs      Crkva Svetog Arhangela Mihaila | Vukov put BB | 15303 Trsic

        This may be useful in learning word representations, statistical phrases, morphology
        or other models requiring only the sequence of words.
        '''

        if tag_components:
            formatted_tagged_file = open(os.path.join(out_dir, FORMAT_DATA_TAGGED_FILENAME), 'wb')
            writer = csv.writer(formatted_tagged_file, 'tsv_no_quote')
        else:
            formatted_tagged_file = open(os.path.join(out_dir, FORMAT_DATA_FILENAME), 'wb')
            writer = csv.writer(formatted_tagged_file, 'tsv_no_quote')

        i = 0

        for node_id, value, deps in parse_osm(infile):
            var_formatted_addresses, country, language = self.formatted_addresses(value, tag_components=tag_components)
            if not var_formatted_addresses:
                continue

            for formatted_address in var_formatted_addresses:
                if formatted_address and formatted_address.strip():
                    formatted_address = tsv_string(formatted_address)
                    if not formatted_address or not formatted_address.strip():
                        continue

                    if tag_components:
                        row = (language, country, formatted_address)
                    else:
                        row = (formatted_address,)

                    writer.writerow(row)

            i += 1
            if i % 1000 == 0 and i > 0:
                formatted_tagged_file.flush()
                print('did {} formatted addresses'.format(i))

        print("KONEC")


if __name__ == '__main__':
    # Handle argument parsing here
    parser = argparse.ArgumentParser()

    parser.add_argument('sources', nargs='*')

    parser.add_argument('-i', '--csv-osm-file',
                        help='Path to csv file')

    parser.add_argument('-f', '--format',
                        action='store_true',
                        default=False,
                        help='Save formatted addresses (slow)')

    parser.add_argument('-u', '--untagged',
                        action='store_true',
                        default=False,
                        help='Save untagged formatted addresses (slow)')

    parser.add_argument('-o', '--out-dir',
                        default=os.getcwd(),
                        help='Output directory')

    args = parser.parse_args()

    if args.csv_osm_file and args.format:
        ru_formatter = OSMAddressRUFormatter()
        ru_formatter.build_training_data(args.csv_osm_file, args.out_dir, tag_components=not args.untagged)
    else:
        print(parser.format_usage())