import csv
import re
import six


newline_regex = re.compile('\r\n|\r|\n')

csv.register_dialect('tsv_no_quote', delimiter='\t', quoting=csv.QUOTE_NONE, quotechar='')

import six

text_type = six.text_type
string_types = six.string_types
binary_type = six.binary_type


def safe_decode(value, encoding='utf-8', errors='strict'):
    if isinstance(value, text_type):
        return value

    if isinstance(value, (string_types, binary_type)):
        return value.decode(encoding, errors)
    else:
        return binary_type(value).decode(encoding, errors)


def safe_encode(value, incoming=None, encoding='utf-8', errors='strict'):
    if not isinstance(value, (string_types, binary_type)):
        return binary_type(value)
    if isinstance(value, text_type):
        return value.encode(encoding, errors)
    else:
        if hasattr(incoming, 'lower'):
            incoming = incoming.lower()
        if hasattr(encoding, 'lower'):
            encoding = encoding.lower()

        if value and encoding != incoming:
            value = safe_decode(value, encoding, errors)
            return value.encode(encoding, errors)
        else:
            return value


def tsv_string(s):
    return safe_encode(newline_regex.sub(u', ', safe_decode(s).strip()).replace(u'\t', u' '))


def unicode_csv_reader(filename, **kw):
    for line in csv.reader(filename, **kw):
        yield [unicode(c, 'utf-8') for c in line]



#txt = u'ул. Освобождения Урала	д. 1. Здание лечебно-диагностического корпуса. Нежилые помещения №№ 36а, 36б, 39 - 48. Этаж № 1.'
txt = u'49. Бытовая № 5, литер А 49 по паспорту БТИ'

street_and_house_dom_regex = re.compile(u'.*д[ом]?.?\s?\d+\s?[А-я]?')
street_and_house_regex = re.compile(u'\d+\s?[А-я]?')


f = open("license_separate_addresses.tsv")
reader = unicode_csv_reader(f, dialect='tsv_no_quote')
headers = reader.next()

fo = open("license_separate_addresses_cut.tsv", "wb")
writer = csv.writer(fo, 'tsv_no_quote')
writer.writerow(headers)

i = 0

fix_column_index = 6
max_column_len_to_fix = 30

for row in reader:
    column_house = row[fix_column_index]
    if len(column_house) > max_column_len_to_fix:
        found = street_and_house_dom_regex.findall(column_house)
        if len(found) > 0:
            row[fix_column_index] = found[0]
        else:
            found = street_and_house_regex.match(column_house)
            if found:
                row[fix_column_index] = found.group()
            else:
                row[fix_column_index] = ''

    k = 0
    for col in row:
        col = tsv_string(col)
        row[k] = col
        k += 1

    writer.writerow(row)

    i += 1
    #if i == 1000:
    #    break