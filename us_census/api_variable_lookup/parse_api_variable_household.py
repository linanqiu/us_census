__author__ = 'linanqiu'


def create_household_csv(filepath):
    import xml.etree.ElementTree as ET
    import urllib

    XML_URL = 'http://api.census.gov/data/2010/sf1/variables.xml'
    xml_string = urllib.request.urlopen(XML_URL).read()
    tree = ET.fromstring(xml_string)

    rows = []

    for child in tree[0]:
        row_id = child.attrib['{http://www.w3.org/XML/1998/namespace}id']
        row_label = child.attrib['label']
        row_concept = child.attrib['concept'] if 'concept' in child.attrib else None
        rows.append({'row_id': row_id, 'row_label': row_label, 'row_concept': row_concept})

    import pandas as pd

    dataframe = pd.DataFrame(rows)
    dataframe = dataframe.ix[3:]
    dataframe = dataframe[dataframe['row_id'].str.contains('P038')].sort_values(by='row_id')
    dataframe = dataframe[['row_id', 'row_label', 'row_concept']]
    dataframe = dataframe.set_index('row_id')

    def extract_type(row):
        label = row.row_label.lower()
        if 'husband-wife' in label:
            return 'husband_wife'
        if 'female householder' in label:
            return 'female_householder'
        if 'male householder' in label:
            return 'male_householder'
        else:
            return None

    dataframe['type'] = dataframe.apply(lambda row: extract_type(row), axis=1)

    def extract_has_children(row):
        label = row.row_label.lower()
        if 'with own children' in label:
            return True
        if 'no own children' in label:
            return False
        else:
            return None

    dataframe['has_children'] = dataframe.apply(lambda row: extract_has_children(row), axis=1)

    def extract_children_age(row):
        label = row.row_label.lower()
        if 'under 6 years only' in label:
            return 'under_6'
        if 'under 6 years and' in label:
            return 'under_6_and_6_to_17'
        if '6 to 17 years only' in label:
            return '6_to_17'
        else:
            return None

    dataframe['children_age'] = dataframe.apply(lambda row: extract_children_age(row), axis=1)

    def extract_race(row):
        concept = row.row_concept.lower()
        if 'white' in concept:
            return 'white'
        if 'black' in concept:
            return 'black'
        if 'indian' in concept:
            return 'indian_alaskan'
        if 'asian' in concept:
            return 'asian'
        if 'hawaii' in concept:
            return 'hawaii_pacific'
        if 'other' in concept:
            return 'other_alone'
        if 'two' in concept:
            return 'two_or_more'

    dataframe['race'] = dataframe.apply(lambda row: extract_race(row), axis=1)

    def extract_hispanic(row):
        concept = row.row_concept.lower()
        if '(hispanic or latino)' in concept:
            return True
        if 'not hispanic or latino' in concept:
            return False
        return None

    dataframe['hispanic_latino_origin'] = dataframe.apply(lambda row: extract_hispanic(row), axis=1)

    dataframe.to_csv(filepath, float_format='%.0f')
