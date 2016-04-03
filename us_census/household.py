__author__ = 'linanqiu'

API_VARIABLE_CSV_PATH = './us_census/api_variable_lookup/household.csv'

import logging

logger = logging.getLogger('HouseholdReader')

import pandas
import census
import us.states as states
import os.path


class HouseholdReader():
    '''
    Reader for reading household data from US Census
    '''

    def __init__(self, CENSUS_API_KEY):
        '''
        We use only Census 2010 SF1 data
        :return: None
        :rtype: None
        '''

        # hardcode census 2010 sf1
        self.census_api = census.Census(CENSUS_API_KEY, year=2010).sf1
        self.read_api_lookup()

    def read_api_lookup(self):
        '''
        Read or generate household.csv, a variable lookup table to help
        deccipher the US census API
        :return: sets instance variables self.api_lookup to a dataframe
        created from household.csv
        :rtype: None
        '''
        if not os.path.exists(API_VARIABLE_CSV_PATH):
            logger.info(
                'household.csv not found in %s. Downloading variables.xml from census.gov and creating household.csv' % API_VARIABLE_CSV_PATH)
            from .api_variable_lookup import parse_api_variable_household as parse_api
            parse_api.create_household_csv(API_VARIABLE_CSV_PATH)

        self.api_lookup = pandas.read_csv(API_VARIABLE_CSV_PATH)

    def filter_api_variable(self):
        '''
        Filters through self.api_lookup to find only the variables that we
        want to query from US Census API
        :return: sets self.api_variables to relevant query variables
        :rtype: None
        '''
        api_variables = self.api_lookup

        # type
        if 'type' in self.params:
            if isinstance(self.params['type'], list):
                api_variables = api_variables[api_variables['type'].isin(self.params['type'])]
            else:
                api_variables = api_variables[api_variables['type'] == self.params['type']]
        else:
            api_variables = api_variables[api_variables['type'].isnull()]

        # has_children
        if 'has_children' in self.params:
            api_variables = api_variables[api_variables['has_children'] == self.params['has_children']]
        else:
            api_variables = api_variables[api_variables['has_children'].isnull()]

        # children_age
        if 'children_age' in self.params:
            if isinstance(self.params['children_age'], list):
                api_variables = api_variables[api_variables['children_age'].isin(self.params['children_age'])]
            else:
                api_variables = api_variables[api_variables['children_age'] == self.params['children_age']]
        else:
            api_variables = api_variables[api_variables['children_age'].isnull()]

        # race
        if 'race' in self.params:
            if isinstance(self.params['race'], list):
                api_variables = api_variables[api_variables['race'].isin(self.params['race'])]
            else:
                api_variables = api_variables[api_variables['race'] == self.params['race']]
        else:
            api_variables = api_variables[api_variables['race'].isnull()]

        # hispanic latino origin
        if 'hispanic_latino_origin' in self.params:
            api_variables = api_variables[
                api_variables['hispanic_latino_origin'] == self.params['hispanic_latino_origin']]
        else:
            api_variables = api_variables[api_variables['hispanic_latino_origin'].isnull()]

        # assign
        self.api_variables = api_variables

    def read(self, geo, params):
        '''
        Queries Census API using the query variables filtered in self.api_variable
        :param geo: geography filters. e.g. {'state': 'OH', 'county': '*'} use '*' for 'all', which would return all
        as individual rows.
        :type geo: dict
        :param params: household parameters. e.g. {'type': 'husband_wife', 'has_children': True, 'children_age':
        'under_6', 'race': ['black', 'asian']}
        :type params: dict
        :return: DataFrame of results from query
        :rtype: pandas.DataFrame
        '''

        self.geo = geo
        self.params = params

        self.filter_api_variable()

        logger.info('Looking up the following variables\n%s' % self.api_variables)

        data = self.query_census(self.api_variables['row_id'].tolist())
        dataframe = pandas.DataFrame(data)
        if 'state' in self.geo and not dataframe.empty:
            dataframe['state'] = dataframe['state'].apply(lambda state_fips: states.lookup(state_fips).abbr)

        if 'county' in self.geo and not dataframe.empty:
            dataframe['county'] = dataframe['county'].astype(float)

        # horizontal sum of queried tables
        dataframe['households'] = dataframe.filter(regex=('P038.*')).astype(float).sum(axis=1)

        cols_keep = list(self.geo.keys())
        cols_keep.insert(0, 'households')

        return dataframe[cols_keep]

    def query_census(self, symbols):
        '''
        Queries US census using the census python API
        :param symbols: variables from http://api.census.gov/data/2010/sf1/variables.html to query
        :type symbols:
        :return: list[str]
        :rtype: pandas.DataFrame
        '''
        state_fips = '*'
        if 'state' in self.geo and self.geo['state'] != '*':
            state_fips = states.lookup(self.geo['state']).fips

        geo_keys = set(self.geo.keys())

        logger.info('Querying using filters: ' + str(self.geo))

        if set(['state']) == geo_keys:
            return self.census_api.state(symbols, state_fips)

        if set(['state', 'county']) == geo_keys:
            county = self.geo['county']
            return self.census_api.state_county(symbols, state_fips, county)

        if set(['state', 'county', 'subdivision']) == geo_keys:
            county = self.geo['county']
            subdivision_fips = self.geo['subdivision']
            return self.census_api.state_county_subdivision(symbols, state_fips, county, subdivision_fips)

        if set(['state', 'county', 'tract']) == geo_keys:
            county = self.geo['county']
            tract = self.geo['tract']
            return self.census_api.state_county_tract(symbols, state_fips, county, tract)

        if set(['state', 'place']) == geo_keys:
            place = self.geo['place']
            return self.census_api.state_place(symbols, state_fips, place)

        if set(['state', 'district']) == geo_keys:
            district = self.geo['district']
            return self.census_api.state_district(symbols, state_fips, district)

        if set(['state', 'msa']) == geo_keys:
            msa = self.geo['msa']
            return self.census_api.state_msa(symbols, state_fips, msa)

        if set(['state', 'csa']) == geo_keys:
            csa = self.geo['csa']
            return self.census_api.state_csa(symbols, state_fips, csa)

        if set(['state', 'district', 'place']) == geo_keys:
            district = self.geo['district']
            place = self.geo['place']
            return self.census_api.state_district_place(symbols, state_fips, district, place)

        if set(['state', 'zipcode']) == geo_keys:
            zipcode = self.geo['zipcode']
            return self.census_api.state_zipcode(symbols, state_fips, zipcode)
