__author__ = 'linanqiu'

from . import population
from . import household


def DataReader(variable, CENSUS_API_KEY):
    '''
    Switcher function to choose between PopulationReader and HouseholdReader
    :param variable: 'population' or 'household'
    :type variable: str
    :return: instance of population.PopulationReader or
    household.HouseholdReader
    :rtype: population.PopulationReader or household.HouseholdReader
    '''
    if variable == 'population':
        return population.PopulationReader(CENSUS_API_KEY)
    if variable == 'household':
        return household.HouseholdReader(CENSUS_API_KEY)
