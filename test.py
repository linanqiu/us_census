from us_census.us_census import DataReader

reader_population = DataReader('population', None)
print(reader_population.read(geo={'state': '*', 'county': '*'},
    params={'sex': 'male', 'age': range(20, 25), 'race': ['asian', 'white']}))

reader_household = DataReader('household', None)
print(reader_household.read(geo={'state': 'OH', 'county': '*'},
    params={'type': 'husband_wife', 'has_children': True, 'children_age': 'under_6', 'race': ['black', 'asian']}))
