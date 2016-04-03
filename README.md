# US Census 2010

Tool to intuitively query the US Census 2010.

Until I have time to write something more solid, this will have to do.

```python
from us_census.us_census import DataReader

reader_population = DataReader('population', CENSUS_API_KEY=None) # surprisingly this works
print(reader_population.read(geo={'state': '*', 'county': '*'},
    params={'sex': 'male', 'age': range(20, 25), 'race': ['asian', 'white']}))

reader_household = DataReader('household', CENSUS_API_KEY=None) # surprisingly this works
print(reader_household.read(geo={'state': 'OH', 'county': '*'},
    params={'type': 'husband_wife', 'has_children': True, 'children_age': 'under_6', 'race': ['black', 'asian']}))
```
