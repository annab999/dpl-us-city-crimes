###### city-specific vars

from pyspark.sql import types

cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
f_cities = [c.replace(' ', '_').lower() for c in cities]

# modified pattern from pandas schema

dict_chicago = {
    'formatted': f_cities[0],
    'schema_template': types.StructType([
        types.StructField('Case Number', types.StringType(), True),
        types.StructField('Date', types.StringType(), True),
        types.StructField('Block', types.StringType(), True),
        types.StructField('IUCR', types.StringType(), True),
        types.StructField('Primary Type', types.StringType(), True),
        types.StructField('Description', types.StringType(), True),
        types.StructField('Location Description', types.StringType(), True),
        types.StructField('Arrest', types.BooleanType(), True),
        types.StructField('Domestic', types.BooleanType(), True),
        types.StructField('Beat', types.StringType(), True),
        types.StructField('Ward', types.IntegerType(), True),
        types.StructField('FBI Code', types.StringType(), True),
        types.StructField('X Coordinate', types.FloatType(), True),
        types.StructField('Y Coordinate', types.FloatType(), True),
        types.StructField('Year', types.IntegerType(), True),
        types.StructField('Latitude', types.FloatType(), True),
        types.StructField('Longitude', types.FloatType(), True),
        types.StructField('Location', types.StringType(), True)
    ]),
    'date_format': 'MM/DD/YYYY hh:mm:ss A',
    'date_string_col': 'Date',
    'with_year_col': True,
    'partitions': 1
}

dict_san_francisco = {
    'formatted': f_cities[1],
    'schema_template': types.StructType([
        types.StructField('PdId', types.LongType(), True),
        types.StructField('IncidntNum', types.IntegerType(), True),
        types.StructField('Incident Code', types.IntegerType(), True),
        types.StructField('Category', types.StringType(), True),
        types.StructField('Descript', types.StringType(), True),
        types.StructField('DayOfWeek', types.StringType(), True),
        types.StructField('Date', types.StringType(), True),
        types.StructField('Time', types.StringType(), True),
        types.StructField('PdDistrict', types.StringType(), True),
        types.StructField('Resolution', types.StringType(), True),
        types.StructField('Address', types.StringType(), True),
        types.StructField('X', types.FloatType(), True),
        types.StructField('Y', types.FloatType(), True),
        types.StructField('location', types.StringType(), True),
        types.StructField('SF Find Neighborhoods 2 2', types.FloatType(), True),
        types.StructField('Current Police Districts 2 2', types.FloatType(), True),
        types.StructField('Current Supervisor Districts 2 2', types.FloatType(), True),
        types.StructField('Analysis Neighborhoods 2 2', types.FloatType(), True),
        types.StructField('DELETE - Fire Prevention Districts 2 2', types.FloatType(), True),
        types.StructField('DELETE - Police Districts 2 2', types.FloatType(), True),
        types.StructField('DELETE - Supervisor Districts 2 2', types.FloatType(), True),
        types.StructField('DELETE - Zip Codes 2 2', types.FloatType(), True),
        types.StructField('DELETE - Neighborhoods 2 2', types.FloatType(), True),
        types.StructField('DELETE - 2017 Fix It Zones 2 2', types.FloatType(), True),
        types.StructField('Civic Center Harm Reduction Project Boundary 2 2', types.FloatType(), True),
        types.StructField('Fix It Zones as of 2017-11-06  2 2', types.FloatType(), True),
        types.StructField('DELETE - HSOC Zones 2 2', types.FloatType(), True),
        types.StructField('Fix It Zones as of 2018-02-07 2 2', types.FloatType(), True),
        types.StructField('CBD, BID and GBD Boundaries as of 2017 2 2', types.FloatType(), True),
        types.StructField('Areas of Vulnerability, 2016 2 2', types.FloatType(), True),
        types.StructField('Central Market/Tenderloin Boundary 2 2', types.FloatType(), True),
        types.StructField('Central Market/Tenderloin Boundary Polygon - Updated 2 2', types.FloatType(), True),
        types.StructField('HSOC Zones as of 2018-06-05 2 2', types.FloatType(), True),
        types.StructField('OWED Public Spaces 2 2', types.FloatType(), True),
        types.StructField('Neighborhoods 2', types.FloatType(), True)
    ]),
    'date_format': 'MM/DD/YYYY',
    'date_string_col': 'Date',
    'with_year_col': False,
    'partitions': 12
}

dict_los_angeles = {
    'formatted': f_cities[2],
    'schema_template': types.StructType([
        types.StructField('DR_NO', types.LongType(), True),
        types.StructField('Date Rptd', types.StringType(), True),
        types.StructField('DATE OCC', types.StringType(), True),
        types.StructField('TIME OCC', types.IntegerType(), True),
        types.StructField('AREA ', types.IntegerType(), True),
        types.StructField('AREA NAME', types.StringType(), True),
        types.StructField('Rpt Dist No', types.IntegerType(), True),
        types.StructField('Part 1-2', types.IntegerType(), True),
        types.StructField('Crm Cd', types.IntegerType(), True),
        types.StructField('Crm Cd Desc', types.StringType(), True),
        types.StructField('Mocodes', types.StringType(), True),
        types.StructField('Vict Age', types.IntegerType(), True),
        types.StructField('Vict Sex', types.StringType(), True),
        types.StructField('Vict Descent', types.StringType(), True),
        types.StructField('Premis Cd', types.IntegerType(), True),
        types.StructField('Premis Desc', types.StringType(), True),
        types.StructField('Weapon Used Cd', types.IntegerType(), True),
        types.StructField('Weapon Desc', types.StringType(), True),
        types.StructField('Status', types.StringType(), True),
        types.StructField('Status Desc', types.StringType(), True),
        types.StructField('Crm Cd 1', types.IntegerType(), True),
        types.StructField('Crm Cd 2', types.IntegerType(), True),
        types.StructField('Crm Cd 3', types.IntegerType(), True),
        types.StructField('Crm Cd 4', types.IntegerType(), True),
        types.StructField('LOCATION', types.StringType(), True),
        types.StructField('Cross Street', types.StringType(), True),
        types.StructField('LAT', types.FloatType(), True),
        types.StructField('LON', types.FloatType(), True)
    ]),
    'date_format': 'MM/DD/YYYY hh:mm:ss A',
    'date_string_col': 'DATE OCC',
    'with_year_col': False,
    'partitions': 24
}

dict_austin = {
    'formatted': f_cities[3],
    'schema_template': types.StructType([
        types.StructField('GO Primary Key', types.IntegerType(), True),
        types.StructField('Council District', types.IntegerType(), True),
        types.StructField('GO Highest Offense Desc', types.StringType(), True),
        types.StructField('Highest NIBRS/UCR Offense Description', types.StringType(), True),
        types.StructField('GO Report Date', types.StringType(), True),
        types.StructField('GO Location', types.StringType(), True),
        types.StructField('Clearance Status', types.StringType(), True),
        types.StructField('Clearance Date', types.StringType(), True),
        types.StructField('GO District', types.StringType(), True),
        types.StructField('GO Location Zip', types.IntegerType(), True),
        types.StructField('GO Census Tract', types.FloatType(), True),
        types.StructField('GO X Coordinate', types.IntegerType(), True),
        types.StructField('GO Y Coordinate', types.IntegerType(), True)
    ]),
    'date_format': 'D-MMM-YY',
    'date_string_col': 'GO Report Date',
    'with_year_col': False,
    'partitions': 1
}

dict_cities = {
    cities[0]: dict_chicago,
    cities[1]: dict_san_francisco,
    cities[2]: dict_los_angeles,
    cities[3]: dict_austin
}