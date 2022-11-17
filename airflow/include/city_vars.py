###### city-specific vars

from pyspark.sql import types

cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
f_cities = [c.replace(' ', '_').lower() for c in cities]

dict_common = {
    'p_ret_type': types.StringType(),
    'p_col': 'street',
    'int_col': 'UNK_INT',
    'str_col': 'UNK_STR',
    'minimal': ['id', 'timestamp', 'city', 'street', 'type', 'description', 'status', 'area', 'victim_age'],
    'extra': ['location', 'latitude', 'longitude', 'domestic', 'weapon', 'victim_sex', 'victim_descent']
}

# parse unclean column for analysis
def parse_chi(block):
    """
    parse street from Chicago given block of format 'NNNXX D STREET ST ..'
    """
    split = block.split()
    street = split[2]
#    if len(split) > 2:             # temporary workaround to weird IndexError: list index out of range for street = split[2]
#        street = split[2]
#    else:
#        return block
    if len(split) > 3:
        for part in split[3:]:
            street += f' {part}'
    return street

def parse_los(location):
    """
    parse street from LA given block of format 'NNN D STREET ST .. (usually)'
    """
    split = location.split()
    for i in range(len(split)):
        if split[i] in ['N', 'E', 'W', 'S']:
            return " ".join(split[i+1:])
    for i in range(len(split)):
        if split[i].isdigit():
            return " ".join(split[i+1:])
    return " ".join(split[-1:])

def parse_aus(go_location):
    """
    parse street from Austin given block of format 'NNN STREET ST .. (usually)'
    """
    go_location = go_location.replace(' NB', '').replace(' EB', '').replace(' WB', '').replace(' SB', '').replace(' SVRD', '')
    return parse_los(go_location)

dict_chicago = {
    'formatted': f_cities[0],
    'schema_template1': types.StructType([               # modified pattern from pandas schema
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
    'schema_template2': types.StructType([
        types.StructField('ID', types.IntegerType(), True),
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
    'schema_template3': types.StructType([
        types.StructField('ID', types.IntegerType(), True),
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
        types.StructField('District', types.StringType(), True),
        types.StructField('Ward', types.IntegerType(), True),
        types.StructField('Community Area', types.IntegerType(), True),
        types.StructField('FBI Code', types.StringType(), True),
        types.StructField('X Coordinate', types.FloatType(), True),
        types.StructField('Y Coordinate', types.FloatType(), True),
        types.StructField('Year', types.IntegerType(), True),
        types.StructField('Updated On', types.TimestampType(), True),
        types.StructField('Latitude', types.FloatType(), True),
        types.StructField('Longitude', types.FloatType(), True),
        types.StructField('Location', types.StringType(), True)
    ]),
    'date_format': 'MM/DD/YYYY hh:mm:ss A',
    'date_string_col': 'Date',
    'with_year_col': True,
    'csv_parts': 1,
    'pq_parts': 2,
    'ordered_cols': ['case_number', 'timestamp', 'block', 'primary_type', 'description', 'arrest', 'beat', 'location_description', 'latitude', 'longitude', 'domestic'],
    'renamed_cols': ['id', 'timestamp', 'street', 'type', 'description', 'status', 'area', 'location', 'latitude', 'longitude', 'domestic'],
    'parser': parse_chi,
    'new_col': 'victim_age',
    'new_val_from': dict_common['int_col']
}

dict_san_francisco = {
    'formatted': f_cities[1],
    'schema_template1': types.StructType([
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
    'csv_parts': 8,
    'pq_parts': 12,
    'renamed_cols': []
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
    'csv_parts': 7,
    'pq_parts': 24,
    'ordered_cols': ['dr_no', 'timestamp', 'location', 'crm_cd_desc', 'status_desc', 'rpt_dist_no', 'premis_desc', 'lat', 'lon', 'weapon_desc', 'vict_age', 'vict_sex', 'vict_descent'],
    'renamed_cols': ['id', 'timestamp', 'street', 'description', 'status', 'area', 'location', 'latitude', 'longitude', 'weapon', 'victim_age', 'victim_sex', 'victim_descent'],
    'parser': parse_los,
    'new_col': 'type',
    'new_val_from': 'description'
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
    'csv_parts': 1,
    'pq_parts': 1,
    'ordered_cols': ['go_primary_key', 'timestamp', 'go_location', 'highest_nibrs/ucr_offense_description', 'go_highest_offense_desc', 'clearance_status', 'go_district'],
    'renamed_cols': ['id', 'timestamp', 'street', 'type', 'description', 'status', 'area'],
    'parser': parse_aus,
    'new_col': 'victim_age',
    'new_val_from': dict_common['int_col']
}

dict_cities = {
    cities[0]: dict_chicago,
    cities[1]: dict_san_francisco,
    cities[2]: dict_los_angeles,
    cities[3]: dict_austin
}

def selector(dict_city, item, par):
    if item == 'schema':
        if dict_city['formatted'] == f_cities[0]:           # Chicago
            if any(str(year) in par for year in range(2003, 2011)):
                return dict_city['schema_template1']
            elif '2011' in par:
                return dict_city['schema_template2']
            else:
                return dict_city['schema_template3']
        elif dict_city['formatted'] == f_cities[1]:         # SF
            if 'Present' not in par:
                return dict_city['schema_template1']
            else:
                return dict_city['schema_template2']
        else:                                               # LA, Austin
            return dict_city['schema_template']
