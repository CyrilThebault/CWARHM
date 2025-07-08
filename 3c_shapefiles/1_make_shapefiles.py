# Copy base settings
# Copies the base settings into the mizuRoute settings folder.

# modules
from pathlib import Path
from shutil import copyfile
from geopandas import gpd
import pandas as pd
import xarray as xr
from datetime import datetime
from shapely.geometry import Point, LineString, MultiLineString
import numpy as np
from rasterio import open as rio_open


# --- Control file handling
# Easy access to control file folder
controlFolder = Path('../0_control_files')

# Store the name of the 'active' file in a variable
controlFile = 'control_active.txt'

# Function to extract a given setting from the control file
def read_from_control( file, setting ):

    # Open 'control_active.txt' and ...
    with open(file) as contents:
        for line in contents:

            # ... find the line with the requested setting
            if setting in line and not line.startswith('#'):
                break

    # Extract the setting's value
    substring = line.split('|',1)[1]      # Remove the setting's name (split into 2 based on '|', keep only 2nd part)
    substring = substring.split('#',1)[0] # Remove comments, does nothing if no '#' is found
    substring = substring.strip()         # Remove leading and trailing whitespace, tabs, newlines

    # Return this value
    return substring

# Function to specify a default path
def make_default_path(suffix):

    # Get the root path
    rootPath = Path( read_from_control(controlFolder/controlFile,'root_path') )

    # Get the domain folder
    domainName = read_from_control(controlFolder/controlFile,'domain_name')
    domainFolder = 'domain_' + domainName

    # Specify the forcing path
    defaultPath = rootPath / domainFolder / suffix

    return defaultPath


# --- Find where the shapefiles are

rootPath = Path( read_from_control(controlFolder/controlFile,'root_path') )
domainName = read_from_control(controlFolder/controlFile,'domain_name')

# CAMELS-spath
CAMELS_spath = read_from_control(controlFolder/controlFile,'camels_spath')

# Specify default path if needed
if CAMELS_spath == 'default':
    CAMELS_spath = rootPath / 'CAMELS-spat'
else:
    CAMELS_spath = Path(CAMELS_spath) # make sure a user-specified path is a Path()

# Metadata
metadata_path = CAMELS_spath
metadata_name = "camels-spat-metadata.csv"

df_metadata = pd.read_csv(metadata_path / metadata_name)


country, station_id = domainName.split("_")

# Get categories
category_value = df_metadata.loc[(df_metadata['Country'] == country) & (df_metadata['Station_id'] == station_id), 'subset_category']

# Ensure category_value is a string
if not category_value.empty:
    category_value = category_value.iloc[0]  # Convert Series to string
else:
    raise ValueError("No matching subset category found.")  # Handle missing value


#shapefiles
spa = 'distributed'

shapefiles_path =  CAMELS_spath / 'shapefiles' / category_value / ('shapes-' + spa) / domainName

river_file_name = domainName + '_'+ spa + '_river.shp'
catchment_file_name = domainName + '_'+ spa + '_basin.shp'


# --- Modify the river basin shapefile for MizuRoute


shp_river_basin = gpd.read_file( shapefiles_path / catchment_file_name )

shp_river_basin['COMID'] = (10*shp_river_basin['COMID']).astype(int)
shp_river_basin = shp_river_basin.assign(area = shp_river_basin['unitarea']*1000000)
shp_river_basin = shp_river_basin.assign(hru_to_seg = shp_river_basin['COMID'])



# --- Modify the the river network shapefile for Mizuroute

shp_river_network_ini = gpd.read_file( shapefiles_path / river_file_name )

is_empty = shp_river_network_ini.isnull().all(axis=1).all()

def fill_empty_rivershp(river, basin):

    river['COMID'] = basin.iloc[0]['COMID']
    river['lengthdir'] = None
    river['sinuosity'] = None
    river['slope'] = None
    river['uparea'] = basin.iloc[0]['area']
    river['order'] = None
    river['strmDrop_t'] = None
    river['slope_taud'] = None
    river['NextDownID'] = 0
    river['maxup'] = 0
    river['up1'] = 0
    river['up2'] = 0
    river['up3'] = 0
    river['up4'] = 0
    river['new_len_km'] = None

    return river


if is_empty:
    
    shp_river_network = fill_empty_rivershp(shp_river_network_ini, shp_river_basin)

else:
    
    shp_river_network = shp_river_network_ini
    
    # Multiply COMID by 10 and convert to integer to ensure splitted catchments are taken into account
    shp_river_network['COMID'] = (10*shp_river_network['COMID']).astype(int)
    shp_river_network['NextDownID'] = (10*shp_river_network['NextDownID']).astype(int)
    for col in ['up1', 'up2', 'up3', 'up4']:
        shp_river_network[col] = (
                shp_river_network[col]
                .where(shp_river_network[col] == 0, (10 * shp_river_network[col]).astype(int))
                .astype(int)  # Ensures integer type even with NaNs
        )


shp_river_network = shp_river_network.assign(length = shp_river_network['new_len_km']*1000)

# Set the NextDownID of the downstream GRU to 0
next_down_ids = set(shp_river_network["NextDownID"])
comid_values = set(shp_river_network["COMID"])

search_value = next_down_ids - comid_values
search_value = next(iter(search_value))

matching_row = shp_river_network[shp_river_network["NextDownID"] == search_value]


shp_river_network.loc[matching_row.index, 'NextDownID'] = 0

shp_river_network = shp_river_network.set_index('COMID')
shp_river_network = shp_river_network.reindex(shp_river_basin['COMID'])
shp_river_network = shp_river_network.reset_index()



# --- Modify the the catchment shapefile for SUMMA

shp_catchment = gpd.read_file( shapefiles_path / catchment_file_name )

shp_catchment['COMID'] = (10*shp_catchment['COMID']).astype(int)
shp_catchment = shp_catchment.assign(HRU_area = shp_catchment['unitarea']*1000000)
shp_catchment = shp_catchment.assign(HRU_ID = shp_catchment['COMID'])
shp_catchment = shp_catchment.assign(GRU_ID = shp_catchment['COMID'])



# Use forcing to get the lat and lon values
forcing_name = read_from_control(controlFolder/controlFile,'forcing_data_name')
forcing_path =  CAMELS_spath / 'forcing' / category_value / forcing_name / (forcing_name + '-' + spa)

forcing_file = domainName + "_" + forcing_name + "_" + spa +".nc"
if forcing_name == "em-earth":
    forcing_file = forcing_file.replace(forcing_name, "em_earth")

nc_forcing = xr.open_dataset(forcing_path / forcing_file)

lat = nc_forcing['latitude'].isel(time=0).values
lon = nc_forcing['longitude'].isel(time=0).values

shp_catchment = shp_catchment.assign(center_lat = lat)
shp_catchment = shp_catchment.assign(center_lon = lon)



# --- Find river network output folder
# River network shapefile path & name
river_network_path = read_from_control(controlFolder/controlFile,'river_network_shp_path')
river_network_name = read_from_control(controlFolder/controlFile,'river_network_shp_name')

# Specify default path if needed
if river_network_path == 'default':
    river_network_path = make_default_path('shapefiles/river_network') # outputs a Path()
else:
    river_network_path = Path(river_network_path) # make sure a user-specified path is a Path()



# --- Find river basin shapefile (routing catchments) output folder

pd.options.mode.chained_assignment = None # Avoid warning: value is trying to be set on a copy of a slice from a DataFrame

# River network shapefile path & name
river_basin_path = read_from_control(controlFolder/controlFile,'river_basin_shp_path')
river_basin_name = read_from_control(controlFolder/controlFile,'river_basin_shp_name')

# Specify default path if needed
if river_basin_path == 'default':
    river_basin_path = make_default_path('shapefiles/river_basins') # outputs a Path()
else:
    river_basin_path = Path(river_basin_path) # make sure a user-specified path is a Path()



# --- Find catchment shapefile output folder
# Catchment shapefile path & name
catchment_path = read_from_control(controlFolder/controlFile,'catchment_shp_path')
catchment_name = read_from_control(controlFolder/controlFile,'catchment_shp_name')

# Specify default path if needed
if catchment_path == 'default':
    catchment_path = make_default_path('shapefiles/catchment') # outputs a Path()
else:
    catchment_path = Path(catchment_path) # make sure a user-specified path is a Path()


# --- Save shapefiles

shp_river_basin.to_file(river_basin_path / river_basin_name)
shp_river_network.to_file(river_network_path / river_network_name)
shp_catchment.to_file(catchment_path / catchment_name)


# --- Code provenance
# Generates a basic log file in the domain folder and copies the control file and itself there.

# Set the log path and file name
logPath = river_network_path / '..'
log_suffix = '_make_shapefiles.txt'

# Create a log folder
logFolder = '_workflow_log'
Path( logPath / logFolder ).mkdir(parents=True, exist_ok=True)

# Copy this script
thisFile = '1_make_shapefiles.py'
copyfile(thisFile, logPath / logFolder / thisFile);

# Get current date and time
now = datetime.now()

# Create a log file
logFile = now.strftime('%Y%m%d') + log_suffix
with open( logPath / logFolder / logFile, 'w') as file:

    lines = ['Log generated by ' + thisFile + ' on ' + now.strftime('%Y/%m/%d %H:%M:%S') + '\n',
             'Adapt CAMELS-spat shapefiles for SUMMA and mizuRoute.']
    for txt in lines:
        file.write(txt)