# Intersect catchment with MERIT DEM
# Finds the mean elevation of each HRU in the model setup with rasterstats.
#
# Note:
# 1. Find the source catchment shapefile;
# 2. Copy the source catchment shapefile to the destintion location;
# 3. Run the zonal statistics algorithm on the copy.

# modules
import os
import geopandas as gpd
from pathlib import Path
from shutil import copyfile
from datetime import datetime
from exactextract import exact_extract

# --- Control file handling
# Easy access to control file folder
controlFolder = Path('../../0_control_files')

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
    
    
# --- Find location of shapefile and DEM
# Catchment shapefile path & name
catchment_path = read_from_control(controlFolder/controlFile,'catchment_shp_path')
catchment_name = read_from_control(controlFolder/controlFile,'catchment_shp_name')

# Specify default path if needed
if catchment_path == 'default':
    catchment_path = make_default_path('shapefiles/catchment') # outputs a Path()
else:
    catchment_path = Path(catchment_path) # make sure a user-specified path is a Path()
    
# DEM path & name
dem_path = read_from_control(controlFolder/controlFile,'parameter_dem_tif_path')
dem_name = read_from_control(controlFolder/controlFile,'parameter_dem_tif_name')

# Specify default path if needed
if dem_path == 'default':
    dem_path = make_default_path('parameters/dem/5_elevation') # outputs a Path()
else:
    dem_path = Path(dem_path) # make sure a user-specified path is a Path()
    
    
# --- Find where the intersection needs to go
# Intersected shapefile path and name
intersect_path = read_from_control(controlFolder/controlFile,'intersect_dem_path')
intersect_name = read_from_control(controlFolder/controlFile,'intersect_dem_name')

# Specify default path if needed
if intersect_path == 'default':
    intersect_path = make_default_path('shapefiles/catchment_intersection/with_dem') # outputs a Path()
else:
    intersect_path = Path(intersect_path) # make sure a user-specified path is a Path()
    
# Make the folder if it doesn't exist
intersect_path.mkdir(parents=True, exist_ok=True)


# --- Copy the source catchment shapefile into the destination location
# Find the name without extension
catchment_base = catchment_name.replace('.shp','')

# Loop over directory contents and copy files that match the filename of the shape
for file in os.listdir(catchment_path):
    if catchment_base in file: # copy only the relevant files in case there are more than 1 .shp files
        
        # make the output file name
        _,ext = os.path.splitext(file)                    # extension of current file
        basefile,_ = os.path.splitext(intersect_name)     # name of the intersection file w/o extension
        newfile = basefile + ext                          # new name + old extension
        
        # copy
        copyfile(catchment_path/file, intersect_path/newfile);
        
        
# --- Rasterstats analysis
# Load the shapefile
gdf = gpd.read_file(intersect_path / intersect_name)
raster_path = dem_path / dem_name

# Calculate zonal statistics
elev_means = exact_extract(str(raster_path), gdf, 'mean', output='pandas')

# Add the mean elevation to the GeoDataFrame
gdf['elev_mean'] = elev_means

# Save the updated GeoDataFrame
gdf.to_file(str(intersect_path/intersect_name))
                                

                                
# --- Code provenance
# Generates a basic log file in the domain folder and copies the control file and itself there.

# Set the log path and file name
logPath = intersect_path
log_suffix = '_catchment_dem_intersect_log.txt'

# Create a log folder
logFolder = '_workflow_log'
Path( logPath / logFolder ).mkdir(parents=True, exist_ok=True)

# Copy this script
thisFile = '1_find_HRU_elevation.py'
copyfile(thisFile, logPath / logFolder / thisFile);

# Get current date and time
now = datetime.now()
# Create a log file 
logFile = now.strftime('%Y%m%d') + log_suffix
with open( logPath / logFolder / logFile, 'w') as file:
    
    lines = ['Log generated by ' + thisFile + ' on ' + now.strftime('%Y/%m/%d %H:%M:%S') + '\n',
             'Found mean HRU elevation from MERIT Hydro adjusted elevation DEM.']
    for txt in lines:
        file.write(txt)  
