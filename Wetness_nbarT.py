#!/usr/bin/env python
#Wetness_1.py

''' 
This code loads in surface reflectance data from the data cube, calculates tasselled cap wetness for each scene, counts the proportion of scenes with values over a given wetness threshold, and outputs a netcdf file of the wetness count.
Created by Bex Dunn 08/05/2017. 
Modified 02/06/2017 to fix normalisation so it normalises per valid pixel not by timeseries as a whole. 
Modified 02/06/2017 to remove the error message about >- symbols and to print which shapefile is
running to the stderr and stdout files.
Modified 29/06/17 to use nan masking on nbart instead of using a nodata value of -999.0, which was incorrectly displaying as wetness
'''
#for writing to error files:
from __future__ import print_function
#get some libraries
import datacube
import xarray as xr
from datacube.storage import masking
from datacube.storage.masking import mask_to_dict
import json
import pandas as pd
import shapely
from shapely.geometry import shape
import numpy as np #need this for pq fuser

#libraries for polygon and polygon mask
import fiona
import shapely.geometry
import rasterio.features
import rasterio
from datacube.utils import geometry
from datacube.storage.masking import mask_valid_data as mask_invalid_data

#for writing to netcdf
from datacube.storage.storage import write_dataset_to_netcdf

#dealing with system commands
import sys

#suppress warnings thrown when using inequalities in numpy (the threshold values!)
import warnings

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

#code to work with a polygon input rather than a lat/long box

#set the save directory for the netcdf file outputs
ncpath = '/g/data/r78/rjd547/groundwater_activities/Stuart_Cor/Stu_5k_raijin/TCI_400/'

# #pick a shape file
shape_file = ('/g/data/r78/rjd547/groundwater_activities/Stuart_Cor/shapefiles/StHwy_albs_w5kgrid.shp')
# open all the shapes within the shape file
shapes = fiona.open(shape_file)

i=int(sys.argv[1])-1 #there is no node zero, but python counts from 0.
print('is is :'+str(i))
if i > len(shapes):
    print('index not in the range for the shapefile: '+str(i)+' not in '+str(len(shapes)))
    sys.exit(0)

#code to take in the system argument i.e. the number of the polygon to use.
#copy attributes from shapefile and define shape_name
geom_crs = geometry.CRS(shapes.crs_wkt)
geo = shapes[i]['geometry']
geom = geometry.Geometry(geo, crs=geom_crs)
geom_bs = shapely.geometry.shape(shapes[i]['geometry'])
shape_name = shape_file.split('/')[-1].split('.')[0]+'_'+str(i)

#print out which shape you are running for, to output and error files:
print('running TCI for '+shape_name+' polygon number '+str(i))
eprint('running TCI for '+shape_name+' polygon number '+str(i))
#tell the datacube which app to use
dc = datacube.Datacube(app='dc-nbar')

#### DEFINE SPATIOTEMPORAL RANGE AND BANDS OF INTEREST
#Use this to manually define an upper left/lower right coords
#Either as polygon or as lat/lon range


#Define temporal range
start_of_epoch = '1987-01-01'
#need a variable here that defines a rolling 'latest observation'
end_of_epoch =  '2016-12-31'

#Define wavelengths/bands of interest, remove this kwarg to retrieve all bands
bands_of_interest = ['blue',
                     'green',
                     'red', 
                     'nir',
                     'swir1', 
                     'swir2'
                     ]

#Define sensors of interest
sensor1 = 'ls5'
sensor2 = 'ls7'
sensor3 = 'ls8'

query = {
    'time': (start_of_epoch, end_of_epoch), 'geopolygon': geom
}

#Group PQ by solar day to avoid idiosyncracies of N/S overlap differences in PQ algorithm performance
pq_albers_product = dc.index.products.get_by_name(sensor1+'_pq_albers')
valid_bit = pq_albers_product.measurements['pixelquality']['flags_definition']['contiguous']['bits']

def pq_fuser(dest, src):
    valid_val = (1 << valid_bit)

    no_data_dest_mask = ~(dest & valid_val).astype(bool)
    np.copyto(dest, src, where=no_data_dest_mask)

    both_data_mask = (valid_val & dest & src).astype(bool)
    np.copyto(dest, src & dest, where=both_data_mask)

wetness_coeff = {}
wetness_coeff['ls5'] = (0.151, 0.179, 0.330, 0.341, -0.711, -0.457)
wetness_coeff['ls7'] = (0.151, 0.179, 0.330, 0.341, -0.711, -0.457)
#wetness_coeff['ls7'] = (0.2626, 0.2141, 0.0926, 0.0656, -0.7629, -0.5388)
wetness_coeff['ls8'] = (0.1511, 0.1973, 0.3283, 0.3407, -0.7117, -0.4559)

## PQ and Index preparation
# retrieve the NBAR and PQ for the spatiotemporal range of interest
#Retrieve the NBAR and PQ data for sensor n
#_________Note we are retrieving nbart, with the terrain correction, here, not just nbar___________
sensor1_nbar = dc.load(product= sensor1+'_nbart_albers', group_by='solar_day', measurements = bands_of_interest,  **query)
sensor1_pq = dc.load(product= sensor1+'_pq_albers', group_by='solar_day', fuse_func=pq_fuser, **query)
         
crs = sensor1_nbar.crs
crswkt = sensor1_nbar.crs.wkt
affine = sensor1_nbar.affine

#Generate PQ masks and apply those masks to remove cloud, cloud shadow, saturated observations
s1_cloud_free = masking.make_mask(sensor1_pq, 
                              cloud_acca='no_cloud',
                              cloud_shadow_acca = 'no_cloud_shadow',
                              cloud_shadow_fmask = 'no_cloud_shadow',
                              cloud_fmask='no_cloud',
                              blue_saturated = False,
                              green_saturated = False,
                              red_saturated = False,
                              nir_saturated = False,
                              swir1_saturated = False,
                              swir2_saturated = False,
                              contiguous=True)
s1_good_data = s1_cloud_free.pixelquality.loc[start_of_epoch:end_of_epoch]
sensor1_nbar = sensor1_nbar.where(s1_good_data)
sensor1_nbar.attrs['crs'] = crs
sensor1_nbar.attrs['affine'] = affine

sensor2_nbar = dc.load(product= sensor2+'_nbart_albers', group_by='solar_day', measurements = bands_of_interest,  **query)
sensor2_pq = dc.load(product= sensor2+'_pq_albers', group_by='solar_day', fuse_func=pq_fuser, **query)                  

s2_cloud_free = masking.make_mask(sensor2_pq, 
                              cloud_acca='no_cloud',
                              cloud_shadow_acca = 'no_cloud_shadow',
                              cloud_shadow_fmask = 'no_cloud_shadow',
                              cloud_fmask='no_cloud',
                              blue_saturated = False,
                              green_saturated = False,
                              red_saturated = False,
                              nir_saturated = False,
                              swir1_saturated = False,
                              swir2_saturated = False,
                              contiguous=True)
s2_good_data = s2_cloud_free.pixelquality.loc[start_of_epoch:end_of_epoch]
sensor2_nbar = sensor2_nbar.where(s2_good_data)
sensor2_nbar.attrs['crs'] = crs
sensor2_nbar.attrs['affine'] = affine

sensor3_nbar = dc.load(product= sensor3+'_nbart_albers', group_by='solar_day', measurements = bands_of_interest,  **query)
sensor3_pq = dc.load(product= sensor3+'_pq_albers', group_by='solar_day', fuse_func=pq_fuser, **query)                  

s3_cloud_free = masking.make_mask(sensor3_pq, 
                              cloud_acca='no_cloud',
                              cloud_shadow_acca = 'no_cloud_shadow',
                              cloud_shadow_fmask = 'no_cloud_shadow',
                              cloud_fmask='no_cloud',
                              blue_saturated = False,
                              green_saturated = False,
                              red_saturated = False,
                              nir_saturated = False,
                              swir1_saturated = False,
                              swir2_saturated = False,
                              contiguous=True)
s3_good_data = s3_cloud_free.pixelquality.loc[start_of_epoch:end_of_epoch]
sensor3_nbar = sensor3_nbar.where(s3_good_data)
sensor3_nbar.attrs['crs'] = crs
sensor3_nbar.attrs['affine'] = affine

#__________This section included so nbarT is correctly used to correct terrain by removing -999.0 values and replacing with nans________________________
sensor1_nbar = sensor1_nbar.where(sensor1_nbar!=-999.0)
sensor2_nbar = sensor2_nbar.where(sensor2_nbar!=-999.0)
sensor3_nbar = sensor3_nbar.where(sensor3_nbar!=-999.0)

#Calculate Taselled Cap Wetness
wetness_sensor1_nbar = ((sensor1_nbar.blue*wetness_coeff[sensor1][0])+(sensor1_nbar.green*wetness_coeff[sensor1][1])+
                          (sensor1_nbar.red*wetness_coeff[sensor1][2])+(sensor1_nbar.nir*wetness_coeff[sensor1][3])+
                          (sensor1_nbar.swir1*wetness_coeff[sensor1][4])+(sensor1_nbar.swir2*wetness_coeff[sensor1][5]))
wetness_sensor2_nbar = ((sensor2_nbar.blue*wetness_coeff[sensor2][0])+(sensor2_nbar.green*wetness_coeff[sensor2][1])+
                          (sensor2_nbar.red*wetness_coeff[sensor2][2])+(sensor2_nbar.nir*wetness_coeff[sensor2][3])+
                          (sensor2_nbar.swir1*wetness_coeff[sensor2][4])+(sensor2_nbar.swir2*wetness_coeff[sensor2][5]))
wetness_sensor3_nbar = ((sensor3_nbar.blue*wetness_coeff[sensor3][0])+(sensor3_nbar.green*wetness_coeff[sensor3][1])+
                          (sensor3_nbar.red*wetness_coeff[sensor3][2])+(sensor3_nbar.nir*wetness_coeff[sensor3][3])+
                          (sensor3_nbar.swir1*wetness_coeff[sensor3][4])+(sensor3_nbar.swir2*wetness_coeff[sensor3][5]))
wetness_multi = xr.concat([wetness_sensor1_nbar, wetness_sensor2_nbar, wetness_sensor3_nbar], dim = 'time')

#set wetness data threshold. catch warning about using numpy inequalities.. RuntimeWarning: invalid value encountered in greaterif not reflexive

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    water_plus_wetveg = wetness_multi.where(wetness_multi>-400)

#count the number of wetness scenes for each pixel
wet_count = wetness_multi.count(dim = 'time')

#count the amount of times that water plus wet veg is above the threshold
threshold_count=water_plus_wetveg.count(dim='time')

#divide the number of times wetness is seen by the number of wetness scenes to get a proportion of time that the 
#pixel is wet or wet veg'd:
new_wet_count = threshold_count/wet_count

#Output new_wet_count to netCDF

#get the original dataset attributes (crs)
#set up variable attributes to hold the attributes from sensor1_nbar
attrs = sensor1_nbar
#dump the extra data to just get the attributes
nbar_data = attrs.data_vars.keys()
for j in nbar_data:
    #drop band data, retaining just the attributes
    attrs =attrs.drop(j)
#set up new variable called wet_vars, and assign attributes to it in a dictionary
wet_vars = {'new_wet_count':''}
wet_count_data = attrs.assign(**wet_vars)
wet_count_data['new_wet_count'] = new_wet_count

try:
    write_dataset_to_netcdf(wet_count_data,variable_params={'new_wet_count': {'zlib':True}}, filename=ncpath+shape_name+'_400run.nc')
except RuntimeError as err:
    print("RuntimeError: {0}".format(err))
        
print('successfully ran TCI for '+shape_name+' polygon number '+str(i))
eprint('successfully ran TCI for '+shape_name+' polygon number '+str(i))
