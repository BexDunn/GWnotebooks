from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import click
import functools
import sys
import os
import csv 
import numpy as np
import rasterio
import fiona
import shapely.geometry
import datetime as DT
import xarray as xr
import pandas as pd
from datetime import datetime
from itertools import product
import logging
import logging.handlers as lh
import dask.array as da
import datacube.api
import copy
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, YEARLY
from datacube.ui import click as ui
# from datacube.ui.expression import parse_expressions
from enum import Enum
from pathlib import Path
from datacube.storage.storage import write_dataset_to_netcdf
from datacube_stats import statistics
from rasterio.enums import ColorInterp
# from gqa_filter import list_gqa_filtered_cells, get_gqa
from dateutil.rrule import rrule, YEARLY
import hdmedians as hd

logging.basicConfig()
_log = logging.getLogger('agdc-temporal-geomedian-test')
_log.setLevel(logging.INFO)
#: pylint: disable=invalid-name
required_option = functools.partial(click.option, required=True)
MY_GEO = {}
MY_DATE = {}
DEFAULT_PROFILE = {
    'blockxsize': 256,
    'blockysize': 256,
    'compress': 'lzw',
    'driver': 'GTiff',
    'interleave': 'band',
    'nodata': -999,
    'tiled': True}

#: pylint: disable=too-many-arguments
@ui.cli.command(name='tidal-temporal-range')
@ui.global_cli_options
# @ui.executor_cli_options
# @click.command()
@required_option('--year_range', 'year_range', type=str, help='2010-2017 i.e 2010-01-01 to 2017-12-31')
@click.option('--epoch', 'epoch', default=40, type=int, help='epoch like 2 5 10')
@click.option('--lon_range', 'lon_range', type=str, default='', help='like (132.10,130.20) under quote') #Katherine (132.27, -14.45)
@click.option('--lat_range', 'lat_range', type=str, default='', help='like (-14.35,-14.10) under quote')
@click.option('--mnt_range', 'mnt_range', type=str, default='7,11', help='like 7,11 under quote')
@click.option('--poly', 'poly', type=str, default='', help='set the location of shp file')
@click.option('--rf_range_low', 'rf_range_low', type=str, required=True,
              help='pass year of drys for multi epoch hyphen separated like 1995-2012')
@click.option('--rf_range_high', 'rf_range_high', type=str, required=True,
              help='pass year of wets for multi epoch hyphen separated like 2001,2010,2011-2014')
@click.option('--season', 'season', default='dummy', type=str, help='summer winter autumn spring')
@click.option('--ls7fl', default=True, is_flag=True, help='To include all LS7 data set it to False')
@click.option('--debug', default=False, is_flag=True, help='Build in debug mode to get details of tide height within time range')
# @ui.parsed_search_expressions
# @ui.pass_index(app_name='agdc-tidal-analysis-app')

def main(epoch, lon_range, lat_range, year_range, mnt_range, poly, rf_range_low, rf_range_high, season, ls7fl, debug):
    # dc = datacube.Datacube(app="tidal-range")
    products = ['ls5_nbar_albers', 'ls7_nbar_albers', 'ls8_nbar_albers']
    dc=datacube.Datacube(app='tidal_temporal_test')
    cr_info = MyCrop(dc, lon_range, lat_range, products, epoch, year_range, mnt_range, poly, rf_range_low, rf_range_high, season, ls7fl, debug)
    print ("Input date range " + year_range )
    for (acq_min, acq_max, dry_years, wet_years) in cr_info.get_epochs():
        if season == "dummy":
            print ("running task for epoch " + str(acq_min) + " TO " + str(acq_max)  + " for lon/lat range " + lon_range + lat_range + " epoch " + str(epoch) + "  at" + str(datetime.now().time()))
        else:
            print ("running task for epoch " + str(acq_min) + " TO " + str(acq_max)
                   + " for lon/lat range " + lon_range + lat_range + " epoch " + str(epoch
                   ) + " for season " + season + str(datetime.now().time()))
        
        cr_info.cov_task(acq_min, acq_max, dry_years, wet_years)


def pq_fuser(dest, src):
    valid_bit = 8
    valid_val = (1 << valid_bit)
    no_data_dest_mask = ~(dest & valid_val).astype(bool)
    np.copyto(dest, src, where=no_data_dest_mask)
    both_data_mask = (valid_val & dest & src).astype(bool)
    np.copyto(dest, src & dest, where=both_data_mask)


class MyCrop():
    def __init__(self, dc, lon_range, lat_range, products, epoch, year_range, mnt_range, poly, rf_range_low, rf_range_high, season, ls7fl, debug):
        self.dc = dc
        if len(lon_range) > 0:
            self.lon = eval(lon_range)
            self.lat = eval(lat_range)
        else:
            self.lon = ''
            self.lat = '' 
        self.products = products
        self.epoch = epoch
        self.start_epoch = datetime.strptime(year_range.split('-')[0] +"-01-01", "%Y-%m-%d").date()
        self.end_epoch = datetime.strptime(year_range.split('-')[1]+"-12-31", "%Y-%m-%d").date()
        self.mnt_range = mnt_range
        self.poly = poly
        self.rf_range_low = rf_range_low
        self.rf_range_high = rf_range_high
        self.season = season
        self.ls7fl = ls7fl
        self.debug = debug

    def get_epochs(self):
        inx = 0
        for dt in rrule(YEARLY, interval=self.epoch, dtstart=self.start_epoch, until=self.end_epoch):

            if dt.date() >= self.end_epoch:
               print ("CALCULATION finished and data available in MY_GEO ")
               return 
            acq_min = dt.date()
            acq_max = acq_min + relativedelta(years=self.epoch, days=-1)
            acq_min = max(self.start_epoch, acq_min)
            acq_max = min(self.end_epoch, acq_max)
            dry_years = self.rf_range_low.split('-')[inx]
            wet_years = self.rf_range_high.split('-')[inx] 
            dry_years = [str(x) for x in dry_years.split(',')]
            wet_years = [str(x) for x in wet_years.split(',')]
            inx += 1
            yield acq_min, acq_max, dry_years, wet_years

    def warp_geometry(geom, src_crs, dst_crs):
        """
        warp geometry from src_crs to dst_crs
        """
        return shapely.geometry.shape(rasterio.warp.transform_geom(src_crs, dst_crs, shapely.geometry.mapping(geom)))


    def geometry_mask(geom, geobox, all_touched=False, invert=False):
        """
        rasterize geometry into a binary mask where pixels that overlap geometry are False
        """
        return rasterio.features.geometry_mask([geom],
                                               out_shape=geobox.shape,
                                               transform=geobox.affine,
                                               all_touched=all_touched,
                                               invert=invert)

    def load_polygon(self):
        with fiona.open(self.poly) as shapes:
            geom_crs = str(shapes.crs_wkt)
            geom = shapely.geometry.shape(next(shapes)['geometry'])
            return geom_crs, geom
    
    def build_my_dataset(self, acq_min, acq_max, dry_years, wet_years):
        
        nbar_data = None 
        dt5 = "2011-12-01"
        dtt7 = "1999-07-01"
        dt7 = "2003-03-01"
        dt8 = "2013-04-01"
        geom_crs = None
        geom = None
        if len(self.poly) > 0:
            geom_crs, geom = self.load_polygon()
        ed = acq_max 
        sd = acq_min
        for i, st in enumerate(self.products):
            prod = None        
            acq_max = ed
            acq_min = sd
            print (" doing for sensor",  st )
            if st == 'ls5_nbar_albers' and acq_max > datetime.strptime(dt5, "%Y-%m-%d").date() and  \
                  acq_min > datetime.strptime(dt5, "%Y-%m-%d").date():
                print ("LS5 post 2011 Dec data is not exist")
                continue
            elif st == 'ls5_nbar_albers' and acq_max > datetime.strptime(dt5, "%Y-%m-%d").date() and \
                  acq_min < datetime.strptime(dt5, "%Y-%m-%d").date():
                acq_max = datetime.strptime(dt5, "%Y-%m-%d").date()
                print (" epoch end date is reset for LS5 2011/12/01")
            if st == 'ls7_nbar_albers' and self.ls7fl and acq_max > datetime.strptime(dt7, "%Y-%m-%d").date() and  \
                  acq_min > datetime.strptime(dt7, "%Y-%m-%d").date():
                print ("LS7 post 2003 March data is not included")
                continue
            elif st == 'ls7_nbar_albers' and self.ls7fl and acq_max > datetime.strptime(dt7, "%Y-%m-%d").date() and \
                  acq_min < datetime.strptime(dt7, "%Y-%m-%d").date():
                acq_max = datetime.strptime(dt7, "%Y-%m-%d").date()
                print (" epoch end date is reset for LS7 2003/03/01")
            if st == 'ls7_nbar_albers' and acq_max < datetime.strptime(dtt7, "%Y-%m-%d").date() and \
                  acq_min < datetime.strptime(dtt7, "%Y-%m-%d").date():
                print ("No LS7 data for this period")
                continue
            if st == 'ls8_nbar_albers' and acq_max < datetime.strptime(dt8, "%Y-%m-%d").date() and \
                  acq_min < datetime.strptime(dt8, "%Y-%m-%d").date():
                print ("No LS8 data for this period")
                continue
            elif st == 'ls8_nbar_albers' and acq_max > datetime.strptime(dt8, "%Y-%m-%d").date() and \
                acq_min < datetime.strptime(dt8, "%Y-%m-%d").date():
                acq_min = datetime.strptime(dt8, "%Y-%m-%d").date()
            if st == 'ls5_nbar_albers':
                prod = 'ls5_pq_albers'
            elif st == 'ls7_nbar_albers':
                prod = 'ls7_pq_albers'
            else:
                prod = 'ls8_pq_albers'
            # add extra day to the maximum range to include the last day in the search
            # end_ep = acq_max + relativedelta(days=1)
            if geom:
                indexers = {'time':(acq_min, acq_max), 'x': (geom.bounds[0], geom.bounds[2]),  \
                            'y': (geom.bounds[1], geom.bounds[3]), \
                            'crs': geom_crs, 'group_by':'solar_day'}
            else:
                indexers = {'time':(acq_min, acq_max), 'x':(str(self.lon[0]), str(self.lon[1])), 
                            'y':(str(self.lat[0]), str(self.lat[1])), 'group_by':'solar_day'}
            pq = self.dc.load(product=prod, fuse_func=pq_fuser, **indexers)   
            if st == 'ls5_nbar_albers' and len(pq) == 0:
                print ("No LS5 data found")
                continue
            if nbar_data is not None and st == 'ls7_nbar_albers' and len(pq) == 0:
                print ("No LS7 data found")
                continue
            if geom:
                indexers = {'time':(acq_min, acq_max), 'x': (geom.bounds[0], geom.bounds[2]), 
                            'y': (geom.bounds[1], geom.bounds[3]),
                            'crs': geom_crs, 'measurements':['blue', 'green', 'red', 'nir', 'swir1', 'swir2'],
                            'group_by':'solar_day'}
            else:
                indexers = {'time':(acq_min, acq_max), 'x':(str(self.lon[0]), str(self.lon[1])), 
                            'y':(str(self.lat[0]), str(self.lat[1])), 
                            'measurements':['blue', 'green', 'red', 'nir', 'swir1', 'swir2'], 'group_by':'solar_day'}
            mask_clear = pq['pixelquality'] & 15871 == 15871
            if nbar_data is not None:
                new_data = self.dc.load(product=st, **indexers)
                new_data = new_data.where(mask_clear)
                nbar_data = xr.concat([nbar_data, new_data], dim='time')
            else:
                nbar_data = self.dc.load(product=st, **indexers)
                nbar_data = nbar_data.where(mask_clear)
        # Sort in time order
        nbar_data = nbar_data.sel(time=sorted(nbar_data.time.data))
        # if season then filtered only season data
        if self.season.upper() == 'WINTER':
            nbar_data = nbar_data.isel(time=nbar_data.groupby('time.season').groups['JJA'])
        elif self.season.upper() == 'SUMMER':
            nbar_data = nbar_data.isel(time=nbar_data.groupby('time.season').groups['DJF'])
        elif self.season.upper() == 'SPRING':
            nbar_data = nbar_data.isel(time=nbar_data.groupby('time.season').groups['SON'])
        elif self.season.upper() == 'AUTUMN':
            nbar_data = nbar_data.isel(time=nbar_data.groupby('time.season').groups['MAM'])
        # filtered out lowest and highest date range
        dry_data = None
        wet_data = None
        for n,x in  enumerate(dry_years):
            if n == 0:
                dry_data = nbar_data.sel(time=slice(x, x))
            else:
                dry_data = xr.concat([dry_data, nbar_data.sel(time=slice(x, x))], 'time')

        for n,x in  enumerate(wet_years):
            if n == 0:
                wet_data = nbar_data.sel(time=slice(x, x))
            else:
                wet_data = xr.concat([wet_data, nbar_data.sel(time=slice(x, x))], 'time')
        # chose only from July - November in case no month range is specified
        st_mn = int(self.mnt_range.split(',')[0])
        en_mn = int(self.mnt_range.split(',')[1])

        dry_data = dry_data.sel(time=((dry_data['time.month'] >= st_mn) & (dry_data['time.month'] <= en_mn)))
        wet_data = wet_data.sel(time=((wet_data['time.month'] >= st_mn) & (wet_data['time.month'] <= en_mn)))
        print (" loaded nbar dry and wet data from " + str(st_mn) + " to " + str(en_mn) + 
                " months " +  str(datetime.now().time())) 
        if self.debug:
            print ("\nDry date list " + str([pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in dry_data.time.data]))
            print ("")
            print ("Wet date list" + str([pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in wet_data.time.data]))
            print ("") 
        return dry_data, wet_data

    def cov_task(self, acq_min, acq_max, dry_years, wet_years):
        #  gather latest datasets as per product names 
           
        dry_data, wet_data  = self.build_my_dataset(acq_min, acq_max, dry_years, wet_years)
        # calculate geo median 
        # For a slice of 1000:1000 for entire time seried do like  
        # smallds = ds_high.isel(x=slice(None, None, 4), y=slice(None, None, 4)) 
        print ("creating GEOMEDIAN for dry range " + str(datetime.now().time()))      
        med_dry = statistics.combined_var_reduction(dry_data, hd.nangeomedian)         
        print ("creating GEOMEDIAN for wet range " + str(datetime.now().time()))      
        med_wet = statistics.combined_var_reduction(wet_data, hd.nangeomedian)         
        key = ''
        if self.season.upper() != 'DUMMY':
            key = "GEO_" + str(acq_min) + "_" + str(acq_max) + "_" + self.season.upper() + "_DRY"
        else:
            key = "GEO_" + str(acq_min) + "_" + str(acq_max) + "_DRY"
        MY_GEO[key] = copy.deepcopy(med_dry)
        key = ''
        if self.season.upper() != 'DUMMY':
            key = "GEO_" + str(acq_min) + "_" + str(acq_max) + "_" + self.season.upper() + "_WET"
        else:
            key = "GEO_" + str(acq_min) + "_" + str(acq_max) + "_WET" 
        MY_GEO[key] = copy.deepcopy(med_wet)
        if (acq_max == self.end_epoch):
            print (" calculation finished and data is available in MY_GEO dictionary ", 
                   str(datetime.now().time()))      
        return


if __name__ == '__main__':
    '''
    The program gets all LANDSAT datasets excluding post March 2003 LS7 datasets and applied cloud free pq data.
    It accepts lon and lat ranges and rainfall range as mandatory. The epoch, year range and month range can also be passed
    to analyse geomedian during that month period. The default is from July to November. Once all the inputs are passed, it 
    extracts epoch/seasonal data and calculates geomedian for dry and wet datasets to a dictionary of six bands MY_GEO.
    Program processes small spatial range to cover more wider time frame.
    '''
    main()
