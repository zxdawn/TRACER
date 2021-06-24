import time
import pandas as pd
import xarray as xr
import numpy as np
import dask.dataframe as dd
from multiprocessing import Pool
from pyresample import create_area_def
from pyresample.bucket import BucketResampler
import warnings
warnings.filterwarnings("ignore")

ais_dir = '../data/US_AIS/2020/'
savedir = '../data/US_AIS_density/'

# resample region and resolution
lon_min = -175
lon_max = -60
lat_min = 0
lat_max = 65
resample_res = 0.02

sd = '20200201'
ed = '20200630'

def read_data(file):
    df = dd.read_csv(file)

    meta = ('timestamp', 'datetime64[ns]')
    df.BaseDateTime = df.BaseDateTime.map_partitions(pd.to_datetime,
                                                     format='%Y-%m-%dT%H:%M:%S',
                                                     meta=meta)

    df = df.set_index('BaseDateTime')

    return df[['LAT', 'LON']].compute().to_xarray()

# create resampled bin
area = create_area_def('ship_area',
                       {'proj': 'longlat', 'datum': 'WGS84'},
                       area_extent=[lon_min-resample_res/2,
                                    lat_min-resample_res/2,
                                    lon_max+resample_res/2,
                                    lat_max+resample_res/2],
                       resolution=resample_res,
                       units='degrees',
                       description=f'{resample_res}x{resample_res} degree lat-lon grid')



def save_data(ship_density, file_pattern):
    # save to netcdf file
    comp = {'dtype': 'float32', 'zlib': True, 'complevel': 7}
    enc = {'ship_density': comp, 'longitude': comp, 'latitude': comp}
    savename = file_pattern.split('/')[-1].replace('csv', 'nc')

    ship_density.rename('ship_density').to_netcdf(savedir+savename,
                                               engine='netcdf4',
                                               encoding=enc
                                              )

def resample_ais(file_pattern):
    print(file_pattern)
    ds = read_data(file_pattern)

    # resample by 1 hour and bin data
    resample_ds = ds.resample(BaseDateTime='1H')
    resample_time = list(resample_ds.groups.keys())
    lon_coord, lat_coord = area.get_lonlats()

    # create empty DataArray for ship density data later
    ship_density = xr.DataArray(np.zeros((len(resample_time),)+lon_coord.shape),
                               coords=[resample_time,
                                       lat_coord[:, 0],
                                       lon_coord[0]],
                               dims=['time', 'latitude', 'longitude'])

    for group in resample_ds:
        lons = group[1].LON.chunk({'BaseDateTime': 1e4})
        lats = group[1].LAT.chunk({'BaseDateTime': 1e4})
        resampler = BucketResampler(area, lons.data, lats.data)
        counts = resampler.get_count()
        ship_density.loc[dict(time=group[0])] = counts
#         break

    save_data(ship_density, file_pattern)

# get filelist
file_patterns = pd.date_range(sd, ed, freq='d').strftime(f'{ais_dir}AIS_%Y_%m_%d.csv')

start = time.time()

# multiprocessing
pool = Pool(10)
pool.map(resample_ais, file_patterns)

end = time.time()
print(f"Runtime of the program is {end - start}")
