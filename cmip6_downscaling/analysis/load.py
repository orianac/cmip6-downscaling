import os

import xarray as xr
import zarr

from cmip6_downscaling.workflows.utils import get_store

KELVIN_TO_CELCIUS = -273.15
SECS_PER_DAY = 86400


def load_cmip(model, scenario, member, bias_corrected=False):

    if bias_corrected:
        prefix = f'cmip6/bias-corrected/conus/4000m/monthly/{model}.{scenario}.{member}.zarr'
    else:
        prefix = f'cmip6/regridded/conus/monthly/4000m/{model}.{scenario}.{member}.zarr'

    store = get_store(prefix)
    ds = xr.open_zarr(store, consolidated=True)

    if not bias_corrected:
        ds = ds.rename(
            {
                'pr': 'ppt',
                'hurs': 'rh',
                'rsds': 'srad',
                'tasmax': 'tmax',
                'tasmin': 'tmin',
                'rsds': 'srad',
            }
        )
        # Convert variables to align with units of terraclimate
        ds['tmax'] += KELVIN_TO_CELCIUS
        ds['tmin'] += KELVIN_TO_CELCIUS
        ds['rh'] /= 100.0
        ds['ppt'] *= SECS_PER_DAY

    return ds


def load_obs():
    mapper = zarr.storage.ABSStore(
        'carbonplan-downscaling',
        prefix='obs/conus/4000m/monthly/terraclimate_plus.zarr',
        account_name="carbonplan",
        account_key=os.environ["BLOB_ACCOUNT_KEY"],
    )
    ds = xr.open_zarr(mapper, consolidated=True)
    ds.x.attrs['units'] = 'm'
    ds.y.attrs['units'] = 'm'

    return ds
