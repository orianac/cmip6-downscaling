import dask
import numpy as np
from esda.moran import Moran


def time_weighted_mean(ds, *args, **kwargs):
    weights = ds.time.dt.days_in_month
    return ds.weighted(weights).mean(dim='time')


def time_weighted_interannual_variability(ds, *args, **kwargs):
    """
    Given dataset returns one map of the standard deviation for
    annual values of the variables.
    """
    #    weights = ds.time.dt.days_in_month
    # couldn't figure out the weighting here so i'm skipping for now
    # MUST FIX - why can't you weight then groupby??
    return ds.groupby('time.year').sum().std(dim='year')


def time_weighted_seasonal_variability(ds, *args, **kwargs):
    """
    Given input dataset, returns 12 maps of the standard deviation for
    each variable.
    """
    #   weights = ds.time.dt.days_in_month
    # couldn't figure out the weighting here so i'm skipping for now
    # MUST FIX - why can't you weight then groupby??
    return ds.groupby('time.month').std()


def seasonal_cycle_mean(obj):

    return obj.mean(('x', 'y'))


def seasonal_cycle_std(obj):
    return obj.mean(('x', 'y'))


def moransI(obj, weights=None):
    """
    Inputs:
    obj: xarray object for which you want the autocorrelation
    weights: same shape as obj, but with weights which will be applied to
    obj to scale the autocorrelation. Simplest could be all ones.
    """
    if not weights:
        weights = np.ones_like(obj.values)

    return Moran(obj.values, weights)


# def extreme_scaling(obj, factors=[1, 2, 4, 8, 16], q=0.02):

#     for factor in factors:

#         obj_coarse = obj.coarsen(x=factor, y=factor).mean()
#         obj_quantile = obj_coarse.quantile(q=q).mean(('x', 'y'))


def select_valid_variables(terraclimate=True):
    if terraclimate:
        variables = ['ppt', 'rh', 'tmax', 'tmin', 'srad', 'pdsi', 'vap', 'pet']
    else:
        variables = ['ppt', 'rh', 'tmax', 'tmin', 'srad']
    return variables


def calc(obj, compute=False, regions=None):
    """
    This function takes an object and then calculates a
    series of metrics for that object. It returns it
    as a dictionary object of xarray objects. If the compute flag
    is turned on these objects will be in memory. Otherwise the
    computations will be lazy.
    """

    metrics = {}

    metrics['time_mean'] = time_weighted_mean(obj)
    metrics['seasonal_cycle_mean'] = seasonal_cycle_mean(obj)
    metrics['interannual_variability'] = time_weighted_interannual_variability(obj)
    metrics['seasonal_variability'] = time_weighted_seasonal_variability(obj)

    # metrics['seasonal_cycle_std'] =
    if regions:
        print('then well do the seasonal stuff  on the region boxes too')
    if compute:
        metrics = dask.compute(metrics)[0]

    return metrics


def create_mask(lat_lon_bounds_dict, ds):
    """
    This will create a mask that aligns with your ds of interest. Requires ds to have coordinates of
    lat and lon.
    """
    lat_bounds, lon_bounds = (
        lat_lon_bounds_dict["lat"],
        lat_lon_bounds_dict["lon"],
    )
    ds = (
        ds.where(ds.lat > lat_bounds[0])
        .where(ds.lat < lat_bounds[1])
        .where(ds.lon > lon_bounds[0])
        .where(ds.lon < lon_bounds[1])
    )
    # hacky- grab a sample variable
    sample_var = list(ds.data_vars)[0]
    mask = (
        (
            ds[sample_var]
            .where(ds.lat > lat_bounds[0])
            .where(ds.lat < lat_bounds[1])
            .where(ds.lon > lon_bounds[0])
            .where(ds.lon < lon_bounds[1])
            .isel(time=0)
            > 0
        )
        .drop(["member_id", "month", "lat", "lon", "time", "height"])
        .squeeze()
    )

    return mask
