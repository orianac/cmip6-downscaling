import math

import numpy as np

days_in_month = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
d2 = np.array([31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365])
d1 = np.array([1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335])


def mf(t, t0=-2, t1=6.5):
    if t < t0:
        f = 0
    elif t > t1:
        f = 1
    else:
        # see also https://github.com/abatz/WATERBALANCE/blob/master/runsnow.m
        # ann params from Dai 2008, Table 1a (row 1)
        a = -48.2292
        b = 0.7205
        c = 1.1662
        d = 1.0223
        # parametric form from Dai 2008
        f = 1 - (a * (np.tanh(b * (t - c)) - d) / 100.0)
    return f


def snowmod(tmean, ppt, radiation, snowpack_prev=None, albedo=0.23, albedo_snow=0.8):
    """Calculate monthly estimate snowfall and snowmelt.

    This function computes monthly estimated snowfall and snowmelt. Output includes end-of-month snowpack,
    water "input" (snowmelt plus rain), and albedo.

    Parameters
    ----------
    tmean : float
        Mean monthly temperatures in C
    ppt : float
        Monthly accumulated precipitation in mm
    radiation : float
        Shortwave solar radiation in MJ/m^2/day
    snowpack_prev : float, optional
        Snowpack at the beginning of the month. If None this is taken to be zero.
    albedo : float
        Albedo in the absence of snow cover.
    albedo_snow : float
        Albedo given snow cover

    Returns
    -------
    data: dict
        End-of-month snowpack, H2O input (rain plus snowmelt), and albedo.
    """

    if snowpack_prev is None:
        snowpack_prev = 0

    mfsnow = mf(tmean)

    # calculate values
    rain = mfsnow * ppt
    snow = ppt - rain
    melt = mfsnow * (snow + snowpack_prev)
    snowpack = snowpack_prev + snow - melt
    h2o_input = rain + melt

    if h2o_input <= 0:
        fractrain = 0.0
    else:
        fractrain = rain / h2o_input

    # make vector of albedo values
    if snowpack > 0 or snowpack_prev > 0:
        out_albedo = albedo_snow
    else:
        out_albedo = albedo

    extra_runoff = 0.05 * h2o_input
    h2o_input -= extra_runoff

    return dict(
        snowpack=snowpack,
        h2o_input=h2o_input,
        albedo=out_albedo,
        fractrain=fractrain,
        extra_runoff=extra_runoff,
        mfsnow=mfsnow,
    )


def monthly_PET(
    radiation,
    tmax,
    tmin,
    wind,
    dpt,
    tmean_prev,
    lat,
    elev,
    month,
    albedo=0.23,
):
    """Calculate monthly Reference ET estimates using the Penman-Montieth equation

    This function runs Reference ET estimates for monthly timesteps using methods based on
    the Penman-Montieth equation as presented in Allen et al (1998).  It incorporates a
    modification which adjusts stomatal conductance downwards at temperatures below 5 C.

    This version has been ported over from John Abatzoglou's implementation used
    for Terraclimate (https://github.com/abatz/WATERBALANCE)

    Parameters
    ----------
    radiation : float
        Monthly average shortwave radiation in MJ/m^2/day
    tmax : float
        Monthly average maximum temperature in C
    tmin : float
        Monthly average minimum temperature in C
    wind : float
        Monthly average wind speed in m/s at 10m above ground
    dpt : float
        Dewpoint temperature in C
    tmean_prev : float
        Mean temp of previous month in C
    lat : float
        Latitude in degrees
    elev : float
        Elevation in meters
    month : int
        Month of year (0 indexed)
    albedo : float
        Surface albedo, default=0.23

    Returns
    -------
    eto : float
    """

    doy = (d1[month] + d2[month]) / 2  # use middle day of month to represent monthly average.
    n_days = days_in_month[month]

    # calculate soil heat flux (total for the month) using change in temperature from previous month
    tmean = (tmax + tmin) / 2
    G = 0.14 * (tmean - tmean_prev) * n_days  # KEY CHANGE
    # convert to wind height at 2m
    hw = 10  # height of wind measurements
    wind = wind * (4.87 / math.log(67 * hw - 5.42))  # convert to wind height at 2m

    # stomatal conductance adjustment for low temperatures
    sr = 100  # stomatal resistance sec/m
    ks_min = 0.01  # minimum value for temps below T1
    Tl = -10  # minimum temp (sc goes to ks_min below this temp)
    T0 = 5  # optimal temp
    Th = 100  # maximum temp (sc goes to zero above this)
    thresh = 5  # temperature threshold below which to apply Jarvis equation (ks=1 above this temp)
    b4 = (Th - T0) / (Th - Tl)
    b3 = 1 / ((T0 - Tl) * (Th - T0) ** b4)
    ks = np.maximum(np.minimum(b3 * (tmean - Tl) * (Th - tmean) ** b4, 1), ks_min)

    # ks[np.isnan(ks)] = ks_min
    # ks[tmean >= thresh] = 1
    if np.isnan(ks):
        ks = ks_min
    if tmean >= thresh:
        ks = 1

    # convert to stomatal resistance.
    sr = sr / ks

    # ra is aerodynamic resistance, rs is bulk surface resistance
    ra = (
        208 / wind
    )  # (log((2-2/3*0.12)/(0.123*0.12))*log((2-2/3*0.12)/(0.1*0.123*0.12)))/(0.41**2*wind) # equal to 208/wind for hh=hw=2.
    rs = sr / (0.5 * 24 * 0.12)  # value of 70 when sr=100

    # Saturation vapor pressure

    P = 101.3 * ((293 - 0.0065 * elev) / 293) ** 5.26  # Barometric pressure in kPa

    es = (
        0.6108 * np.exp(tmin * 17.27 / (tmin + 237.3)) / 2
        + 0.6108 * np.exp(tmax * 17.27 / (tmax + 237.3)) / 2
    )
    ea = 0.6108 * np.exp(dpt * 17.27 / (dpt + 237.3))
    vpd = es - ea

    vpd = np.maximum(
        0, vpd
    )  # added because this can be negative if dewpoint temperature is greater than mean temp (implying vapor pressure greater than saturation).

    # delta - Slope of the saturation vapor pressure vs. air temperature curve at the average hourly air temperature
    delta = (4098 * es) / (tmean + 237.3) ** 2

    lhv = 2.501 - 2.361e-3 * tmean  # latent heat of vaporization
    cp = 1.013 * 10 ** -3  # specific heat of air
    # Changing this to who it is in John's petvpd
    gamma = cp * P / (0.622 * lhv)  # Psychrometer constant (kPa C-1)
    pa = P / (1.01 * (tmean + 273) * 0.287)  # mean air density at constant pressure

    # Calculate potential max solar radiation or clear sky radiation
    GSC = 0.082  # MJ m -2 min-1 (solar constant)
    phi = np.pi * lat / 180
    dr = 1 + 0.033 * np.cos(2 * np.pi / 365 * doy)
    delt = 0.409 * np.sin(2 * np.pi / 365 * doy - 1.39)
    omegas = np.arccos(-np.tan(phi) * np.tan(delt))
    Ra = (
        24
        * 60
        / np.pi
        * GSC
        * dr
        * (omegas * np.sin(phi) * np.sin(delt) + np.cos(phi) * np.cos(delt) * np.sin(omegas))
    )  # Daily extraterrestrial radiation
    Rso = Ra * (
        0.75 + 2e-5 * elev
    )  # For a cloudless day, Rs is roughly 75% of extraterrestrial radiation (Ra)
    Rso = np.maximum(0, Rso)
    # radfraction is a measure of relative shortwave radiation, or of
    # possible radiation (cloudy vs. clear-sky)
    radfraction = radiation / Rso
    radfraction = np.minimum(1, radfraction)

    # longwave  and net radiation
    longw = (
        4.903e-9
        * ((tmax + 273.15) ** 4 + (tmin + 273.15) ** 4)
        / 2
        * (0.34 - 0.14 * np.sqrt(ea))
        * (1.35 * radfraction - 0.35)
    )
    netrad = (radiation * (1 - albedo) - longw) * n_days

    # PET
    pet = (
        0.408
        * ((delta * (netrad - G)) + (pa * cp * vpd / ra * 3600 * 24 * n_days))
        / (delta + gamma * (1 + rs / ra))
    )

    return pet


def hydromod(t_mean, ppt, pet, awc, soil, snow_storage, mfsnow):
    """
    Run simple hydro model to calculate water balance terms. This is
    a translation from matlab into python of a simple hydro model
    written by John Abatzoglou. The original model is located here:
    https://github.com/abatz/WATERBALANCE/blob/master/hydro_tax_ro.m

    This function computes monthly estimated water balance terms given
    specified parameters and states.

    Parameters
    ----------
    t_mean : float
        Mean monthly temperatures in C
    ppt : float
        Monthly accumulated precipitation in mm
    pet : float
        Potential evapotranspiration
    awc : float
        Available water content (constant)
    soil : float
        Soil water storage at beginning of month.
    snow_storage : float
        Snow storage at beginning of month.
    mfsnow: float
        Melt fraction of snow as calculated in snow model.
    Returns
    -------
    data: dict
        Dictionary with data values for aet, deficit, runoff, snow_storage, soil, and runoff_snow
    """
    melt_fraction = mfsnow
    snowfall = (1 - melt_fraction) * ppt
    melt = melt_fraction * (snowfall + snow_storage)
    rain = ppt - snowfall
    input_h2o = rain + melt
    extra_runoff = input_h2o * 0.05
    input_h2o *= 0.95

    snow_drives_hydrology = True

    if snow_drives_hydrology:
        runoff_snow = min(extra_runoff, melt)
        r_rain = extra_runoff - runoff_snow
        rain_input = rain - r_rain
        excess_after_liquid = max(0, rain_input - pet)

    snow_storage = (1 - melt_fraction) * (snowfall + snow_storage)
    delta_soil = input_h2o - pet

    if (delta_soil < 0) and (snow_storage > -delta_soil):
        # if snowstorage has enough to fulfill delta_soil,
        # remove the delta_soil from snow_storage, assign it to
        # snow_drink and then zero-out delta_soil
        snow_storage += delta_soil
        snow_drink = -delta_soil
        delta_soil = 0
    elif (delta_soil < 0) and (snow_storage < -delta_soil):
        # if snow_storage doesn't have enough capacity to fulfill
        # the delta_soil need, bring delta_soil closer to zero by drinking
        # snow_storage and then zero-out snow_storage
        delta_soil += snow_storage
        snow_drink = snow_storage
        snow_storage = 0
    else:
        snow_drink = 0

    ## question for joe: what about corner case of soil_moisture=0 - john's never
    # acknowledges that - but maybe it's a matlab thing that > includes >= or something
    # now you've drunk your snowpack you'll draw from soil_moisture
    f1 = delta_soil < 0
    ff = -delta_soil > soil

    if ff:
        # if the need is greater than availability in soil then
        # constrain it to soil (this holds the water balance)
        delta_soil = -soil
    if f1:
        # if delta_soil is negative a.k.a. soil will drain
        drain_soil = delta_soil * (1 - np.exp(-soil / awc))
    else:
        drain_soil = 0

    demand = pet
    supply = input_h2o + snow_drink

    if demand >= supply:
        # if there is more evaporative demand than supply,
        # aet will be constrained by the soil drainage
        # and your deficit will be the difference between
        # pet and aet
        aet = supply - drain_soil
        deficit = pet - aet
        runoff = 0
        soil = soil + drain_soil
    else:
        # if there is enough water to satisfy pet then
        # aet will match pet and there is no deficit
        aet = pet
        deficit = 0

    # this boolean is hardcoded true right now
    if snow_drives_hydrology:
        excess = max(0, soil + delta_soil - awc)
        excess_rain_only = max(0, soil + excess_after_liquid - awc)
        if (demand < supply) and (soil + delta_soil > awc):
            # if supply exceeds demand and the updated soil water is greater than
            # the available capacity
            runoff = excess
            runoff_snow += excess - excess_rain_only
            soil = awc
        elif (demand < supply) and (soil + delta_soil <= awc):
            # if supply exceeds demand and updated soil water
            # is less than available water capacity
            soil += delta_soil
            runoff = 0

    return {
        'aet': aet,
        'deficit': deficit,
        'runoff': runoff,
        'snow_storage': snow_storage,
        'soil': soil,
        'runoff_snow': runoff_snow,
    }


def aetmod(et0, h2o_input, awc, soil_prev=None):
    """Calculate monthly actual evapotranspiration (AET)

    This function computes AET given ET0, H2O input, soil water capacity, and beginning-of-month soil moisture

    Parameters
    ----------
    eto : float
        Monthly reference evapotranspiration in mm
    h2o_input : float
        Monthly water input to soil in mm
    awc : float
        Soil water capacity in mm
    soil_prev : float, optional
        Soil water content for the previous month (mm)

    Returns
    -------
    data : dict
        aet, def, soil, and runoff
    """

    awc = np.maximum(awc, 10.0)
    if np.isnan(awc):
        awc = 50.0

    runoff = 0.05 * h2o_input
    h2o_input -= runoff

    if soil_prev is None:
        soil_prev = 0
    deltasoil = h2o_input - et0  # positive=excess H2O, negative=H2O deficit

    if deltasoil >= 0:
        # Case when there is a moisture surplus
        aet = et0
        deficit = 0
        soil = np.minimum(
            soil_prev + deltasoil, awc
        )  # increment soil moisture, but not above water holding capacity
        runoff += np.maximum(
            soil_prev + deltasoil - awc, 0
        )  # when awc is exceeded, send the rest to runoff
    else:  # deltasoil < 0
        # Case where there is a moisture deficit: soil moisture is reduced
        # this is the net change in soil moisture (neg)
        soildrawdown = soil_prev * (1 - np.exp(deltasoil / awc))
        aet = np.minimum(h2o_input + soildrawdown, et0)
        deficit = et0 - aet
        soil = soil_prev - soildrawdown

    return {"aet": aet, "soil": soil, "runoff": runoff, "deficit": deficit}
