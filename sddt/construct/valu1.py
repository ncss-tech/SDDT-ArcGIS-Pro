#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@Version: 0.1


So if the component has 
0 - 5 Oe awc: Null
5 - 20 A awc: 0.3
 
Then AWS for 0-20 is 45mm and the thickness for 0-20 would 15 cm
"""

import arcpy
from itertools import groupby
from itertools import zip_longest as zipl
import sys
import traceback
import numpy as np
from typing import Any, Generic, Iterator, Sequence, TypeVar, Callable



Shape = TypeVar("Shape", tuple, list)
DType = TypeVar("DType")
Key = TypeVar("Key", int, str)

class Array(np.ndarray, Generic[Shape, DType]):
    """  
    Use this to type-annotate numpy arrays, e.g. 
        image: Array['H,W,3', np.uint8]
        xy_points: Array['N,2', float]
        nd_mask: Array['...', bool]
        (https://stackoverflow.com/questions/35673895/
        type-hinting-annotation-pep-484-for-numpy-ndarray)
    """
    pass


Nx2 = TypeVar("Nx2", bound=Array[tuple[int, 2], float])
n1x2 = TypeVar("n1x2", bound=Array[tuple[1, 2], float])
n5 = TypeVar("n5", bound=Array[tuple[5], float])
Nx = TypeVar("Nx", bound=Array[tuple[int], float])


def pyErr(func: str) -> str:
    """When a python exception is raised, this funciton formats the traceback
    message.

    Parameters
    ----------
    func : str
        The function that raised the python error exception

    Returns
    -------
    str
        Formatted python error message
    """
    try:
        etype, exc, tb = sys.exc_info()
        
        tbinfo = traceback.format_tb(tb)[0]
        tbinfo = '\t\n'.join(tbinfo.split(','))
        msgs = (f"PYTHON ERRORS:\nIn function: {func}"
                f"\nTraceback info:\n{tbinfo}\nError Info:\n\t{exc}")
        return msgs
    except:
        return "Error in pyErr method"


def arcpyErr(func: str) -> str:
    """When an arcpy by exception is raised, this function formats the 
    message returned by arcpy.

    Parameters
    ----------
    func : str
        The function that raised the arcpy error exception

    Returns
    -------
    str
        Formatted arcpy error message
    """
    try:
        etype, exc, tb = sys.exc_info()
        line = tb.tb_lineno
        msgs = (f"ArcPy ERRORS:\nIn function: {func}\non line: {line}"
                f"\n\t{arcpy.GetMessages(2)}\n")
        return msgs
    except:
        return "Error in arcpyErr method"


def getVersion(tabs_d) -> str:
    try:
        if arcpy.Exists('version'):
            with arcpy.da.SearchCursor(**tabs_d['version']) as sCur:
                gssurgo_v = next(sCur)[0]
                return gssurgo_v
        else:
            gssurgo_v = '1.0'
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return ''
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return ''


def nanSum(base_arr: Nx2, i_arr: Nx2):
    """Sums two numpy arrays together similar to numpy nansum but while 
    preserving relationship NaN + Nan = Nan, as numpy >1.2 nansum 
    returns 0 for NaN + Nan
    This function does not return anything, it directly manipulates
    base_arr.

    Parameters
    ----------
    base_arr : Nx2
        The numpy array that will be manipulated by adding i_arr to it.
    i_arr : Nx2
        The input numpy array that will be added to base_arr. Must be the same 
        dimensions as base_arr.
    """
    try:
        was_nan = np.isnan(base_arr)
        still_nan = np.isnan(i_arr)
        # Index of base_arr cells flipping from nan
        flipped = np.logical_and(was_nan, ~still_nan)
        # Prepare base_arr layer cells that are no longer nan to accept 
        # new values from i_arr
        base_arr[flipped] = 0
        # flip nan properties to 0 so they don't propagate to base_arr
        i_arr[still_nan] = 0
        base_arr += i_arr
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def overlapRange(
        fixed: tuple[float, float], horizon: list[float, float]
    ) -> float:
    """Determines the thickness of the overlap between depth ranges
    of a soil layer and a genetic soil horizon.

    Parameters
    ----------
    fixed : tuple[float, float]
        The top and bottom depths of a soil layer.
    horizon : list[float, float]
        The top and bottom depths of a genetic soil horizon.

    Returns
    -------
    float
        Thickness of the intersection.
    """
    try: 
        depth_r = (
            (max(fixed[1], horizon[0]) - max(fixed[0], horizon[0]))
            - (max(fixed[1], horizon[1]) - max(fixed[0], horizon[1]))
        )
        if not depth_r:
            return np.nan
        else:
            return depth_r

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def awsCalc(thickness: int, awc: tuple[float]) -> float:
    """Calculates the available water storage for a given thickness of
    soil. It converts this to mm of water storage. 
    Conversion factor multiply by 10: 10 mm/cm

    Parameters
    ----------
    thickness : int
        Thickness of soil depth which represents the intersection of genetic
        soil horizon and a soil layer/zone in [cm]
    awc : float
        The available water holding capacity of the genetic soil horizon
        as proportion of soil volume.

    Returns
    -------
    float
        Available water storage in [mm].
    """
    awc, = awc
    return thickness * awc * 10


def socCalc(thickness: int, prop_i: tuple[float, float, int]) -> float:
    """Calculates grams of organic Carbon per square meter soil within a layer.

    Pedon layer volume = thickness [cm] * 100 [cm] * 100 [cm]: [cm]^3
    Proportion soil = (100 - fragvol) / 100
    Actual soil volume = thickness * 100 * (100 - fragval): [cm]^3
    Org C per volume = om / 1.724 / 100 * db: [g]/[cm]^3
        conversion factor of %SOM to %SOC from NSSH 618.44
    Org C mass = actual layer volume * Org C per volume
        thickness * (100 - fragvol) * om / 1.724 * db: [g]/[pedon layer]

    Parameters
    ----------
    om : float
        Horizon 
    thickness : int
        Thickness of the intersection of the soil horizon and the soil layer.
        [cm]
    prop_i : tuple[float, float, int]
        Three soil horizon properties
        1) Percent organic matter
        2) Soil bulk density: [g]/[cm]^3
        3) percent volume of rock fragments. Fragment volume is assumed
        to contribute 0 grams of organic Carbon.

    Returns
    -------
    float
        Mass of organic carbon in from soil horizon within soil layer.
        [g]
    """
    try:
        om, db, fragvol = prop_i
        soc = thickness * (100 - fragvol) * om / 1.724 * db
        return soc
    except:
        return np.nan


def byKey(x: Sequence, i: int=0) -> Any:
    """Helper function that returns ith element from a Sequence

    Parameters
    ----------
    x : Sequence
        Any indexable Sequence
    i : int, optional
        Index of element to be returned, by default 0

    Returns
    -------
    Any
        ith element from Sequence
    """
    return x[i]


def fragAg(chgrp: Iterator[Any]) -> float:
    """Retrive and sum fragments for each horizon. If RV is Null, estimate
    from the mean of the hi and lo values.

    Parameters
    ----------
    chgrp : Iterator
        Groupby Iterator object that has chkey, and rv, hi, 
        lo fragment volume %

    Returns
    -------
    float
        Sum of all fragments for horizon.
    """
    chgrp = list(chgrp)
    frag = 0
    for _, rv, hi, lo in chgrp:
        if rv is not None:
            frag += rv
        else:
            frag += (hi - lo) / 2
    return frag


def checkDensity(db: float, sand: float, silt: float, clay: float) -> bool:
    """Calculates whether a horizon is too dense for commodity crops 
    relative to the soil fine earth fraction.

    a = bulk density - sand * 0.0165 + silt * 0.0130 + clay * 0.0125
    b = 0.002081 * sand + 0.003912 * silt + 0.0024351 * clay
    if a > b, then too dense for commodity crops.

    Parameters
    ----------
    db : float
        Soil bulk density [g]/[cm]^3
    sand : float
        Percent sand [%]
    silt : float
        Percent silt [%]
    clay : float
        Percent clay [%]

    Returns
    -------
    bool
        Returns True if soil horizon is determined to be Dense or if 
        it can't be determined. Return False if soil horizon is not too dense.
    """
    try:
        txt_a = np.array([sand, silt, clay], dtype=np.float16)
        idx = txt_a != txt_a
        blanks = np.sum(idx)
        if blanks == 1:
            # Replace missing fine earth fraction
            txt_a[idx] = 100 - np.nansum(txt_a)

        elif blanks > 1:
            # Null values for more than one, return True so as to 
            # exclude horizon and below from AWS rootzone
            return True
        # All values required to run the Dense Layer calculation are available
        if np.sum(txt_a) != 100:
            return True
        
        if not db:
            return True
        if db <= 1.45:
            # it isn't dense, no matter the fine earth composition
            # smallest b: 0.2081 and 1.25 + 0.2081 = 1.4581
            return False
        a_coef = np.array([0.0165, 0.013, 0.0125])
        b_coef = np.array([0.002081, 0.003912, 0.0024351])
        a = db - (txt_a * a_coef).sum()
        b = (txt_a * b_coef).sum()
        if a > b:
            # This is a Dense Layer
            return True
        else:
            # This is not a Dense Layer
            return False
        
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return True
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return True


def horzByLayer(
        h_depths: list[float, float], 
        d_ranges: tuple[tuple[float, float], ...], 
        accum_prop: Nx2, 
        prop_i: tuple[float, float, int], 
        func: Callable
    ):
    """Accumulates a soil property for all genetic soil horizons that
    intersect the specified soil layers.

    While this function does not return anything, it directly manipulates
    the accum_prop numpy array.

    Parameters
    ----------
    h_depths : list[float, float]
        Top and bottom depths of genetic soil horizon [cm].
    d_ranges : list[tuple[float, float], ...]
        Top and bottom depths of fixed soil layers [cm].
    accum_prop : np.array
        An array that accumulates the soil property. Must be the same length
        as d_ranges.
    prop_i : tuple[float, float, int]
        Three soil horizon properties
        1) Percent organic matter
        2) Soil bulk density: [g]/[cm]^3
        3) percent volume of rock fragments. Fragment volume is assumed
        to contribute 0 grams of organic Carbon.
    func : function
        A function that transforms a soil property. This function will given
        the thickness of the intersection of the genetic soil horizon and
        soil depth layer and the soil property for the genetic soil horizon.
    """
    prop_ai = np.array([(
        (thick := overlapRange(layer_i, h_depths)),
        func(thick, prop_i)
        )
        for layer_i in d_ranges
    ])
    
    ### Maybe just call nansum here


    # Index of nan accumulated properties
    was_nan = np.isnan(accum_prop)
    still_nan = np.isnan(prop_ai)
    # Index of accum_prop cells flipping from nan
    flipped = np.logical_and(was_nan, ~still_nan)
    # Prepare accum_prop layer cells that are no longer nan to accept 
    # new values from prop_ai
    accum_prop[flipped] = 0
    # flip nan properties to 0 so they don't propagate to accum_prop
    prop_ai[still_nan] = 0
    accum_prop += prop_ai
    # # Nan trumps value with += operator, so use nansum
    # accum_prop = np.nansum(np.dstack((accum_prop, prop_ai)), 2)
    # # But numpy >1.2 nansum returns 0 for nan + nan, but we need nan
    # accum_prop[still_nan] = np.nan
    # return accum_prop


def horzAg(

        cokey: Key, d_ranges: tuple[tuple[float, float],],
        chors: Iterator[list[
            Key, str, float, float, int, int, int, float, float, float,
            float, float
        ],],
        cor1_depth: dict[Key, float], 
        cor2_depth: dict[Key, float], org_exempt: set[Key,],
        org_texture: set[Key,], fragvol_d: dict[Key, float],
        maj_earth_keys: set[Key,]
    ) -> tuple[Nx2, Nx2, n1x2]:
    """Aggregates component SOC and AWS by soil layer depths from each 
    genetic soil horizon.

    Parameters
    ----------
    cokey : Key
        Component key used to extract component restriction depths, organic
        texture flag, and major earthy component flag from cor1_depth, 
        cor2_depth, and maj_earth variables.
    d_ranges : tuple[tuple[float, float],]
        A sequence of depth pairs (top and bottom depths) of each soil depth 
        layer for which soil properties will be aggregated. [cm]
    chors : Iterator[list[ 
        Key, str, float, float, int, int, int, float, float, float, float, float
    ]]
        The soil properties retrieved from search cursor of component horizon:
        chkey, desgnmaster, hzdept_r [cm], hzdepb_r [cm], sandtotal_r [pct],
        silttotal_r [% volume], claytotal_r [% volume], om_r [% volume], 
        dbthirdbar_r [g/cm^3], ec_r [ds/m], ph1to1h2o_r [pH], awc_r [% volume]
    cor1_depth : dict[Key, float]
        If cokey present, depth to Lithic bedrock, Paralithic bedrock, or 
        Densic bedrock.
    cor2_depth : dict[Key, float]
        If cokey present, depth to Lithic bedrock, Paralithic bedrock, 
        Densic bedrock, Densic material, Fragipan, Duripan, or Sulfuric.
    org_exempt : set[Key,]
        If cokey present, soil is organic and organic surface is not excluded.
    org_texture : set[Key,]
        if chkey present, horizon has an organic texture.
    fragvol_d : dict[Key, float]
        If chkey present, horizon has rock fragments, [% volume].
    maj_earth_keys : set[Key,]
        If cokey present, component is a major soil map unit component.

    Returns
    -------
    tuple[Nx2, Nx2, Nx2]
        Returns three arrays with values and thickness [cm] for each 
        soil depth layer for available water storage [mm] and 
        soil organic carbon [g/m^2] and 
        the total available water storage [mm] within the commodity 
        root zone and the rootzone depth [cm].
    """
    try:
        if cokey in maj_earth_keys:
            maj_earth = True
        else:
            maj_earth = False
        if cokey in org_exempt:
            # Don't need to verify organic surface horizon
            org_flag = False
        else:
            org_flag = True
        org_thick = 0
        # Does a corestriction truncate commondity root zone
        if cokey in cor2_depth:
            b_depth = cor2_depth[cokey]
        else:
            b_depth = 150

        aws_a = np.zeros((len(d_ranges), 2), dtype= np.float32) * np.nan
        soc_a = np.zeros((len(d_ranges), 2), dtype= np.float32) * np.nan
        aws_r = np.zeros((1, 2), dtype= np.float32) * np.nan
        # calculate SOC and AWS for each layer from component horizons
        for horizon in chors:
            horizon = list(horizon)
            chkey = horizon[1]
            hor = horizon[2]
            h_depths1 = horizon[3: 5]
            h_depths2 = h_depths1
            sand = horizon[5]
            silt = horizon[6]
            clay = horizon[7]
            om = horizon[8]
            db = horizon[9]
            ec = horizon[10]
            ph = horizon[11]
            awc_i = horizon[-1]

            # get horizon Fragment volume
            if chkey in fragvol_d:
                frag_v = fragvol_d[chkey]
            else:
                frag_v = 0
            # get bedrock depth, exclude OM recorded for bedrock
            if (br_depth := cor1_depth.get(cokey)) and br_depth < h_depths2[1]:
                if br_depth <= h_depths2[0]:
                    om = None
                # This condition really should never happen
                else:
                    h_depths2 = (h_depths2[0], br_depth)

            if awc_i is not None:
                horzByLayer(h_depths1, d_ranges, aws_a, (awc_i,), awsCalc)
            if (om is not None) and db:
                horzByLayer(
                    h_depths2, d_ranges, soc_a, (om, db, frag_v), socCalc
                )

            # AWS commodity Rootzone
            if maj_earth:
                # if an organic horizon will not be checked for pH and density
                if chkey in org_texture or hor in ('O', 'L'):
                    org_hor = True
                else:
                    org_hor = False
                # Check if part of an organic surface
                if org_flag:
                    if org_hor:
                        # accumulate top organic horizon
                        org_thick += (h_depths1[1] - h_depths1[0])
                    else:
                        # any subsequent organic horizons assumed buried
                        org_flag = False
                # if not below restriction from a higher horizon
                # or below 150cm, check if horz is restrictive to commodidty:
                #   bulk density, EC and pH restrictions
                if h_depths1[0] < b_depth:
                    # Does EC truncate commodity root zone
                    if ec and ec >= 12:
                        b_depth = min(h_depths1[0], b_depth)
                        h_depths1[1] = min(h_depths1[1], b_depth)
                    # is horizon too acidic, 
                    # organic soils exempt as AL toxicity not an issue 
                    # per Bob Dobos
                    elif not org_hor and ph and ph <= 3.5 :
                        b_depth = min(h_depths1[0], b_depth)
                        h_depths1[1] = min(h_depths1[1], b_depth)
                    # is horizon too dense, 
                    # organic soils exempt as never too dense
                    elif not org_hor and checkDensity(db, sand, silt, clay):
                        b_depth = min(h_depths1[0], b_depth)
                        h_depths1[1] = min(h_depths1[1], b_depth)
                    # Calc AWS contribution to commondity rootzone
                    if awc_i is not None and not org_flag:
                        # Call horzByLayer for single layer, AWS rootzone
                        horzByLayer(
                            h_depths1, ((org_thick, b_depth),),
                            aws_r, (awc_i,), awsCalc
                        )
                       
        return (aws_a, soc_a, aws_r)
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def nccpiAg(
        nccpi_i: Iterator[list[Key, float, float, float, float, float]]
    ) -> n5:
    """Retrive and order NCCPI for each component.

    Parameters
    ----------
    nccpi : Iterator[list[Key, float, float, float, float, float]
        Groupby Iterator object that has ckey, and the five NCCPI values,
        37149: 'small grain', 37150: 'cotton', 44492: 'soy', 
        54955: 'NCCPI', 57994: 'corn'

    Returns
    -------
    n5
        Numpy array with 5 floats values:
        NCCPI values ordered: corn, soy, cotton, small grain, NCCPI
    """
    sg, cotton, soy, nccpi, corn = list(nccpi_i)
    # strip rule key as they are now sorted
    return np.array(
        [corn[1], soy[1], cotton[1], sg[1], nccpi[1]], dtype= np.float32
    )


def compAg(
        comps: Iterator[list[
            Key, Key, float, str, str, str, str, str, str
        ],],
        comp_horz_d: dict[Key, tuple[Nx2, Nx2, n1x2]],
        n_rows: int, pwsl_keys: set[Key,], maj_earth_keys: set[Key,],
        nccpi_d: dict[Key, n5]
    ) -> tuple[int, int, n5, Nx2, Nx2, n1x2, int, int, int, int, Key, int]:
    """This function summarizes all the bits of information for each 
    component of a map unit. It recevies data grouped by map unt from the
    groupby function.

    Parameters
    ----------
    comps : Iterator[list[Key, Key, float, str, str, str, str, str, str],]
        These are the elements packaged by map unit from the groupby interator 
        from the component table. For each component the following elements
        are sent:
        0) Key: map unit key (mukey)
        1) Key: component key (cokey)
        2) Component percentage
        3) Component name
        4) Component kind
        5) Local phase
        6) Other phase
        7) Hydric rating
        8) Drainage class
    comp_horz_d : dict[Key, tuple[Nx2, Nx2, n1x2]]
        dictionary of the horizon data aggregated by each soil depth layer
        by component, where N is the number of soil depth layers. Each
        row is the aggregated property and the thickness of intersection.
        Key: component key (cokey)
        First array: is the available water storage [mm]
        Second array: is the soil organic Carbon [g/m^2]
        Third array: is the available water storage of the NCCPI 
        "root zone" [cm]
    n_rows : int
        The number of soil depth layers.
    pwsl_keys : set[Key,]
        The component keys (cokeys) of soil components that are potentially
        wetlands.
    maj_earth_keys : set[Key,]
        The component keys (cokeys) of soil components that meet the 
        criteria of being an earthy and major component.
    nccpi_d : dict[Key, n5]
        The NCCPI ratings summarized from the cointerp table
        Key: component key (cokey)
        array: the five NCCPI values: corn, soy, cotton, small grain, NCCPI max

    Returns
    -------
    bool : tuple[int, int, n5, Nx2, Nx2, n1x2, int, int, int, int, Key, int]
        Aggrgated by map unit, summed percentage of major earthy components, 
        summed percentage of all components, weighted average of the five
        NCCPI values (corn, soy, cotton, small grain, NCCPI),
        the weighted average of availabe water storage [mm] and thickness [cm] 
        for each soil depth layer,
        the weighted average of soil organic carbon [g/m^2] and thickness [cm]
        for each soil depth layer,
        the weighted average of for all major earthy components of 
        availabe water storage [mm] summed for NCCPI root zone 
        and thickness [cm], 
        summed percentage of potential wetland components, 
        droughty flag (1 drought, 0 not), summed percentage of componets with
        available water capcity data, summed percentage of components with
        soil organic matter data.
        Key of the dominant component
        Component percentage of the dominant component
        Will return an empty tuple if unsuccessful.
    """
    try:
        drain_classes = {'Very poorly drained', 'Poorly drained'}
        # Percent Potential Wetland Soil Landscape (PWSL)
        pwsl_per = 0
        # Whether Null or incomplete
        pwsl_n = True
        hay_earth = False
        water_pct = 0
        maj_earth_pct = 0
        comp_pct_sum = 0
        soc_pct = 0
        aws_pct = 0
        dom_cokey = None
        dom_pct = 0
        # corn, soy, cotton, small grains, all
        nccpi = np.zeros((5), dtype= np.float32)

        # The numpy arrays store the thickness of intersection [0] and
        # accumulated property for each depth layer [1]
        aws_lyrs = np.zeros((n_rows, 2), dtype= np.float32) * np.nan
        soc_lyrs = np.zeros((n_rows, 2), dtype= np.float32) * np.nan
        aws_rz = np.zeros((1, 2), dtype= np.float32) * np.nan
        for comp in list(comps):
            pct = comp[2]
            if not pct:
                # skip component as it contributes nothing
                # what if aws is null?
                continue
            co_pro = pct / 100
            comp_pct_sum += pct
            cokey = comp[1]
            if cokey in maj_earth_keys:
                maj_earth_pct += pct
                if cokey in nccpi_d:
                    hay_earth = True
                    nccpi += nccpi_d[cokey] * co_pro

            # Summarize AWS and SOC
            if cokey in comp_horz_d:
                aws_lyrs_i, soc_lyrs_i, aws_rz_i = comp_horz_d[cokey]
                if aws_lyrs_i.any():
                    nanSum(aws_lyrs, aws_lyrs_i * co_pro)
                    nanSum(aws_rz, aws_rz_i * co_pro)
                    aws_pct += pct
                if soc_lyrs_i.any():
                    nanSum(soc_lyrs, soc_lyrs_i * co_pro)
                    soc_pct += pct

            # Component PWSL?
            name = comp[3].lower()
            kind = comp[4]
            local = comp[5].lower() if comp[5] else ''
            other = comp[6].lower() if comp[6] else ''
            hydric_rat = comp[7]
            drain = comp[8]

            # Dominant Component?
            if pct > dom_pct:
                dom_cokey = cokey
                dom_pct = pct
            if pct == dom_pct:
                # if tie and current misc, just use previous comp
                if kind != 'Miscellaneous area':
                    dom_cokey = cokey
                    dom_pct = pct
            # is it Water?
            if ((kind == 'Miscellaneous area' or kind is None) 
                and (name.startswith('water') or ' water' in name 
                     or 'swamp' in name)
                and 'table' not in name and name != 'waterhill'):
                water_pct += pct
            elif hydric_rat == 'Yes':
                pwsl_per += pct
                pwsl_n = False
            elif (hydric_rat == 'Unranked' 
                  and (cokey in pwsl_keys
                       or drain in drain_classes
                       # in local phase
                       or "drained" in local or "undrained" in local
                       or "channeled" in local or "protected" in local
                       or "ponded" in local or "flooded" in local
                       # other phase
                       or "drained" in other or "undrained" in other
                       or "channeled" in other or "protected" in other
                       or "ponded" in other or "flooded" in other)
            ):
                pwsl_per += pct
                pwsl_n = False
            elif hydric_rat is None and drain in drain_classes:
                pwsl_per += pct
                pwsl_n = False
            elif hydric_rat == 'No':
                pwsl_n = False
        # After each map unit component summarized
        # Summarize PWSL
        if water_pct >= 80:
            pwsl_per = 999
        elif pwsl_n:
            # All components have null hydric rating 
            # and not drain_classes and not water
            pwsl_per = None
        else:
            pwsl_per = int(pwsl_per)
        # calculate weighed average of major earthy components
        if hay_earth:
            maj_earth_pro = maj_earth_pct / 100
            aws_rz /= maj_earth_pro
            nccpi /= maj_earth_pro
            # Is the mapunit droughty?
            if aws_rz[0, 1] <= 152:
                droughty = 1
            elif aws_rz[0, 1] > 152:
                droughty = 0
            else:
                # Null
                droughty = None
        else:
            # if nccpi incomplete or no major earthy
            droughty = None
            nccpi *= np.nan
            aws_rz *= np.nan

        return (maj_earth_pct, comp_pct_sum, nccpi, aws_lyrs, soc_lyrs, aws_rz, 
                pwsl_per, droughty, aws_pct, soc_pct, dom_cokey, dom_pct)

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        msg = arcpyErr(func)
        arcpy.AddError(msg)
        return ()
    except:
        func = sys._getframe().f_code.co_name
        msg = pyErr(func)
        arcpy.AddError(msg)
        return () # msg + f"{nccpi_d[cokey]=}; {cokey=}"


def main(args):
    try:
        gdb_p = args[0] # r'D:\geodata\soils\gSSURGO\test\gSSURGO_DC.gdb' # 
        module_p = args[1] # r'D:\tools\NRCSbin\Soil-Data-Development-Tools---ArcGIS-Pro_AKS\sddt\construct' #
        d_ranges = (
            (0,5), (5, 20), (20, 50), (50, 100), (100, 150), (150, 999),
            (0, 20), (0, 30), (0, 100), (0, 150), (0, 999)
        )
        arcpy.env.workspace = gdb_p
        arcpy.env.overwriteOutput = True
        where = np.where
        isnan = np.isnan
        arcpy.AddMessage(f"{gdb_p = !s}")
        # tables needed
        # table: [[Fields], 'query', [sql clause]]
        in_table = 'in_table'
        field_names = 'field_names'
        where_clause = 'where_clause'
        sql_clause = 'sql_clause'
        gssurgo_v = getVersion({
            'version': {
                in_table: gdb_p + '/version',
                field_names: ['version'], 
                where_clause:"name = 'gSSURGO'" 
            }})
        arcpy.AddMessage(f"gSSURGO version {gssurgo_v}")
        
        if gssurgo_v == '2.0':
            # 37149: 'small grain', 37150: 'cotton', 44492: 'soy', 
            # 54955: 'NCCPI', 57994: 'corn'
            nccpi_keys = (37149, 37150, 44492, 54955, 57994)
            arcpy.management.ImportXMLWorkspaceDocument(
                gdb_p, f"{module_p}/valu1_v2.xml", "SCHEMA_ONLY"
            )
            arcpy.management.ImportXMLWorkspaceDocument(
                gdb_p, f"{module_p}/DominantComponent_v2.xml", "SCHEMA_ONLY"
            )
        else:
            nccpi_keys = ('37149', '37150', '44492', '54955', '57994')
            arcpy.management.ImportXMLWorkspaceDocument(
                gdb_p, f"{module_p}/valu1_v1.xml", "SCHEMA_ONLY"
            )
            arcpy.management.ImportXMLWorkspaceDocument(
                gdb_p, f"{module_p}/DominantComponent_v1.xml", "SCHEMA_ONLY"
            )
        arcpy.AddMessage("\tClean tables have been imported.")

        tabs_d = {
            'codiagfeatures': {
                in_table: gdb_p + '/codiagfeatures',
                field_names: ['cokey'], 
                where_clause: "featkind = 'Histic epipedon'"
            },
            "chfrags": {
                in_table: gdb_p + '/chfrags', field_names: [
                    "chkey", "fragvol_r", "fragvol_l", "fragvol_h"
                ],
                where_clause: (
                    "fragvol_r IS NOT NULL OR "
                    "(fragvol_l IS NOT NULL AND fragvol_h IS NOT NULL)"
                ),
                sql_clause: [None, "ORDER BY chkey ASC"]
            },
            'chorizon': {
                in_table: gdb_p + '/chorizon',
                field_names: [
                    'cokey', 'chkey', 'desgnmaster', 'hzdept_r',
                    'hzdepb_r', 'sandtotal_r', 'silttotal_r', 'claytotal_r',
                    'om_r', 'dbthirdbar_r', 'ec_r', 'ph1to1h2o_r', 'awc_r'
                ],
                where_clause: "hzdept_r IS NOT NULL AND hzdepb_r IS NOT NULL",
                sql_clause: [None, "ORDER BY cokey, hzdept_r ASC"]
            },
            "chtexturegrp": {
                in_table: gdb_p + '/chtexturegrp',
                field_names: ['chkey', 'chtgkey', 'texture'], 
                where_clause: "rvindicator = 'Yes'"
            },
            "chtexture": {
                in_table: gdb_p + '/chtexture',
                field_names: ["chtgkey"],
                where_clause: (
                    "lieutex IN ('Slightly decomposed plant material', "
                    "'Moderately decomposed plant material', "
                    "'Highly decomposed plant material', "
                    "'Undecomposed plant material', 'Muck', 'Mucky peat', "
                    "'Peat', 'Coprogenous earth')"
                )
            },
            "cointerp": {
                in_table: gdb_p + '/cointerp',
                field_names: ['cokey', 'interphr'],
                where_clause: f"rulekey IN {nccpi_keys}",
                sql_clause: [None, "ORDER BY cokey ASC, rulekey ASC"]
            },
            'component1': {
                in_table: gdb_p + '/component',
                field_names: ['cokey'],
                where_clause: (
                    "comppct_r IS NOT NULL "
                    "AND majcompflag = 'Yes'"
                    "AND (taxclname LIKE '%Hist%' OR taxsuborder LIKE '%Hist%' "
                    "OR taxorder = 'Histosols' OR taxsubgrp LIKE '%Hist%')"
                )
            },
            'component2': {
                in_table: gdb_p + '/component',
                field_names: ['cokey'],
                where_clause: (
                    "compkind <> 'Miscellaneous area' "
                    "AND majcompflag = 'Yes'"
                )
            },
            'component3': {
                in_table: gdb_p + '/component',
                field_names: [
                    'mukey', 'cokey', 'comppct_r', 'compname',
                    'compkind', 'localphase',
                    'otherph', 'hydricrating', 'drainagecl'
                ],
                #where_clause: "comppct_r IS NOT NULL",
                sql_clause: [None, "ORDER BY mukey ASC"]
            },
            "corestrictions1": {
                in_table: gdb_p + '/corestrictions',
                field_names: ["cokey", "resdept_r"],
                where_clause: (
                    "resdept_r IS NOT NULL AND resdept_r < 150 "
                    "AND reskind IN ('Lithic bedrock', 'Paralithic bedrock', "
                    "'Densic bedrock')"
                ),
                sql_clause: [None, "ORDER BY cokey ASC, resdept_r DESC"]
            },
            "corestrictions2": {
                in_table: gdb_p + '/corestrictions',
                field_names: ["cokey", "resdept_r"],
                where_clause: (
                    "resdept_r IS NOT NULL AND resdept_r < 150 AND reskind IN "
                    "('Lithic bedrock', 'Paralithic bedrock', "
                    "'Densic bedrock', 'Densic material', 'Fragipan', "
                    "'Duripan', 'Sulfuric')"
                ),
                sql_clause: [None, "ORDER BY cokey ASC, resdept_r DESC"]
            },
            "dom_com": {
                in_table: gdb_p + '/DominantComponent',
                field_names: ['mukey', 'cokey', 'comppct_r']
            },
            "mapunit": {
                in_table: gdb_p + '/mapunit',
                field_names: ['mukey'],
                where_clause: (
                    r"LOWER(muname) LIKE '%drained%' "
                    r"OR LOWER(muname) LIKE '%channeled%' "
                    r"OR LOWER(muname) LIKE '%protected%' "
                    r"OR LOWER(muname) LIKE '%ponded%' "
                    r"OR LOWER(muname) LIKE '%flooded%'"
                ),
                sql_clause: [None, "ORDER BY mukey"]
            },
            # Get date information for metadata
            'sacatalog': {
                in_table: gdb_p + '/sacatalog',
                field_names: ['SAVEREST'], 
                sql_clause: [None, "ORDER BY SAVEREST DESC"]
            },
            "sdvattribute": {
                in_table: gdb_p + '/sdvattribute',
                field_names: ["nasisrulename"],
                where_clause: (
                    "attributename like "
                    "'National Commodity Crop Productivity Index%'"
        )}}
    
        # components that have a histic epipedon
        with arcpy.da.SearchCursor(**tabs_d['codiagfeatures']) as sCur:
            hist_keys = {cokey for cokey, in sCur}
        # components that are histisols 
        # or hist part of subgroup, suborder, taxonomic class
        with arcpy.da.SearchCursor(**tabs_d['component1']) as sCur:
            hist_keys = {cokey for cokey, in sCur} | hist_keys

        # Major earthy components
        with arcpy.da.SearchCursor(**tabs_d['component2']) as sCur:
            maj_earth_keys = {cokey for cokey, in sCur}

        # get chorizon texture group key that have organic in lieu textures
        with arcpy.da.SearchCursor(**tabs_d['chtexture']) as sCur:
            chtxtgrp_keys = {key for key, in sCur}
        # Find RV organic textures
        org_textures = {
            'CE', 'COP-MAT', 'HPM', 'MPM', 'MPT', 'MUCK', 'PDOM', 'PEAT',
            'SPM', 'UDOM'
        }
        with arcpy.da.SearchCursor(**tabs_d['chtexturegrp']) as sCur:
            # RV texture is organic or in lieu texture is organic
            org_surf_keys = {
                chk for chk, chtk, txt in sCur 
                if txt in org_textures or chtk in chtxtgrp_keys
            }

        # Aggregate frags by horizon
        with arcpy.da.SearchCursor(**tabs_d['chfrags']) as sCur:
            chfrags = {chk: fragAg(fh)  for chk, fh in groupby(sCur, byKey)}

        # Compile component restriction depths
        with arcpy.da.SearchCursor(**tabs_d['corestrictions1']) as sCur:
            # Retrieve upper most restriction top depth
            # groupby returns this for each component with restriction(s)
            # [('24760546', 18), ('24760546', 76), ('24760546', 81)]
            # byKey returns the top most restriction and then 0 index for depth
            cor1_depths = {
                ck: byKey(list(cr))[1] for ck, cr in groupby(sCur, byKey)
            }
        with arcpy.da.SearchCursor(**tabs_d['corestrictions2']) as sCur:
            cor2_depths = {
                ck: byKey(list(cr))[1] for ck, cr in groupby(sCur, byKey)
            }

        # Aggregate horizon data
        with arcpy.da.SearchCursor(**tabs_d['chorizon']) as sCur:
            comp_horz_d = {
                ck: horzAg(
                    ck, d_ranges, h, cor1_depths, cor2_depths, hist_keys,
                    org_surf_keys, chfrags, maj_earth_keys
                )
                for ck, h in groupby(sCur, byKey)
            }

        # Get mukeys with PWSL keywords
        with arcpy.da.SearchCursor(**tabs_d['mapunit']) as sCur:
            pwsl_keys = {k for k, in sCur}

        # Get NCCPI values by cokey
        with arcpy.da.SearchCursor(**tabs_d['cointerp']) as sCur:
            nccpi_d = {ck: nccpiAg(nccpi) for ck, nccpi in groupby(sCur, byKey)}

        # Aggregate Components by mukey
        arcpy.AddMessage("\tPopulating tables")
        n_rows = len(d_ranges)
        dom_com_d = {}

        valu1_des = arcpy.Describe(f"{gdb_p}/Valu1")
        valu1_flds = [f.name for f in valu1_des.fields]
        valu1_flds = valu1_flds[1:]
        with (
            arcpy.da.InsertCursor('Valu1', valu1_flds) as iCur,
            arcpy.da.SearchCursor(**tabs_d['component3']) as sCur,
        ):
            for mk, comps in groupby(sCur, byKey):
                mu_t = compAg(
                    comps, comp_horz_d, n_rows, pwsl_keys, maj_earth_keys, 
                    nccpi_d
                )
                if not (pwsl := mu_t[6]):
                    pwsl = None
                # replace Nan with None and round
                nccpi = mu_t[2]
                nccpi = where(isnan(nccpi), None, nccpi.round(3))
                aws = mu_t[3]
                aws_v = where(isnan(aws[:,1]), None, aws[:,1].round(2))
                aws_t = where(isnan(aws[:,0]), None, aws[:,0].round(2))
                soc = mu_t[4]
                soc_v = where(isnan(soc[:,1]), None, soc[:,1].round(0))
                soc_t = where(isnan(soc[:,0]), None, soc[:,0].round(2))
                rz = mu_t[5]
                rz = where(isnan(rz), None, rz.round(2))
                # aws, aws thickness, aws comp pct 
                # soc, soc thickness, soc comp pct
                # nccpi, pct earthy comp
                # root zone, root zone aws
                # droughty, pwsl, sum pct comps, mukey
                v_row = [
                    *aws_v, *aws_t, mu_t[8],
                    *soc_v, *soc_t, mu_t[9],
                    *nccpi, mu_t[0],
                    *rz[0,:], 
                    mu_t[7], pwsl, mu_t[1], mk
                ]
                iCur.insertRow(v_row)
                dom_com_d[mk]= mu_t[-2:]
        # Populate Dominant Component table
        with arcpy.da.InsertCursor(**tabs_d['dom_com']) as iCur:
            for dom_com_its in dom_com_d.items():
                mk, (ck, pct) = dom_com_its
                iCur.insertRow([mk, ck, pct])

        return True

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


if __name__ == '__main__':
    main(sys.argv[1:])