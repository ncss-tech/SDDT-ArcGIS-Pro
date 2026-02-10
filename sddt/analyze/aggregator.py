#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 02/10/2026
    @by: Alexnder Stum
@Version: 0.6

# --- Update 02/05/2026; v 0.6
- Added a band-aid to accomodate domain values missing from SSURGO mdstatdomdet
table. This is only applicable in Dominant Condition Tie-breaks
# --- Update 02/05/2026; v 0.5
- compiled horizon values = 0 were not being passed to cursor and therefore
were null
- Add horzAbs function and enabled aggregator to determine the absolute
    min/max value of any horizon intersecting specified depth layer
# --- Update 02/05/2026; v 0.4.1
- Parsing error in constructing SQL query for component Search Cursor
# --- Update 02/05/2026; v 0.4
- Removed arcpyErr and pyErr functions, calling from sddt
# --- 
Updated 09/23/2025; v 0.3.4
- There are apparently components with Null % composition. Filtering those out
    at SearchCursor
# --- 
Updated 09/17/2025; v 0.3.3
- When DominantComponent table hasn't be created, the tab_d key was incorrect
# --- 
Updated 08/19/2025; v 0.3.2
- Consolidated comp_hi_con and comp_lo_con into comp_con and fixed errors
- Created domain_it function to handle tie's related to dominant condition 
    which leverages domain sequence thereby respecting ordinal rankings
- Fixed error with horzion where statement
- Fixed error with WTA and interps
- Made Min/Max more generic, fixed error with component level
- Made Most/Least limiting more generic, 
    it now accounts for suitability/limitation
- Added transforms for pH
# --- 
Updated 08/14/2025; v 0.3.1
- Fixed logic errors in comp_lo_con function when lower tie selected
- Added try block to comp_hi_con and comp_lo_con

# --- 
Updated 07/16/2025; v 0.3
- Replaced byKey function and a couple of lambda functions with itemgetter
- Placed lambda definitions outside of filter calls
- Horizon Minimum and Maximum ag handle np.nan
- Handle Percent Present
- Handle Primary and Secondary constraints
- Handle full profile horizon summary: 0-10000
- Interp table and column names per SDV when present

# ---
Updated 07/15/2025; v 0.2
- Fixed Dominant Component Horizon key and index errors
- Fixed precision lev1
- Fixed aggregation for nominal horizon properties

"""
v = '0.6'


import arcpy
from itertools import groupby
from operator import itemgetter as iget
import re
import sys

import numpy as np
from numpy import vectorize
from numpy import isnan
from math import log10
import os
from typing import Any, Generic, Iterator, Sequence, TypeVar, Callable, Union
from sortedcontainers import SortedList

from .. import pyErr
from .. import arcpyErr


Numeric = Union[int, float]
Key = TypeVar("Key", int, str)
Shape = TypeVar("Shape", tuple, list)
DType = TypeVar("DType")
Key = TypeVar("Key", int, str)


def do_twice(func):
    def wrapper_do_twice(*args, **kwargs):
        func(*args, **kwargs)
        func(*args, **kwargs)
    return wrapper_do_twice


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


def dom_com(tabs_d, gs_v, gdb_p, module_p):
    try:
        if not arcpy.Exists(f"{gdb_p}/DominantComponent"):
            # Create Dominant Component table is it doesn't exist
            arcpy.AddMessage('Creating Dominant Component table')
            with arcpy.da.SearchCursor(**tabs_d['comp2']) as sCur:
                # groupby mukey, then sort by percent and select last (largest)
                dom_com_d = {
                    mk: sorted([(pct, ck) for _, ck, pct in comps])[-1] 
                    for mk, comps in groupby(sCur, iget(0))
                }
            construct_p = os.path.dirname(module_p) + '/construct'
            if gs_v == "1.0":
                arcpy.AddMessage(f"{construct_p}/DominantComponent_v1.xml")
                arcpy.management.ImportXMLWorkspaceDocument(
                    gdb_p, 
                    f"{construct_p}/DominantComponent_v1.xml", "SCHEMA_ONLY"
                )
            else:
                arcpy.AddMessage(f"{construct_p}/DominantComponent_v2.xml")
                arcpy.management.ImportXMLWorkspaceDocument(
                    gdb_p, 
                    f"{construct_p}/DominantComponent_v2.xml", "SCHEMA_ONLY"
                )
            with arcpy.da.InsertCursor(**tabs_d['Dominant2']) as iCur:
                for mk, (pct, ck) in dom_com_d.items():
                    iCur.insertRow([mk, ck, pct])
        # return component keys of dominant compoents
        with arcpy.da.SearchCursor(**tabs_d['Dominant1']) as sCur:
                cokeys = {ck for ck, in sCur}
        return cokeys

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False
    

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
        was_nan = isnan(base_arr)
        still_nan = isnan(i_arr)
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


def propCalc(thickness: int, prop: tuple[Numeric]) -> float:
    """Weigths a given horizon property by the thickness of intersection

    Parameters
    ----------
    thickness : int
        Thickness of soil depth which represents the intersection of genetic
        soil horizon and a soil layer/zone in [cm]
    prop : Numeric
        The horizon property.

    Returns
    -------
    float
        Property weighted by thickness.
    """
    prop, = prop
    return thickness * prop


def byKey3(x: Sequence, i: int=0) -> Any:
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


# WSS does the arithmetic mean of pH and not proton activity mean
# These functions available for calculating proton activity mean
def nada(v): return v
def nada2(v, p): round(v, p)
def toH(pH): return 10**-pH
def topH(H): return -log10(H)
def topH2(H, p): return round(-log10(H), p)


def horzAg(d_ranges: tuple[tuple[float, float],],
           chors: Iterator[list[Key, int, int, Numeric]], pH: bool=False
    ) -> Numeric:
    """Aggregates soil properties by soil layer depths from each 
    genetic soil horizon. It is called by horizons grouped cokey

    Parameters
    ----------
    d_ranges : tuple[tuple[float, float],]
        A sequence of depth pairs (top and bottom depths) of each soil depth 
        layer for which soil properties will be aggregated. [cm]
    chors : Iterator[list[Key, int, int, Numeric]]
        The component key (not used), the horizon property to be
        aggregated across all horizons that intersect the depth layer, and
        horizion depths: hzdept_r [cm], hzdepb_r [cm].

    Returns
    -------
    The weighted average of the property across all horizons that intersect
    the layer
        
    """
    try:
        # only query chors that have a value
        # query by depth range
        # Use below for proton activity mean
        # if pH:
        #     transform1 = toH #lambda ph: 10**-ph
        #     transform2 = vectorize(topH) # lambda H: -log10(H)
        # else:
        #     transform1 = nada #lambda ph: ph
        #     transform2 = nada #lambda H: H

        prop_a = np.zeros((len(d_ranges), 2), dtype= np.float32) * np.nan

        # Aggregate property for each intersecting horizon
        for horizon in chors:
            # unpack
            horizon = list(horizon)
            prop_i = horizon[-1] # transform1(horizon[-1])
            h_depths = horizon[1:3]
            
            horzByLayer(h_depths, d_ranges, prop_a, prop_i) #, propCalc)

        return prop_a[:,1] / prop_a[:,0] #transform2(prop_a[:,1] / prop_a[:,0])
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        raise


def horzAbs(d_ranges: tuple[tuple[float, float],],
            chors: Iterator[list[Key, int, int, Numeric]], mORm: Callable
    ) -> Numeric:
    """Finds the absolute maximum or minimum soil properties 
    by soil layer depths from each 
    genetic soil horizon. It is called by horizons grouped cokey

    Parameters
    ----------
    d_ranges : tuple[tuple[float, float],]
        A sequence of depth pairs (top and bottom depths) of each soil depth 
        layer for which soil properties will be aggregated. [cm]
    chors : Iterator[list[Key, int, int, Numeric]]
        The component key (not used), the horizon property to be
        aggregated across all horizons that intersect the depth layer, and
        horizion depths: hzdept_r [cm], hzdepb_r [cm].

    Returns
    -------
    The weighted average of the property across all horizons that intersect
    the layer
        
    """
    try:
        props = []
        # Aggregate property for each intersecting horizon
        for horizon in chors:
            # unpack
            horizon = list(horizon)
            prop_i = horizon[-1]
            h_depths = horizon[1:3]
            for layer_i in d_ranges:
                # Does horizon overlap depth layer?
                if not isnan(horOverlap(layer_i, h_depths)):
                    props.append(prop_i)
                    break

        return np.array([mORm(props),]) 
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        raise


def horzModal(d_ranges: tuple[tuple[float, float],],
           chors: Iterator[list[Key, str, int, int]],
    ) -> list[str]:
    """Determines the dominant categorical soil property by soil layer depths 
    from each genetic soil horizon. It is called by horizons grouped cokey

    Parameters
    ----------
    d_ranges : tuple[tuple[float, float],]
        A sequence of depth pairs (top and bottom depths) of each soil depth 
        layer for which soil properties will be aggregated. [cm]
    chors : Iterator[list[Key, str, int, int]]
        The component key (not used), the categorical horizon property, and
        horizion depths: hzdept_r [cm], hzdepb_r [cm].

    Returns
    -------
    The dominant categorical property for each layer depth
        
    """
    try:
        # group depths by category for each intersecting horizon
        categories = []
        depths = []
        for horizon in chors:
            # unpack
            horizon = list(horizon)
            categories.append(horizon[-1])
            h_depths = horizon[1:3]
            
            depths.append(
                [horOverlap(layer_i, h_depths) for layer_i in d_ranges]
            )
        # Create a list of dominant properties by depth layer
        dom_prop = [
            max(grp_depths := sumby(categories, d_list), key=grp_depths.get) 
            for d_list in zip(*depths)
        ]
    
        return dom_prop
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        raise
        return False
    

def sumby(cats, values):
    grpby = {}
    for cat, dp in zip(cats, values):
        if cat is None:
            cat = 'unpopulated'
        if cat in grpby:
            grpby[cat] += dp
        else:
            grpby[cat] = dp
    return grpby


def horzByLayer(
        h_depths: list[float, float], 
        d_ranges: list[list[float, float],], 
        accum_prop: Nx2, 
        prop_i: Numeric, 
        #func: Callable
    ):
    """Accumulates a soil property for a genetic soil horizon that
    intersect the specified depth layers.

    This function does not return anything as it directly manipulates
    the accum_prop numpy array.

    Parameters
    ----------
    h_depths : list[float, float]
        Top and bottom depths of genetic soil horizon [cm].
    d_ranges : list[tuple[float, float], ...]
        Top and bottom depths of fixed soil layers [cm].
    accum_prop : np.array
        An array that accumulates the soil property. Must be the same length
        as d_ranges. Column 0 is the accumulated thickness 
        and column 1 the accumulated property
    prop_i : Numeric
        Property of the current soil horzion
    func : function
        A function that transforms a soil property. This function will given
        the thickness of the intersection of the genetic soil horizon and
        soil depth layer and the soil property for the genetic soil horizon.
    """
    try:
        prop_ai = np.array([
            ((thickness := horOverlap(layer_i, h_depths)), thickness * prop_i)
            for layer_i in d_ranges
        ])
        
        ### Maybe just call nansum here

        # Index of nan accumulated properties
        ac_nan = isnan(accum_prop[:,0])
        # Index of nan properties in current horizon
        ai_nan = isnan(prop_ai[:,0])
        k_nan = np.logical_and(ac_nan, ai_nan)
        accum_prop[~k_nan, :] = np.nansum([accum_prop, prop_ai], 0)
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        raise


def horOverlap(
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

        return depth_r or np.nan

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False
    

def hor2comp(
        comps: Iterator[list[Key, Key, int]] , comp_ag_d: dict[Key, Any]
    )-> list[Any, int]:
    """Collates aggregated horizon data within a map unit component group

    Parameters
    ----------
    comps : Iterator[Key, int, Numeric]
        A set of components with and each has the map unit key, comonent key, 
        component percentage

    comp_ag_d : dict[Key, Any]
        The aggregated horizon property (value) by component key (key)

    Returns
    -------
    list[Any, int]
        aggregated horizon property, component percentage
    """
    try:
        return [(prop[0], pct) if (prop := comp_ag_d.get(ck) is not None) 
                else ('not horizonated', pct) for _, ck, pct in comps]
    except:
            func = sys._getframe().f_code.co_name
            arcpy.AddError(pyErr(func))
            arcpy.AddError(f"{list(comps)}")
            return None


def interp_node(
        ag_method: str, mapunits: dict, tabs_d: dict,
        gssurgo_v: str, gdb_p: str, module_p: str, tiebreak: str, 
        fuzzy: bool, interp_n: str
                ):
    try: 
        # Ignore not rated map units (null ratings)
        if(gssurgo_v == "1.0" and ag_method 
           in ('Most Limiting', 'Least Limiting', 'Weighted Average')
        ):
            # 'cokey', 'interphrc', 'interphr'
            with arcpy.da.SearchCursor(**tabs_d['cointerp1nn']) as sCur:
                # cokey: [interp class, interp value]
                cointerp_d = {ck: [cl, val] for ck, cl, val in sCur}
        elif gssurgo_v == "1.0":
            # 'cokey', 'interphrc', 'interphr'
            with arcpy.da.SearchCursor(**tabs_d['cointerp1']) as sCur:
                # cokey: [interp class, interp value]
                cointerp_d = {ck: [cl, val] for ck, cl, val in sCur}
        elif(
            ag_method in ('Most Limiting', 'Least Limiting', 'Weighted Average')
        ):
            # Read in mdruleclass table
            with arcpy.da.SearchCursor(**tabs_d['mdruleclass']) as sCur:
                mdrules = dict(sCur)
            # Read in cointerp table
            # 'cokey', 'interphrck', 'interphr'
            with arcpy.da.SearchCursor(**tabs_d['cointerp2nn']) as sCur:
                # cokey: [interp class, interp value]
                cointerp_d = {
                    ck: [mdrules[cls_k], val] for ck, cls_k, val in sCur
                }
        else:
            # Read in mdruleclass table
            with arcpy.da.SearchCursor(**tabs_d['mdruleclass']) as sCur:
                mdrules = dict(sCur)
            # Read in cointerp table
            # 'cokey', 'interphrck', 'interphr'
            with arcpy.da.SearchCursor(**tabs_d['cointerp2']) as sCur:
                # cokey: [interp class, interp value]
                cointerp_d = {
                    ck: [mdrules[cls_k], val] for ck, cls_k, val in sCur
                }

        if ag_method == 'Dominant Component':
            cokeys = dom_com(tabs_d, gssurgo_v, gdb_p, module_p)
            
            with (
            arcpy.da.InsertCursor(**tabs_d['property']) as iCur,
            arcpy.da.SearchCursor(**tabs_d['comp2']) as sCur,
            ):
                for mk, ck, pct in sCur:
                    if ck in cokeys and ck in cointerp_d:
                        cl_val = cointerp_d[ck] #cl = cointerp_d[ck][0]
                        iCur.insertRow([mapunits.get(mk), mk, pct] + cl_val) #+ cl
            return True

        if ag_method == 'Weighted Average':
            # remove class column
            del_fld = tabs_d['property']['field_names'].pop(3)
            arcpy.management.DeleteField(
                tabs_d['property']['in_table'], del_fld
            )
            # No interp class with Weighted Average
            with (
                arcpy.da.SearchCursor(**tabs_d['comp2']) as sCur,
                arcpy.da.InsertCursor(**tabs_d['property']) as iCur
            ):
                for mk, comps in groupby(sCur, iget(0)):
                    # collate inteprs values by map unit, mk just place holder
                    # for comp_wtavg rating class, percent, rating
                    cointerps = [
                            [mk, ck, pct, cint[1]] for _, ck, pct in comps 
                            if(cint := cointerp_d.get(ck))
                        ] or [[mk, '', None, None],]
                    if len(cointerps) > 1:
                        pct, val = comp_wtavg(cointerps)
                        val = round(val, 2)
                        iCur.insertRow(
                            [mapunits.get(mk), mk, pct, val]
                        )
                    else:
                        _, _, pct, val = cointerps[0]
                        if val:
                            val = round(val, 2)
                        iCur.insertRow(
                            [mapunits.get(mk), mk, pct, val]
                        )
            return True
        
        # to sort by interp class
        if ag_method == "Dominant Condition":
            with (
                arcpy.da.SearchCursor(**tabs_d['comp2']) as sCur,
                arcpy.da.InsertCursor(**tabs_d['property']) as iCur
            ):
                if tiebreak == 'Higher':
                    vi = -1
                    null = -1
                else:
                    vi = 0
                    null = 2
                for mk, comps in groupby(sCur, iget(0)):
                    # collate inteprs values by map unit 
                    # [mk, ck, % composition, interp rating, interp class]
                    cointerps = [
                        [mk, ck, pct, cint[1], cint[0]] 
                        for _, ck, pct in comps 
                        if(cint := cointerp_d.get(ck))
                    ] or [[mk, ck, None, 0, 'Not Rated'],]
                    # Determine which interp class is dominant
                    # Where compsition tied, max valued class returned
                    if len(cointerps) > 1:
                        pct, cl, val = comp_con(cointerps, 4, vi, null) #pct, cl, val = comp_con(cointerps, 4, vi, null)
                    else:
                        _, _, pct, val, cl = cointerps[0]
                    iCur.insertRow([mapunits.get(mk), mk, pct, cl, val])

        # Least or Most
        else:
            # Determine if interp is a suitability or limitation
            sa_p = gdb_p + '/sainterp'
            with arcpy.da.SearchCursor(
                sa_p, 'interptype', where_clause=f"interpname = '{interp_n}'"
            ) as sCur:
                itype = next(sCur)[0]
            arcpy.AddMessage(f"Evaluating as {itype}")
            # Function dicitionary
            v_lev = None
            func_d = {
                1: lambda ci: ci[2] == v_lev, # filter by fuzzy value
                2: lambda ci: ci[1] == cls, # filter by interp class
                'Least Limitinglimitation': min,
                'Most Limitingsuitability': min,
                'Least Limitingsuitability': max,
                'Most Limitinglimitation': max,
                3: 1, # slice only first element
                4: 2, # slice both elements
            }
            f_func = func_d[fuzzy + 1]
            m_func = func_d[ag_method + itype]
            #i = func_d[fuzzy + 3]

            with (
                arcpy.da.SearchCursor(**tabs_d['comp2']) as sCur,
                arcpy.da.InsertCursor(**tabs_d['property']) as iCur
            ):
                for mk, comps in groupby(sCur, iget(0)):
                    # collate inteprs values by map unit 
                    # [% composition, interp class, interp value]
                    cointerps = [
                        [pct, *cint] for _, ck, pct in comps 
                        if(cint := cointerp_d.get(ck))
                    ] or [[None, 'Not Rated', None],]
                    # find minimum interp value
                    if len(cointerps) > 1:
                        pcts, clss, vals = list(zip(*cointerps))
                        # get min or max fuzzy value
                        v_lev = m_func(vals)
                        # get interp class of selected fuzzy value
                        cls = clss[vals.index(v_lev)]

                        # apply indexing function to filter and sum %
                        pct = sum([
                                i_grp[0] for i_grp in filter(f_func, cointerps)
                        ])
                        cl_and_val = [cls, v_lev]

                    else:
                        pct = cointerps[0][0]
                        cl_and_val = cointerps[0][1:]
                    iCur.insertRow(
                        [mapunits.get(mk), mk, pct] + cl_and_val#[:i]
                    )
        return True

    except arcpy.ExecuteError:
        # arcpy.AddError(f"{cointerps}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        # arcpy.AddError(f"{cl_and_val}")
        # arcpy.AddError(f"{[mapunits.get(mk), mk, pct] + cl_and_val[:i]}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False
    

def comp_node(
        ag_method: str, mapunits: dict, tabs_d: dict, gssurgo_v: str,
        gdb_p: str, module_p: str, tiebreak: str, p: int, q: str, delim: str,
        comp_ag_d: dict[Key, Numeric]=None, domain_d= {}, pH:bool=False
    ):
    # mukey, component percent, specified property
    try:
        if comp_ag_d:
            # where information aggregated from other than component
            comp_call = 'comp2'
        else:
            comp_call = 'comp1'
        
        if ag_method == 'Dominant Component':
            # Specify only major components
            tabs_d[comp_call]['where_clause'] += " AND majcompflag = 'Yes'"

            ####### This needs to use Dominant Comp table #########
            cokeys = dom_com(tabs_d, gssurgo_v, gdb_p, module_p)

            tabs_d[comp_call]['where_clause'] += \
                f""" AND cokey IN ({q}{delim.join(cokeys)}{q})"""

            with (
                    arcpy.da.InsertCursor(**tabs_d['property']) as iCur,
                    arcpy.da.SearchCursor(**tabs_d[comp_call]) as sCur,
                ):
                # Numeric, round to p and no sequence field
                if p is not None:
                    if comp_ag_d:
                            for mk, ck, pct in sCur:
                                # is it the dom com with horizon data?
                                if ck in comp_ag_d:
                                    iCur.insertRow(
                                        [mapunits.pop(mk), mk, pct, 
                                        round(comp_ag_d[ck][0], p)]
                                    )
                    else:
                        for mk, ck, pct, prop in sCur:
                                iCur.insertRow(
                                    [mapunits.pop(mk), mk, pct, prop]
                                )
                    # Populate Null
                    for mk, asym in mapunits.items():
                        iCur.insertRow([asym, mk, None, None])
                else:
                    if comp_ag_d:
                        for mk, ck, pct in sCur:
                            # is it the dom com with horizon data?
                            if ck in comp_ag_d:
                                #prop = comp_ag_d[ck][0]
                                iCur.insertRow(
                                    [mapunits.pop(mk), mk, pct, 
                                     comp_ag_d[ck][0], prop, domain_d[prop]]
                                )
                    else:
                        for mk, ck, pct, prop in sCur:
                            iCur.insertRow(
                                [mapunits.pop(mk), mk, pct, 
                                 prop, domain_d[prop]]
                            )
                    # Populate Null
                    for mk, asym in mapunits.items():
                        iCur.insertRow([asym, mk, None, None, None])

        elif ag_method == 'Weighted Average':
            with (
                arcpy.da.SearchCursor(**tabs_d[comp_call]) as sCur,
                arcpy.da.InsertCursor(**tabs_d['property']) as iCur
            ):
                if comp_ag_d:
                    # Activate if proton activity mean is desired
                    # if pH:
                    #     transform1 = toH #lambda ph: 10**-ph
                    #     transform2 = topH2 # lambda H: round(-log10(H), p)
                    # else:
                    #     transform1 = nada # lambda ph: ph
                    #     transform2 = nada2 # lambda prop_x: round(prop_x, p)
                    for mk, comps in groupby(sCur, iget(0)):
                        comps_p=[
                            # transform1(hor[0]
                            (mk, ck, pct, hor[0]) 
                            for mk, ck, pct in comps
                            if (hor := comp_ag_d.get(ck) is not None) 
                        ]
                        if comps_p:
                            # arcpy.AddMessage(f"{comps_p}")
                            pct, prop = comp_wtavg(comps_p)
                            iCur.insertRow(
                                # transform2(prop, p)
                                [mapunits.pop(mk), mk, pct, round(prop, p)]
                            )
                else:
                    for mk, comps in groupby(sCur, iget(0)):
                        pct, prop = comp_wtavg(comps)
                        iCur.insertRow(
                            [mapunits.pop(mk), mk, pct, round(prop, p)]
                        )
                # Populate Null
                for mk, asym in mapunits.items():
                    iCur.insertRow([asym, mk, None, None])

        elif ag_method == "Dominant Condition":
            # sort by third element, the property
            with (
                arcpy.da.SearchCursor(**tabs_d[comp_call]) as sCur,
                arcpy.da.InsertCursor(**tabs_d['property']) as iCur
            ):
                if tiebreak == 'Higher':
                    vi = -1
                else:
                    vi = 0
                if comp_ag_d:
                    # apply function to comps that look up sequence?
                    for mk, comps in groupby(sCur, iget(0)):
                        comps_p = hor2comp(comps, comp_ag_d)
                        comps_p2 = domain_it(comps_p, domain_d)
                        pct, prop, seq = comp_con(comps_p2, 3, vi, None)
                        iCur.insertRow([mapunits.pop(mk), mk, pct, prop, seq])
                else:
                    for mk, comps in groupby(sCur, iget(0)):
                        comps_p2 = domain_it(comps, domain_d)
                        # arcpy.AddMessage(f"{comps_p2}")
                        pct, prop, seq = comp_con(comps_p2, 3, vi, None)
                        iCur.insertRow([mapunits.pop(mk), mk, pct, prop, seq])
                # Populate Null
                for mk, asym in mapunits.items():
                    iCur.insertRow([asym, mk, None, None, None])

        elif ag_method == "Percent Present":
            # This method requires a selection of a primary constraint
            # remove seq column
            del_fld = tabs_d['property']['field_names'].pop()
            arcpy.management.DeleteField(
                tabs_d['property']['in_table'], del_fld
            )
            with (
                arcpy.da.SearchCursor(**tabs_d[comp_call]) as sCur,
                arcpy.da.InsertCursor(**tabs_d['property']) as iCur
            ):
                if comp_ag_d:
                    for mk, comps in groupby(sCur, iget(0)):
                        props = set()
                        pct = 0
                        for _, ck, c_pct in comps:
                            if(h_props := comp_ag_d.get(ck)) is not None:
                                props.update(h_props)
                                pct += c_pct
                        # prop_str = ', '.join(props)
                        iCur.insertRow(
                            [mapunits.pop(mk), mk, sum(pcts), ', '.join(props)] #prop_str, domain_d[prop_str]
                        )
                else:
                    for mk, comps in groupby(sCur, iget(0)):
                        # Sum percents of comps and collate primary in map unit
                        pcts, props = zip(*[comp[2:] for comp in comps])
                        props = set(props)
                        # prop_str = ', '.join(props)
                        iCur.insertRow(
                            [mapunits.pop(mk), mk, sum(pcts), ', '.join(props)] #prop_str, domain_d[prop_str]
                        )
                # Populate 0% present
                for mk, asym in mapunits.items():
                    iCur.insertRow([asym, mk, 0, ''])

        else:
            if ag_method == "Maximum":
                m_func = max
            else:
                m_func = min
            with (
                arcpy.da.SearchCursor(**tabs_d[comp_call]) as sCur,
                arcpy.da.InsertCursor(**tabs_d['property']) as iCur
            ):
                if comp_ag_d:
                    for mk, comps in groupby(sCur, iget(0)):
                        # find maximum WTA depth layer
                        prop_d = {}
                        for mk, ck, pct in comps:
                            # Get property data for cokey and prop not null
                            if(prop_i := comp_ag_d.get(ck)) is not None:             
                               if not isnan(prop_i[0]):
                                    # sum compositions by unique property values
                                    
                                    if prop_d.get(prop_i[0]):
                                        prop_d[prop_i[0]] += pct
                                    else:
                                        prop_d[prop_i[0]] = pct
                        if prop_d:
                            p_sel = m_func(prop_d.keys())
                            iCur.insertRow(
                                [mapunits.pop(mk), mk, prop_d[p_sel], 
                                 round(p_sel, p)]
                            )
                else:
                    for mk, comps in groupby(sCur, iget(0)):
                        prop_d = {}
                        for mk, ck, pct, prop_i in comps:
                            if prop_i in prop_d:
                                prop_d[prop_i] += pct
                            else:
                                prop_d[prop_i] = pct
                        if prop_d:
                            p_sel = m_func(prop_d.keys())
                        iCur.insertRow(
                            [mapunits.pop(mk), mk, prop_d[p_sel], 
                             round(p_sel, p)]
                        )
                # Populate Null
                for mk, asym in mapunits.items():
                    iCur.insertRow([asym, mk, None, None])

        return True
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def comp_it(p_pct):
    # Could create a variant that treats Null as 0
    _, _, pct, prop = p_pct
    return prop * pct, pct


def comp_wtavg(
        comps: Iterator[list[ Key, Key, int, Numeric],],
    ) -> tuple[float, int]:
    """Weighted average

    Parameters
    ----------
    comps : Iterator[list[Key, int, Numeric],]
        These are the elements packaged by map unit from the groupby interator 
        from the component table. For each component the following elements
        are sent:
        0) Key: map unit key (mukey)
        1) Key: component key (cokey)
        2) Component percentage
        3) Cotinuous attribute


    Returns
    -------
    bool : tuple[float, int]
        Aggregated by map unit: 
            0) sum of component percentages
            1) weighted average of the soil component property
        Will return an empty tuple if unsuccessful.
    """
    try:
        # comp_it unpacks and returns weighted property and pct
        # zip puts all wgted properties and pcts in respecitve lists
        # which are each summed
        prop_sum, comp_pct_sum = map(sum, zip(*map(comp_it, comps)))

        return (comp_pct_sum, prop_sum / comp_pct_sum)

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
    

def comp_con(
        comps: Iterator[list[ Key, Key, int, int, str],], 
        k: int, vi: int, null: Numeric, 
    ) -> list[int, str]:
    """_summary_

    Parameters
    ----------
    comps : Iterator[list[Key, Key, int, int, str],]
        These are the elements packaged by map unit from the groupby interator 
        from the component table. For each component the following elements
        are sent:
        0) Key: map unit key (mukey)
        1) Key: component key (cokey)
        2) Component percentage
        3) Interp rating value or domain sequence of class
        4) Interp rating class or property class
    k : int
        The position class elements are being sorted and grouped by
    vi : int
        Index position, either 0 (min) or -1 (max). 
        Really only relavent when there is a tie where two or more
        class components % sums to the most dominant condition.

    Returns
    -------
    list[int, str]
        1) Summed percentage of the dominant condition
        2) property  or interp class (str) 
    """
    try:
        v_class = dict()
        # sort and group components by class
        for cls, p_grp in groupby(sorted(comps, key=iget(k)), key=iget(k)):
            # sum percentage by class
            pcts = 0
            rat_seq = 0
            c_count = 0
            for prop_p in p_grp:
                pcts += prop_p[2]
                c_count += 1
                if prop_p[3] != None:
                    rat_seq += prop_p[3]
                else:
                    rat_seq = None
            # The class value of any interp class will do, class is the same
            # [sequence/interp val, class, summed sequence/interp]
            v_cl = prop_p[3:] + [rat_seq, c_count]
            # replace None with null value or max/min will error out
            if v_cl[0] == None:
                v_cl[0] = null
            if pcts in v_class:
                v_class[pcts].add(v_cl)
            # Keep a sorted list of all property sets of the same % composition
            else:
                v_class[pcts] = SortedList([v_cl])

        # get max % composition
        max_pct = max(v_class.keys())
        # get minimum max_pct and return class
        v_sel = v_class[max_pct][vi]
        if v_sel[2] is None:
            return max_pct, v_sel[1], None
        return max_pct, v_sel[1], v_sel[2] / v_sel[3]
        # return max_pct, v_class[max_pct][vi][-1]

    except:
        arcpy.AddError(f"comps: {list(comps)}")
        # arcpy.AddError(f"{prop_p} | {v_sel}")
        func = sys._getframe().f_code.co_name
        msg = pyErr(func)
        arcpy.AddError(msg)


def domain_it(
        comps: Iterator[list[ Key, Key, int, str],],
        domain_d
    ) -> list[Key, Key, int, str, int]:
    """Inserts the domain sequence into the comps package to direct tie breaks
    in Dominant Condition aggregation

    Parameters
    ----------
    comps : Iterator[list[Key, Key, int, Any],]
        These are the elements packaged by map unit from the groupby interator 
        from the component table. For each component the following elements
        are sent:
        0) Key: map unit key (mukey)
        1) Key: component key (cokey)
        2) Component percentage
        3) nominal property class

    Returns
    -------
    list[Key, Key, int, int, str]
        0) Key: map unit key (mukey)
        1) Key: component key (cokey)
        2) Component percentage
        3) Domain sequence index 
        4) nominal property class
    """
    try:
        comps2 = []
        for comp in comps:
            comp = list(comp)
            choice = comp[3]
            seq = domain_d.get(choice)
            # occasionally some choices are not in mdstatdomdet
            if not seq:
                seq = domain_d['z_max']
                domain_d[choice] = seq
                domain_d['z_max'] += 1
                arcpy.AddWarning(
                    f"{choice} mising from domain list, which could impact ties"
                )
            comp.insert(3, seq)
            comps2.append(comp)
        return comps2
    except:
        arcpy.AddError(f"{comps=} | {domain_d=}")
        func = sys._getframe().f_code.co_name
        msg = pyErr(func)
        arcpy.AddError(msg)


def main(args):
    try:
        feat = args[0] # SSURGO Feature to join to
        gdb_p = args[1] # SSURGO database
        table = args[2] # SSURGO table summarized attribute sourced from
        att_col = args[3] # The table column being summarized
        agg_meth = args[4] # Aggregation method
        prim_constraint = args[5] # criteria
        sec_table = args[6]
        sec_att = args[7]
        sec_constraint = args[8] # criteria
        d_ranges = args[9]
        month1 = args[10]
        tie_break = args[11]
        null0_b = args[12]
        comp_cut = args[13]
        fuzzy_b = args[14]
        null_rat_b = args[15]
        # custom: [Column Physical Name, Logical data type, Unit of measure]
        # sdv: all SDV Attribute fields
        sdv_dict = args[16] # SDV row as dictionary
        major_b = args[17] # args[17] # Only consider major components
        custom_b = args[18]
        primNOT = args[19]
        secNOT = args[20]
        abs_mm_b = args[21]
        module_p = args[22]
        

        arcpy.AddMessage(f"Summarize Soil Information {v=}")
        arcpy.env.workspace = gdb_p
        arcpy.env.overwriteOutput = True

        in_table = 'in_table'
        field_names = 'field_names'
        where_clause = 'where_clause'
        sql_clause = 'sql_clause'

        # Aggregation Algorithms and acronymns 
        agg_d = {
            "Dominant Condition": 'DCD', "Dominant Component": 'DCP', 
            "Minimum": 'MIN', "Maximum": 'MAX', "Weighted Average": 'WTA', 
            "Percent Present": 'PP', 'Least Limiting': 'LL', 
            'Most Limiting': 'ML'
        }

        # Create domain dictionary
        # if agg_meth == "Dominant Condition" and table != 'cointerp':
        try:
            db_p = f"{gdb_p}/mdstattabcols"
            with (arcpy.da.SearchCursor(
                db_p, 'domainname', where_clause=f"colphyname = '{att_col}'"
            ) as sCur):
                dom_n = next(sCur)[0]

            db_p = f"{gdb_p}/mdstatdomdet"
            with (arcpy.da.SearchCursor(
                db_p, ['choice', 'choicesequence'], 
                where_clause= f"domainname = '{dom_n}'"
            ) as sCur):
                domain_d = dict(sCur)
            domain_d['z_max'] = 10000
        except:
            domain_d = None

        above_comp = {'mapunit', 'muaggatt'}
        if table.startswith('ch'):
            lev1 = 'horizon'
        elif table == 'cointerp':
            lev1 = 'interp'
        elif table in above_comp:
            lev1 = 'mapunit'
        else:
            lev1 = 'component'

        if not sec_table or table == sec_table:
            lev2 = None
        elif sec_table.startswith('ch'):
            lev2 = 'horizon'
        else:
            lev2 = 'component'
        
        # Output table name and columns
        if d_ranges:
            if len(d_ranges) == 1:
                dmin = d_ranges[0][0]
                dmax = d_ranges[0][1]
                d_cat = f"{dmin}to{dmax}"
            else:
                d_cat = "multi"
                tops, bots = zip(*d_ranges)
                dmin = min(tops)
                dmax = max(bots)
        else:
            d_ranges = None
            d_cat = ''
        if not custom_b:
            tab_n = f"{sdv_dict['resultcolumnname']}_{agg_d[agg_meth]}_{d_cat}"
            col_n = tab_n
            if sdv_dict['attributelogicaldatatype'] == 'Integer':
                prop_dtype0 = 'LONG # #'
                prec = 0
            elif sdv_dict['attributelogicaldatatype'] == 'Float':
                prop_dtype0 = 'FLOAT # #'
                prec = sdv_dict['attributeprecision']
            else:
                prop_dtype0 = (
                    f"TEXT # {sdv_dict.get('attributefieldsize') or 254}"
                )
                prec = None
        else:
            if lev1 == 'interp':
                # Look up interp name in SDV Attribute: 
                    # NASIS Rule Name (nasisrulename)
                # get Result Column Name (resultcolumnname)
                q = f"nasisrulename = '{att_col}'"
                q = q.replace("''", "'")
                #arcpy.AddMessage(q)
                db_p = f"{gdb_p}/sdvattribute"
                with (arcpy.da.SearchCursor(
                    db_p, "resultcolumnname", where_clause=q
                ) as sCur):
                    col_stub = next(sCur)
                if col_stub:
                    col_n = f"{col_stub[0]}_{agg_d[agg_meth]}"
                    tab_n = f"ag_{col_stub[0]}_{agg_d[agg_meth]}"
                else:
                    att_col2 = att_col[att_col.index('-') + 2:]
                    # find all capitalized words
                    pattern = r"\b[A-Z]\w*\b"
                    caps = re.findall(pattern, att_col2)
                    # pattern to remove vowels
                    tt = str.maketrans("","","aeiouyAEIOUY")
                    trunk = ''.join(
                        [w[:2] + w[2:].translate(tt)[0] + w.translate(tt)[-1]  
                        if len(w)>4 else w for w in caps]
                    )
                    tab_n = f"ag_{trunk}_{agg_d[agg_meth]}"
                    col_n = f"{trunk}_{agg_d[agg_meth]}"
                prop_dtype0 = 'TEXT # 254'
                prop_dtype1 = 'Float # #'
                prec = 2
            else:
                # if integer or float make _class field TEXT of length 1
                # if TEXT make _val field short integer
                # only 2 SDV table columns both integer and have a domain
                tab_n = f"ag_{table}_{att_col}_{agg_d[agg_meth]}_{d_cat}"
                col_n = f"{att_col}_{agg_d[agg_meth]}_{d_cat}"
                if sdv_dict[1] == 'Integer':
                    prop_dtype = 'Numeric'
                    prop_dtype1 = 'LONG # #'
                    prec = 0
                elif sdv_dict[1] == 'Float':
                    prop_dtype = 'Numeric'
                    prop_dtype1 = 'FLOAT # #'
                    prec = sdv_dict[4]
                else:
                    prop_dtype = 'Text'
                    prop_dtype0 = f'TEXT # {sdv_dict[3] or 254}'
                    prop_dtype1 = 'SHORT # #'
                    prec = None

        tab_n = tab_n.strip('_')#.replace(' ','')
        col_n = col_n.strip('_')

        v_tab = arcpy.ListTables('version')
        if v_tab:
            gssurgo_v = getVersion({
                'version': {
                    in_table: gdb_p + '/version',
                    field_names: ['version'], 
                    where_clause:"name = 'gSSURGO'" 
                }})
        else:
            gssurgo_v = '1.0'
        if gssurgo_v == '2.0':
            mk_dtype = "LONG # #"
            q = ""
            delim = ", "
        else:
            mk_dtype = "TEXT # 30"
            q = "'"
            delim = "', '"
            # Read in interp keys
            # read in rule keys
        interpk = None
        rulek = None
        
        if arcpy.ListTables(f"{gdb_p}/{tab_n}"):
            arcpy.management.Delete(f"{gdb_p}/{tab_n}")
        arcpy.management.CreateTable(gdb_p, tab_n)
        if lev1 == "interp": #fuzzy_b and agg_meth in ('Least Limiting', 'Most Limiting'): #if 
            arcpy.management.AddFields(
            in_table=f"{gdb_p}/{tab_n}",
            field_description=(
                f"AREASYMBOL TEXT # 20 # #;"
                f"MUKEY {mk_dtype} # #;"
                f"COMPPCT_R SHORT # # # #;"
                f"class_{col_n} {prop_dtype0} # #;"
                f"val_{col_n} {prop_dtype1} # #"),
            template=None
            )
            fields = [
                'AREASYMBOL', 'MUKEY', 'COMPPCT_R', 
                'class_' + col_n, 'val_' + col_n
            ]
        elif prop_dtype == 'Text':
            arcpy.management.AddFields(
            in_table=f"{gdb_p}/{tab_n}",
            field_description=(
                f"AREASYMBOL TEXT # 20 # #;"
                f"MUKEY {mk_dtype} # #;"
                f"COMPPCT_R SHORT # # # #;"
                f"prop_{col_n} {prop_dtype0} # #;"
                f"seq_{col_n} {prop_dtype1} # #"),
            template=None
            )
            fields = [
                'AREASYMBOL', 'MUKEY', 'COMPPCT_R', 
                'prop_' + col_n, 'seq_' + col_n
            ]
        # Numeric
        else:
            arcpy.management.AddFields(
                in_table=f"{gdb_p}/{tab_n}",
                field_description=(
                    f"AREASYMBOL TEXT # 20 # #;"
                    f"MUKEY {mk_dtype} # #;"
                    f"COMPPCT_R SHORT # # # #;"
                    f"prop_{col_n} {prop_dtype1} # #"),
                template=None
            )
            fields = ['AREASYMBOL', 'MUKEY', 'COMPPCT_R', 'prop_' + col_n]

        # Dynamically define SQL where statement
        if primNOT:
            primNOT = 'NOT '
        else:
            primNOT = ''
        if secNOT:
            secNOT = 'NOT '
        else:
            secNOT = ''

        if prim_constraint:
            atts = prim_constraint.split(';')
            prim_str = f"""IN {primNOT} ({", ".join(atts)})"""
        if sec_constraint:
            atts = sec_constraint.split(';')
            sec_str = f"""IN {secNOT} ({", ".join(atts)})"""

        # -- Secondary aggregation levels
        # Acquire cokeys of components that satisfy constraint
        # Secondary levels are always component level, 
        # exception being chorizon property can be surmized by a chorizon cat
        if sec_table and sec_table != 'component' and sec_table != table:
            with arcpy.da.SearchCursor(
                f"{gdb_p}/{sec_table}", 'cokey', where_clause=sec_str) as sCur:
                cks = [ck for ck, in sCur]
            ck_str = f"""cokey IN ({q}{delim.join(cks)}{q})"""
        else:
            ck_str = ''

        # Used when component is the primary table
        comp_where1 = f"comppct_r IS NOT NULL AND {att_col} IS NOT NULL"
        # Otherwise
        comp_where2 = "comppct_r IS NOT NULL"
        if comp_cut:
            comp_where1 += f" AND comppct_r >= {comp_cut}"
            comp_where2 += f" AND comppct_r >= {comp_cut}"
        if major_b:
            comp_where1 += " AND majcompflag = 'Yes'"
            comp_where2 += " AND majcompflag = 'Yes'"
        if prim_constraint and table == 'component':
            comp_where1 += f" AND {att_col} {prim_str}"
        if sec_table == 'component':
            comp_where1 += f" AND {sec_att} {sec_str}"
            comp_where2 += f" AND {sec_att} {sec_str}"
        elif sec_table:
            comp_where1 += f" AND {ck_str}"
            comp_where2 += f" AND {ck_str}"

        # When chorizon is primary table
        if d_ranges:
            ch_where = f" hzdepb_r >= {dmin} AND hzdept_r <= {dmax}"
        else:
            ch_where = "hzdept_r IS NOT NULL AND hzdepb_r IS NOT NULL"
            # Shouldn't be anything deeper...
            d_ranges = [[0, 10000],]
        if table == 'chorizon':
            ch_where += f" AND {att_col} IS NOT NULL"
        if prim_constraint and table == 'chorizon':
            ch_where += f" AND {att_col} {prim_str}"
        if sec_table == 'chorizon':
            ch_where += f" AND {sec_att} {sec_str}"
        elif sec_table and ck_str:
            ch_where += f" AND {ck_str}"
        
        #notnull = str.maketrans("", "", f"{att_col} IS NOT NULL AND ")
        
        tabs_d = {
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
            'chorizon1': {
                in_table: gdb_p + '/chorizon',
                field_names: 
                ['cokey', 'hzdept_r', 'hzdepb_r', att_col],
                where_clause: ch_where,
                sql_clause: [None, "ORDER BY cokey, hzdept_r ASC"]
            },
            # cateogorical properties
            'chorizon1c': {
                in_table: gdb_p + '/chorizon',
                field_names: 
                ['cokey', 'hzdept_r', 'hzdepb_r', att_col],
                where_clause: ch_where, #.translate(notnull),
                sql_clause: [None, "ORDER BY cokey, hzdept_r ASC"]
            },
            # children of chorizon table
            'chorizon2': {
                in_table: gdb_p + '/chorizon',
                field_names: 
                ['cokey', 'chkey', 'hzdept_r', 'hzdepb_r'],
                where_clause: ch_where,
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
                    ""
                )
            },
            "cointerp1": {
                in_table: gdb_p + '/cointerp',
                field_names: ['cokey', 'interphrc', 'interphr'],
                where_clause: f"rulename = '{att_col}'"
            },
            # Exclude null ratings for LL and ML
            "cointerp1nn": {
                in_table: gdb_p + '/cointerp',
                field_names: ['cokey', 'interphrc', 'interphr'],
                where_clause: f"rulename = '{att_col}' AND interphr IS NOT NULL"
            },
            # gSSURGO > 1.0
            "cointerp2": {
                in_table: gdb_p + '/cointerp',
                field_names: ['cokey', 'interphrck', 'interphr'],
                where_clause: f"interpkey = {interpk} AND rulekey = {rulek}"
            },
            # Exclude null ratings for LL and ML
            "cointerp2nn": {
                in_table: gdb_p + '/cointerp',
                field_names: ['cokey', 'interphrck', 'interphr'],
                where_clause: (f"interpkey = {interpk} AND rulekey = {rulek}"
                               "AND interphr IS NOT NULL")
            },
            # summarizing a component property
            'comp1': {
                in_table: gdb_p + '/component',
                field_names: ['mukey', 'cokey', 'comppct_r', att_col],
                where_clause: comp_where1,
                sql_clause: [None, "ORDER BY mukey ASC"]
            },
            # summarizing a property below component
            'comp2': {
                in_table: gdb_p + '/component',
                field_names: ['mukey', 'cokey', 'comppct_r'],
                where_clause: comp_where2,
                sql_clause: [None, "ORDER BY mukey ASC"]
            },
            # summarizing a component property for Dominant Component
            'comp3': {
                in_table: gdb_p + '/component',
                field_names: ['mukey', 'cokey', 'comppct_r', att_col],
                where_clause: comp_where1,
                sql_clause: [None, "ORDER BY mukey ASC"]
            },
            'Dominant1': {
                in_table: gdb_p + '/DominantComponent',
                field_names: ['cokey'],
            },
            "Dominant2": {
                'in_table': gdb_p + '/DominantComponent',
                'field_names': ['mukey', 'cokey', 'comppct_r']
            },
            'legend': {
                in_table: gdb_p + '/legend',
                field_names: ['lkey', 'areasymbol']
            },
            'mapunit1': {
                in_table: gdb_p + '/mapunit',
                field_names: ['mukey', 'lkey']
            },
            'mapunit2': {
                in_table: gdb_p + '/mapunit',
                field_names: ['mukey', 'lkey', att_col]
            },
            'mdruleclass': {
                in_table: gdb_p + '/mdruleclass',
                field_names: ['classkey', 'classtxt']
            },
            'property':{
                in_table: f"{gdb_p}/{tab_n}",
                field_names: fields
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
        ### Update metadata

        # Activate if proton activity mean is desired
        # if 'ph1to1h2o' in att_col or 'ph01mcacl2' in att_col:
        #     pH = True
        # else:
        #     pH = False

        # -- Get areasymbol  
        with arcpy.da.SearchCursor(**tabs_d['legend']) as sCur:
            # Legend key: Areasymbol
            legends = dict(sCur)

        # -- Map unit
        if lev1 == 'mapunit':
            # get mapunit info and call table_write
            with arcpy.da.SearchCursor(**tabs_d['mapunit2']) as sCur:
                mapunits = {mk: (legends.get(lk), att) for mk, lk, att in sCur}
        else:
            # Otherwise get generic mapunit info
            with arcpy.da.SearchCursor(**tabs_d['mapunit1']) as sCur:
                mapunits = {mk: legends.get(lk) for mk, lk in sCur}

        # -- Component Interpretation
        if lev1 == 'interp':
            done = interp_node(
                agg_meth, mapunits, tabs_d, gssurgo_v, gdb_p,
                module_p, tie_break, fuzzy_b, att_col
            )

        else:
            # -- Component Crop Yield
            if table == 'cocropyld':
                with arcpy.da.SearchCursor(
                    gdb_p + "/cocropyld", att_col,
                    where_clause=(f"cropname = '{prim_constraint} "
                                f"AND yldunits = {sec_constraint}")
                    ) as sCur:
                    comp_ag_d = {ck: prop for ck, prop in sCur}

            # -- Horizon lev1 aggregation
            elif lev1 == 'horizon':
                # Call horizon aggregation
                if prop_dtype == 'Numeric':
                    with arcpy.da.SearchCursor(**tabs_d['chorizon1']) as sCur:
                        if agg_meth == "Percent Present":
                            comp_ag_d = dict()
                            for ck, _, _, prop in sCur:
                                if ck in comp_ag_d:
                                    comp_ag_d[ck].append(prop)
                                else:
                                    comp_ag_d[ck] = [prop,]
                        elif abs_mm_b:
                            if agg_meth == 'Maximum':
                                mORm = max
                            else:
                                mORm = min
                            arcpy.AddMessage(
                                f'Finding horizon with absolute {mORm.__name__}'
                            )
                            comp_ag_d = {
                                ck: horzAbs(d_ranges, h, mORm) for ck, h  in
                                groupby(sCur, iget(0))
                            }
                        else:
                            comp_ag_d = {
                                ck: horzAg(d_ranges, h) for ck, h  in # ,pH
                                groupby(sCur, iget(0))
                            }
                else:
                    with arcpy.da.SearchCursor(**tabs_d['chorizon1c']) as sCur:
                        comp_ag_d = {
                            ck: horzModal(d_ranges, h) for ck, h  in 
                            groupby(sCur, iget(0))
                        }
            # -- A component table property
            elif table == 'component':
                comp_ag_d = None
            #arcpy.AddMessage(f"{comp_ag_d.keys()}")

            done = comp_node(
                agg_meth, mapunits, tabs_d, gssurgo_v, gdb_p,
                module_p, tie_break, prec, q, delim, comp_ag_d, domain_d #, pH
            )

            arcpy.management.AddIndex(
                f"{gdb_p}/{tab_n}", 'MUKEY', tab_n + '_key', True
            )
            # arcpy.management.AddIndex(f"{gdb_p}/{tab_n}", col_n, tab_n, False)
            
        if done:
            # Return name of new table
            return (tab_n, col_n)
        else:
            return None

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return None
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return None


if __name__ == '__main__':
    main(sys.argv[1:])