#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 03/20/2026
    @by: Alexnder Stum
@Version: 0.1
"""




from itertools import groupby
from math import log10
from operator import itemgetter as iget
import sys
from typing import Any, Callable, Generic, Iterator, TypeVar, Union

import numpy as np
from numpy import isnan
import arcpy

from .. import pyErr
from .. import arcpyErr

Numeric = Union[int, float]
Key = TypeVar("Key", int, str)
Shape = TypeVar("Shape", tuple, list)
DType = TypeVar("DType")


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


def horizon_main(prop_dtype, d_cursor_args, agg_meth, abs_mm_b, d_ranges):
    try:
        # Call horizon aggregation
        if prop_dtype == 'Numeric':
            with arcpy.da.SearchCursor(
                **d_cursor_args['chorizon1']
            ) as sCur:
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
            with arcpy.da.SearchCursor(
                **d_cursor_args['chorizon1c']
            ) as sCur:
                comp_ag_d = {
                    ck: horzModal(d_ranges, h) for ck, h  in 
                    groupby(sCur, iget(0))
                }
        return comp_ag_d

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        raise
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        raise