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
from operator import itemgetter as iget
import sys
import os
from typing import Iterator, TypeVar, Union
from sortedcontainers import SortedList

from numpy import isnan

import arcpy

from .. import pyErr
from .. import arcpyErr
from .agg_horizon import hor2comp


Numeric = Union[int, float]
Key = TypeVar("Key", int, str)


def dom_com(d_cursor_args, gs_v, gdb_p, module_p):
    try:
        if not arcpy.Exists(f"{gdb_p}/DominantComponent"):
            # Create Dominant Component table is it doesn't exist
            arcpy.AddMessage('Creating Dominant Component table')
            with arcpy.da.SearchCursor(**d_cursor_args['comp2']) as sCur:
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
            with arcpy.da.InsertCursor(**d_cursor_args['Dominant2']) as iCur:
                for mk, (pct, ck) in dom_com_d.items():
                    iCur.insertRow([mk, ck, pct])
        # return component keys of dominant compoents
        with arcpy.da.SearchCursor(**d_cursor_args['Dominant1']) as sCur:
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


def comp_node(
        ag_method: str, mapunits: dict, d_cursor_args: dict, gssurgo_v: str,
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
            d_cursor_args[comp_call]['where_clause'] += " AND majcompflag = 'Yes'"

            ####### This needs to use Dominant Comp table #########
            cokeys = dom_com(d_cursor_args, gssurgo_v, gdb_p, module_p)

            d_cursor_args[comp_call]['where_clause'] += \
                f""" AND cokey IN ({q}{delim.join(cokeys)}{q})"""
            with (
                    arcpy.da.InsertCursor(**d_cursor_args['property']) as iCur,
                    arcpy.da.SearchCursor(**d_cursor_args[comp_call]) as sCur,
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
                arcpy.da.SearchCursor(**d_cursor_args[comp_call]) as sCur,
                arcpy.da.InsertCursor(**d_cursor_args['property']) as iCur
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
                            if (hor := comp_ag_d.get(ck)) is not None 
                        ]
                        if comps_p:
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
                arcpy.da.SearchCursor(**d_cursor_args[comp_call]) as sCur,
                arcpy.da.InsertCursor(**d_cursor_args['property']) as iCur
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
                        pct, prop, seq = comp_con(comps_p2, 3, vi, None)
                        iCur.insertRow([mapunits.pop(mk), mk, pct, prop, seq])
                # Populate Null
                for mk, asym in mapunits.items():
                    iCur.insertRow([asym, mk, None, None, None])

        elif ag_method == "Percent Present":
            # This method requires a selection of a primary constraint
            # remove seq column
            del_fld = d_cursor_args['property']['field_names'].pop()
            arcpy.management.DeleteField(
                d_cursor_args['property']['in_table'], del_fld
            )
            with (
                arcpy.da.SearchCursor(**d_cursor_args[comp_call]) as sCur,
                arcpy.da.InsertCursor(**d_cursor_args['property']) as iCur
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
                arcpy.da.SearchCursor(**d_cursor_args[comp_call]) as sCur,
                arcpy.da.InsertCursor(**d_cursor_args['property']) as iCur
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
        # arcpy.AddError(f"{comps_p}")
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