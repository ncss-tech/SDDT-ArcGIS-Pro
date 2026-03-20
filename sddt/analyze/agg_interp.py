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

import arcpy

from .. import pyErr
from .. import arcpyErr
from .agg_component import dom_com, comp_wtavg, comp_con


def interp_node(
        ag_method: str, mapunits: dict, d_cursor_args: dict,
        gssurgo_v: str, gdb_p: str, module_p: str, tiebreak: str, 
        fuzzy: bool, interp_n: str
                ):
    try: 
        # Ignore not rated map units (null ratings)
        if(gssurgo_v == "1.0" and ag_method 
           in ('Most Limiting', 'Least Limiting', 'Weighted Average')
        ):
            # 'cokey', 'interphrc', 'interphr'
            with arcpy.da.SearchCursor(**d_cursor_args['cointerp1nn']) as sCur:
                # cokey: [interp class, interp value]
                cointerp_d = {ck: [cl, val] for ck, cl, val in sCur}
        elif gssurgo_v == "1.0":
            # 'cokey', 'interphrc', 'interphr'
            with arcpy.da.SearchCursor(**d_cursor_args['cointerp1']) as sCur:
                # cokey: [interp class, interp value]
                cointerp_d = {ck: [cl, val] for ck, cl, val in sCur}
        elif(
            ag_method in ('Most Limiting', 'Least Limiting', 'Weighted Average')
        ):
            # Read in mdruleclass table
            with arcpy.da.SearchCursor(**d_cursor_args['mdruleclass']) as sCur:
                mdrules = dict(sCur)
            # Read in cointerp table
            # 'cokey', 'interphrck', 'interphr'
            with arcpy.da.SearchCursor(**d_cursor_args['cointerp2nn']) as sCur:
                # cokey: [interp class, interp value]
                cointerp_d = {
                    ck: [mdrules[cls_k], val] for ck, cls_k, val in sCur
                }
        else:
            # Read in mdruleclass table
            with arcpy.da.SearchCursor(**d_cursor_args['mdruleclass']) as sCur:
                mdrules = dict(sCur)
            # Read in cointerp table
            # 'cokey', 'interphrck', 'interphr'
            with arcpy.da.SearchCursor(**d_cursor_args['cointerp2']) as sCur:
                # cokey: [interp class, interp value]
                cointerp_d = {
                    ck: [mdrules[cls_k], val] for ck, cls_k, val in sCur
                }

        if ag_method == 'Dominant Component':
            cokeys = dom_com(d_cursor_args, gssurgo_v, gdb_p, module_p)
            
            with (
            arcpy.da.InsertCursor(**d_cursor_args['property']) as iCur,
            arcpy.da.SearchCursor(**d_cursor_args['comp2']) as sCur,
            ):
                for mk, ck, pct in sCur:
                    if ck in cokeys and ck in cointerp_d:
                        cl_val = cointerp_d[ck] #cl = cointerp_d[ck][0]
                        iCur.insertRow([mapunits.get(mk), mk, pct] + cl_val) #+ cl
            return True

        if ag_method == 'Weighted Average':
            # remove class column
            del_fld = d_cursor_args['property']['field_names'].pop(3)
            arcpy.management.DeleteField(
                d_cursor_args['property']['in_table'], del_fld
            )
            # No interp class with Weighted Average
            with (
                arcpy.da.SearchCursor(**d_cursor_args['comp2']) as sCur,
                arcpy.da.InsertCursor(**d_cursor_args['property']) as iCur
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
                arcpy.da.SearchCursor(**d_cursor_args['comp2']) as sCur,
                arcpy.da.InsertCursor(**d_cursor_args['property']) as iCur
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
                arcpy.da.SearchCursor(**d_cursor_args['comp2']) as sCur,
                arcpy.da.InsertCursor(**d_cursor_args['property']) as iCur
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