#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 03/26/2026
    @by: Alexnder Stum
@Version: 0.1


"""
import calendar
from itertools import groupby
from operator import itemgetter as iget
import sys

import numpy as np

import arcpy

from .agg_component import dom_com
from .. import pyErr
from .. import arcpyErr


def med_u(freq_a):
    sum_a = np.add.accumulate(freq_a)
    return int(np.argmax(sum_a > sum_a[-1] / 2)) + 1


def med_l(freq_a):
    sum_a = np.add.accumulate(freq_a)
    return int(np.argmax(sum_a >= sum_a[-1] / 2)) + 1


def dom_l(freq_a):
    return int(np.argmax(freq_a)) + 1


def dom_u(freq_a):
    return int(freq_a.size - np.argmax(freq_a[::-1]) - 1) + 1


def make_freq_a1(comp, N, nm, domain_d):
    try:
        # frequency array
            freq_a = np.zeros([N])
            # set of months by number
            seq_s = set()
            for mc in comp:
                # frequency class
                cl = mc[1]
                # month number
                seq = mc[2]
                # Search for components with redundant months
                if seq not in seq_s:
                    # Tally frequency to freqency array
                    freq_a[domain_d[cl]] += 1
                    seq_s.add(seq)
                else:
                    freq_a[0] = -1
                    return freq_a

            # Account for missing month records
            if freq_a[0] >= 0:
                freq_a[0]  += nm - freq_a.sum()
            # Collect frequency arrays by cokey
            return freq_a
            
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return None
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return None


def med_dom(sCur1, sCur2, domain_d, tie_break, ag_method, mapunits, tab_p, fields, nm):
    """ Handles Dominant Condition, Median Frequency"""
    try:
        N = len(domain_d)
        invert_d = {v: k for k, v in domain_d.items()}
        # com_d = {}
        # would map be faster?
        com_d = {ck: make_freq_a1(comp, N, nm, domain_d)
                 for ck, comp in groupby(sCur1, iget(0))}

        if ag_method == 'Dominant Condition':
            if tie_break == 'Higher':
                med_f = dom_u
            else:
                med_f = dom_l
        else:
            if tie_break == 'Higher':
                med_f = med_u
            else:
                med_f = med_l

        # groupby mukey and insert
        iCur = arcpy.da.InsertCursor(tab_p, fields)
        # for each map unit
        # arcpy.AddMessage(f"{sCur2.fields}")
        for mk, comps in groupby(sCur2, iget(0)):
            # column 0: weigthed mensual occurence, 
            # column 1: sum of componet %'s that have at least 
                # one mensual occurence
            freq_mast_a = np.zeros([N, 2])
            perc_sum = 0
            # for each component
            for comp in comps:
                ck = comp[1]
                perc = comp[2]
                perc_sum += perc
                freq_a = com_d.get(ck)
                # component has no comonth records
                if freq_a is None:
                    continue
                # if a month repeated for a component 
                if freq_a[0] < 0:
                    row_suff = [None, "Undetermined", None]
                    iCur.insertRow([mapunits[mk], mk] + row_suff)
                    break
                freq_mast_a[:, 0] +=  freq_a * perc
                # index of mensual frequencies found for component
                # idx = np.where(freq_a)[0]
                # freq_mast_a[idx, 1] += perc
                freq_mast_a[:, 1] += freq_a.astype(bool) * perc 
            else:
                # no components had any records
                if not freq_mast_a.any():
                    row_suff = [None, "No Records", None]
                # Check that Null isn't super majority
                # freq_mast_a[0, 0] > (6 * perc_sum)
                wgt_sum = freq_mast_a[:, 0].sum()
                if freq_mast_a[0, 1] > (wgt_sum / 2):
                    row_suff = [freq_mast_a[0, 1], "Majority Null", None]
                else:
                    di = med_f(freq_mast_a[1:, 0])
                    med_class = invert_d[di]
                    comppct = freq_mast_a[di, 1]
                    row_suff = [comppct, med_class, di]
                # arcpy.AddMessage(f"{[mapunits[mk], mk] + row_suff}")
                # arcpy.AddMessage(f"{iCur.fields}")

                iCur.insertRow([mapunits[mk], mk] + row_suff)

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        arcpy.AddError(f"{freq_mast_a=}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def min_f(freq_mast_a, domain_d):
    """comppct, freq class, seq, months"""
    try:
        month_a = np.array(calendar.month_abbr)[1:]
        invert_d = {v: k for k, v in domain_d.items()}
        index = np.where(freq_mast_a[1:, :-1].sum(axis=1))[0]
        if index.size:
            # index of the lowest frequency class with a mensual occurence
            fi = index[0] + 1
            # months of min freq
            month_i = np.where(freq_mast_a[fi, :-1])[0]
            month_str = ', '. join(month_a[month_i])
            comppct = freq_mast_a[fi, -1]
            class_str = invert_d[fi]
        else:
            comppct = None
            class_str = None
            fi = None
            month_str = ''
        
        return [comppct, class_str, fi, month_str]

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        # arcpy.AddError(f"{comps_p}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def max_f(freq_mast_a, domain_d):
    """comppct, freq class, seq, months"""
    try:
        month_a = np.array(calendar.month_abbr)[1:]
        invert_d = {v: k for k, v in domain_d.items()}
        index = np.where(freq_mast_a[1:, :-1].sum(axis=1))[0]
        if index.size:
            # index of the highest frequency class with a mensual occurence
            fi = index[-1] + 1
            # months of max freq
            month_i = np.where(freq_mast_a[fi, :-1])[0]
            month_str = ', '. join(month_a[month_i])
            comppct = freq_mast_a[fi, -1]
            class_str = invert_d[fi]
        else:
            arcpy.AddMessage(f"{freq_mast_a}")
            comppct = None
            class_str = None
            fi = None
            month_str = ''
        
        return [comppct, class_str, fi, month_str]

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        # arcpy.AddError(f"{comps_p}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def pp_f(freq_mast_a, domain_d):
    """comppct, freq classes"""
    try:
        invert_d = {v: k for k, v in domain_d.items()}
        index = np.where(freq_mast_a[1:, :-1].sum(axis=1))[0]
        if index.size:
            f_classes = [invert_d[i] for i in index]
            class_str = ', '.join(f_classes)
            pcts = freq_mast_a[:, -1].sum()
        else:
            class_str = ''
            pcts = 0
        
        return [pcts, class_str]

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        # arcpy.AddError(f"{comps_p}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def count_freq(freq_mast_a, domain_d):
    """comppct, freq classes, class, months"""
    try:
        month_a = np.array(calendar.month_abbr)[1:]
        invert_d = {v: k for k, v in domain_d.items()}
        # index of freq classes with a mensual occurence
        cl_index = np.where(freq_mast_a[1:, :-1].sum(axis=1))[0]
        if cl_index.size:
            f_classes = [invert_d[i] for i in cl_index]
            class_str = ', '.join(f_classes)
            # months of occurrence
            m_index = np.where(freq_mast_a[1:, :-1].sum(axis=0))[0]
            count = m_index.size
            month_str = ', '. join(month_a[m_index])
            comppct = freq_mast_a[:, -1].sum()
        else:
            comppct = 0
            count = 0
            month_str = ''
            class_str = ''
        
        return [comppct, class_str, count, month_str]

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        # arcpy.AddError(f"{comps_p}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def make_freq_a2(comp, N, domain_d):
    try:
         # frequency array
            freq_a = np.zeros([N, 12])

            for mc in comp:
                cl = mc[1]
                cl_seq = domain_d[cl]
                m_seq = mc[2] -1
                # Tally frequency to freqency array
                freq_a[cl_seq, m_seq] += 1
            # Collect frequency arrays by cokey
            return freq_a
            
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return None
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return None


def men_count(sCur1, sCur2, domain_d, ag_method, mapunits, tab_p, fields):
    """Handles Highest/Lowest Frequency, Frequency Count, Percent Present"""
    try:
        N = len(domain_d)
        # com_d = {}
        # compile a mensual frequency matrix for each component in comonth
        com_d = {ck: make_freq_a2(comp, N, domain_d)
                  for ck, comp in groupby(sCur1, iget(0))}

        if ag_method == 'Highest Frequency':
            count_f = max_f
            row_null = [None, None, None, None]
        elif ag_method == 'Lowest Frequency':
            count_f = min_f
            row_null = [None, None, None, None]
        elif ag_method == 'Percent Present':
            count_f = pp_f
            row_null = [0, '']
        else: # Frequency Count
            count_f = count_freq
            row_null = [0, '', 0, '']
            
        # groupby mukey and insert
        iCur = arcpy.da.InsertCursor(tab_p, fields)
        # for each map unit
        for mk, comps in groupby(sCur2, iget(0)):
            freq_mast_a = np.zeros([N, 13])
            # for each component
            for comp in comps:
                # cokey
                ck = comp[1]
                # component percent
                perc = comp[2]
                freq_a = com_d.get(ck)
                # component has no comonth records
                if freq_a is None:
                    continue
                if freq_a.any():
                    freq_mast_a[:, :-1] +=  freq_a
                    # index of mensual frequencies found for component
                    idx = np.where(freq_a)[0]
                    freq_mast_a[idx, -1] += perc
            
            if not freq_mast_a.any():
                iCur.insertRow([mapunits[mk], mk] + row_null)
            else:
                row_suffix = count_f(freq_mast_a, domain_d)
                iCur.insertRow([mapunits[mk], mk] + row_suffix)
    
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        # arcpy.AddError(f"{comps_p}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False
        

def comonth_node(
        gdb_p, mapunits, comptype, ag_meth, att_col, domain_d, prim_str, sec_str, 
        d_cursor_args, months, tie_break, module_p, gs_v, q, delim,
        tab_n, fields
    ):
    try:
        domain_d.pop('z_max')
        domain_d[None] = 0
        tab_p = f"{gdb_p}/{tab_n}"
        if comptype == 'Dominant Component':
            cokeys = dom_com(d_cursor_args, gs_v, gdb_p, module_p)
            cokey_q = f""" AND cokey IN ({q}{delim.join(cokeys)}{q})"""
            d_cursor_args['comp2']['where_clause'] += cokey_q
        else:
            cokey_q = ''
                
        # read comonth table
        if months:
            ms = months.split(';')
            nm = len(ms)
            ms = [m.strip("'") for m in ms]
            m_delim = "', '".join(ms)
            m_str = f"""month IN ('{m_delim}')"""

        # All 12 months
        else:
            nm = 12
            m_str = f"""month IN ('{"', '".join(list(calendar.month_name)[1:])}')"""
            
        where_clause = m_str
        if ag_meth in ("Frequency Count", "Percent Present"):
            # there must be a primary constraint and possibly a secondary
            where_clause += f" AND {att_col} {prim_str}"
            if sec_str:
                if 'pond' in att_col:
                    sec_col = 'ponddurcl'
                else:
                    sec_col = 'floddurcl'
                where_clause += f" AND {sec_col} {sec_str}"

        sCur1 = arcpy.da.SearchCursor(
            f"{gdb_p}/comonth", ['cokey', att_col, 'monthseq'], 
            where_clause=where_clause
        )
        # Get Component mukey, cokey, and pct
        with arcpy.da.SearchCursor(**d_cursor_args['comp2']) as sCur2:
            if ag_meth in ("Dominant Condition", "Median Frequency"):
                med_dom(
                    sCur1, sCur2, domain_d, tie_break, ag_meth, 
                    mapunits, tab_p, fields, nm
                )
            else:
                men_count(
                    sCur1, sCur2, domain_d, ag_meth, mapunits, tab_p, fields
                )

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
