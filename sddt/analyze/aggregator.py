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
@Version: 0.9


# --- Update 04/20/2026, v. 0.9
- Added flooding and ponding functionality
- pushed summary table construtions out to agg_commons as bld_table
# --- Update 04/07/2026, v. 0.8.2
- If primary or secondary constraints are inverted by user, 
query would error as 'NOT' in wrong place
# --- Update 04/06/2026, v. 0.8.1
- It was excluding minors, even when user specified All Components
# --- Update 03/26/2026; v 0.8
- Ammended errors related to summarizing categorical horizon attributes
- Revamped aggregator to include Which Components parameter, removed Absolute
Min/Max boolean parameter and the Major boolean paramter
- 
# --- Update 03/20/2026; v 0.7
- Fixed concatonation errors related to secondary constraints
- Modulated the component, interp, horizon, and other functions which are 
now called from agg_horizon, agg_component, agg_interp, agg_commons sub-modules
# --- Update 02/20/2026; v 0.6.2
- Filtering out cocropyld records that are Null, some crops only populated
for irrigated and not nonirrigated and v.v. 
- Fixed more Primary and Secondary constraint parsing errors
- Fixed bug with cocropyld to be compaitable with comp_ag_d format
# --- Update 02/05/2026; v 0.6.1
- Parsing Primary and Secondary constraints with single quotes.
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
v = '0.9'


import re
import sys

import arcpy

from .. import pyErr
from .. import arcpyErr

from .agg_horizon import horizon_main
from .agg_component import comp_node
from .agg_interp import interp_node
from .agg_month import comonth_node
from .agg_commons import cursor_args
from .agg_commons import getVersion
from .agg_commons import bld_table


def main(args):
    try:
        gdb_p = args[0] # SSURGO database
        table = args[1] # SSURGO table summarized attribute sourced from
        att_col = args[2] # The table column being summarized
        comptype = args[3]
        agg_meth = args[4] # Aggregation method
        prim_constraint = args[5] # criteria
        sec_table = args[6]
        sec_att = args[7]
        sec_constraint = args[8] # criteria
        d_ranges = args[9]
        months = args[10]
        tie_break = args[11]
        null0_b = args[12]
        comp_cut = args[13]
        fuzzy_b = args[14]
        null_rat_b = args[15]
        # custom: [Column Physical Name, Logical data type, Unit of measure]
        # sdv: all SDV Attribute fields
        sdv_dict = args[16] # SDV row as dictionary
        custom_b = args[17]
        primNOT = args[18]
        secNOT = args[19]
        module_p = args[20]
        
        arcpy.AddMessage(f"Summarize Soil Information {v=}")
        arcpy.env.workspace = gdb_p
        arcpy.env.overwriteOutput = True


        if not agg_meth and comptype == 'Dominant Component':
            agg_meth = 'Dominant Component'

        # Aggregation Algorithms and acronymns 
        agg_d = {
            "Dominant Condition": 'DCD', "Dominant Component": 'DCP', 
            "Minimum": 'MIN', "Maximum": 'MAX', "Weighted Average": 'WTA', 
            "Percent Present": 'PP', 'Least Limiting': 'LL', 
            'Most Limiting': 'ML', "Absolute Minimum": "AMIN", 
            "Absolute Maximum": "AMAX", "Median Frequency": "MFREQ", 
            "Highest Frequency": "HFREQ", "Lowest Frequency": "LFREQ",
            "Frequency Count": "FREQC"
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

        if table.startswith('ch'):
            lev1 = 'horizon'
        elif table == 'cointerp':
            lev1 = 'interp'
        else:
            lev1 = 'component'
        
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

        if comptype == "Dominant Component":
            agg_label = agg_d[comptype]
        else:
            agg_label = agg_d[agg_meth]

        if not custom_b:

            tab_n = f"{sdv_dict['resultcolumnname']}_{agg_label}_{d_cat}"
            col_n = tab_n
            if sdv_dict['attributelogicaldatatype'] == 'Integer':
                prop_dtype0 = ['LONG', '#', '#']
                prec = 0
            elif sdv_dict['attributelogicaldatatype'] == 'Float':
                prop_dtype0 = ['FLOAT', '#', '#']
                prec = sdv_dict['attributeprecision']
            else:
                prop_dtype0 = ['TEXT', '#', 
                               f"{sdv_dict.get('attributefieldsize') or 254}"
                ]
                prec = None
        else:
            if lev1 == 'interp':
                # Look up interp name in SDV Attribute: 
                    # NASIS Rule Name (nasisrulename)
                # get Result Column Name (resultcolumnname)
                q = f"nasisrulename = '{att_col}'"
                q = q.replace("''", "'")
                prop_dtype = ''
                db_p = f"{gdb_p}/sdvattribute"
                with (arcpy.da.SearchCursor(
                    db_p, "resultcolumnname", where_clause=q
                ) as sCur):
                    col_stub = next(sCur, None)
                if col_stub:
                    col_n = f"{col_stub[0]}_{agg_label}"
                    tab_n = f"ag_{col_stub[0]}_{agg_label}"
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
                    tab_n = f"ag_{trunk}_{agg_label}"
                    col_n = f"{trunk}_{agg_label}"
                prop_dtype0 = ['TEXT', '#', 254]
                prop_dtype1 = ['Float', '#', '#']
                prec = 2
            else:
                # if integer or float make _class field TEXT of length 1
                # if TEXT make _val field short integer
                # only 2 SDV table columns both integer and have a domain
                tab_n = f"ag_{table}_{att_col}_{agg_label}_{d_cat}"
                col_n = f"{att_col}_{agg_label}_{d_cat}"
                if sdv_dict[1] == 'Integer':
                    prop_dtype = 'Numeric'
                    prop_dtype1 = ['LONG', '#', '#']
                    prec = 0
                    prop_dtype0 = None
                elif sdv_dict[1] == 'Float':
                    prop_dtype = 'Numeric'
                    prop_dtype1 = ['FLOAT', '#', '#']
                    prec = sdv_dict[4]
                    prop_dtype0 = None
                else:
                    prop_dtype = 'Text'
                    prop_dtype0 = ['TEXT', '#', f'{sdv_dict[3] or 254}']
                    prop_dtype1 = ['SHORT', '#', '#']
                    prec = None

        tab_n = tab_n.strip('_')#.replace(' ','')
        col_n = col_n.strip('_')

        v_tab = arcpy.ListTables('version')
        if v_tab:
            gssurgo_v = getVersion({
                'version': {
                    "in_table": gdb_p + '/version',
                    "field_names": ['version'], 
                    "where_clause":"name = 'gSSURGO'" 
                }})
        else:
            gssurgo_v = '1.0'
        if gssurgo_v == '2.0':
            mk_dtype = ["LONG", '', '']
            q = ""
            delim = ", "
        else:
            mk_dtype = ["TEXT", '', 30]
            q = "'"
            delim = "', '"
            # Read in interp keys
            # read in rule keys
        interpk = None
        rulek = None
        
        sym_fld, fields = bld_table(
            gdb_p, tab_n, col_n, lev1, agg_meth,
            mk_dtype, prop_dtype, prop_dtype0, prop_dtype1
        )

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
            # sometimes they have single quotes already and get double bagged
            atts = [att.strip("'") for att in atts]
            atts_delim = "', '".join(atts)
            prim_str = f""" {primNOT} IN ('{atts_delim}')"""
        else:
            prim_str = ''
        if sec_constraint:
            atts = sec_constraint.split(';')
            # sometimes they have single quotes already and get double bagged
            atts = [att.strip("'") for att in atts]
            atts_delim = "', '".join(atts)
            sec_str = f"""{sec_att} {secNOT} IN ('{atts_delim}')"""
            # arcpy.AddMessage(f"{sec_str=}")
        else:
            sec_str = ''

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
        if comptype != "All Components":
            comp_where1 += " AND majcompflag = 'Yes'"
            comp_where2 += " AND majcompflag = 'Yes'"
        if prim_constraint and table == 'component':
            comp_where1 += f" AND {att_col} {prim_str}"
        if sec_table == 'component':
            comp_where1 += f" AND {sec_str}"
            comp_where2 += f" AND {sec_str}"

        # When chorizon is primary table
        if d_ranges:
            ch_where = f" hzdepb_r >= {dmin} AND hzdept_r <= {dmax}"
        else:
            ch_where = "hzdept_r IS NOT NULL AND hzdepb_r IS NOT NULL"
            # Shouldn't be anything deeper...
            d_ranges = [[0, 10000],]
        if table == 'chorizon' and agg_meth != 'Dominant Condition':
            ch_where += f" AND {att_col} IS NOT NULL"
        if prim_constraint and table == 'chorizon':
            ch_where += f" AND {att_col} {prim_str}"
        if sec_table == 'chorizon':
            ch_where += f" AND {sec_str}"
            # arcpy.AddMessage(f"{ch_where=}")
        elif sec_table and ck_str:
            ch_where += f" AND {ck_str}"
        
        d_cursor_args = cursor_args(
            gdb_p, att_col, ch_where, interpk, rulek, 
            comp_where1, comp_where2, tab_n, fields
        )
        
        ### Update metadata

        # Activate if proton activity mean is desired
        # if 'ph1to1h2o' in att_col or 'ph01mcacl2' in att_col:
        #     pH = True
        # else:
        #     pH = False

        # -- Get areasymbol  
        with arcpy.da.SearchCursor(**d_cursor_args['legend']) as sCur:
            # Legend key: Areasymbol
            legends = dict(sCur)

        # Get generic mapunit info
        with arcpy.da.SearchCursor(**d_cursor_args['mapunit1']) as sCur:
            mapunits = {mk: legends.get(lk) for mk, lk in sCur}

        # -- Component Interpretation
        if lev1 == 'interp':
            if comptype == "Dominant Component":
                agg_meth = "Dominant Component"
            done = interp_node(
                agg_meth, mapunits, d_cursor_args, gssurgo_v, gdb_p,
                module_p, tie_break, fuzzy_b, att_col
            )
        # --- Component Flooding and Ponding
        elif table == 'comonth':
            done = comonth_node(
                gdb_p, mapunits, comptype, agg_meth, att_col, domain_d, 
                prim_str, sec_str, d_cursor_args, months, tie_break, module_p, 
                gssurgo_v, q, delim, tab_n, fields
            )

        # -- Component Crop Yield
        else:
            if table == 'cocropyld':
                prim_constraint = prim_constraint.strip("'")
                sec_constraint = sec_constraint.strip("'")
                where_cl = (f"cropname = '{prim_constraint}' "
                                f"AND yldunits = '{sec_constraint}' "
                                f"AND {att_col} IS NOT NULL")
                with arcpy.da.SearchCursor(
                    gdb_p + "/cocropyld", 
                    ['cokey', att_col],
                    where_clause=where_cl
                ) as sCur:
                    comp_ag_d = {ck: [prop,] for ck, prop in sCur}
                if len(comp_ag_d) == 0:
                    comp_ag_d['Place holder'] = None
                    arcpy.AddMessage(
                        "No components found attributed with the "
                        "requested yield data"
                    )
            # -- Horizon lev1 aggregation
            elif lev1 == 'horizon':
               comp_ag_d = horizon_main(
                   prop_dtype, d_cursor_args, agg_meth, d_ranges
                )
               if len(comp_ag_d) == 0:
                    comp_ag_d['Place holder'] = None
                    arcpy.AddMessage(
                        "No components found attributed with the "
                        "requested Horizon data"
                    )
            # -- A component table property
            elif table == 'component':
                comp_ag_d = None

            agg_meth = agg_meth.removeprefix("Absolute ")
            if comptype == "Dominant Component":
                agg_meth = "Dominant Component"
            done = comp_node(
                agg_meth, mapunits, d_cursor_args, gssurgo_v, gdb_p,
                module_p, tie_break, prec, q, delim, comp_ag_d, domain_d #, pH
            )

            arcpy.management.AddIndex(
                f"{gdb_p}/{tab_n}", 'MUKEY', tab_n + '_key', True
            )
            
        if done:
            return (tab_n, sym_fld)
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