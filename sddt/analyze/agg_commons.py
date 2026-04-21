#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 04/20/2026
    @by: Alexnder Stum
@Version: 0.2

# --- Update 04/20/2026; v 0.2
- Added bld_table function
"""


import sys
from typing import Any

import arcpy

from .. import pyErr
from .. import arcpyErr


def getVersion(d_cursor_args) -> str:
    try:
        if arcpy.Exists('version'):
            with arcpy.da.SearchCursor(**d_cursor_args['version']) as sCur:
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


def cursor_args(
        gdb_p: str, att_col: str, ch_where: str, interpk: str, rulek: str, 
        comp_where1: str, comp_where2: str, tab_n: str, fields: list[str]
    ) -> dict[str, dict[str, Any]]:
    """Takes information to build cursor parameters

    Parameters
    ----------
    gdb_p : str
        Path of the gSSURGO File Geodatabase
    att_col : str
        Primary attribute column being summarized
    ch_where : str
        Query statemetn for component horizon
    interpk : str
        Only used for gSSURGO > 1.0. The keys of interps. Currently obsolete
    rulek : str
        Only used for gSSURGO > 1.0. The keys of interp ruules. 
        Currently obsolete
    comp_where1 : str
       Component where clause when the primary attribute is in the component
       table
    comp_where2 : str
        Component where clause when the primary attribute is not in the 
        component table
    tab_n : str
        Name of the output summary table 
    fields : list[str]
        Fields found in the output summary table

    Returns
    -------
    dict[str, dict[str, Any]]
        The first key refers to specific tables and use case scenarios. 
        Each sub-dictionary has two-four keys which correspond to arcpy
        cursor parameters:
        in_table: table path (required)
        field_names: list of fields (strings) to be called by cursor (required)
        where_clause: an sql clause that filters which records are found by 
            cursor (optional)
        sql_clause: an sql clauase that largely used to for ordering 
            records (optional)
    """
    in_table = 'in_table'
    field_names = 'field_names'
    where_clause = 'where_clause'
    sql_clause = 'sql_clause'

    d_cursor_args = {
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
    
    return d_cursor_args


def bld_table(
        gdb_p: str, tab_n: str, col_n: str, lev1: str, ag_meth: str,
        mk_dtype: list[str, str, Any], prop_dtype: str, 
        prop_dtype0: list[str, str, Any], prop_dtype1: list[str, str, Any]
    ) -> list[str]:
    """Function builds the output summary table using 
    arcpy.management.CreateTable and arcpy.management.AddFields

    Parameters
    ----------
    gdb_p : str
        Path of the gSSURGO File Geodatabase
    tab_n : str
        Name of the output summary table 
    col_n : str
        Base name of the primary summary field
    lev1 : str
        Indicates table level, only used to flag interps
    ag_meth : str
        Aggregation method
    mk_dtype : list[str, str, Any]
        Map unit key data type
    prop_dtype : str
        The data type of the primary atribute being summarized
    prop_dtype0 : list[str, str, Any]
        Three elements of the AddFields field_description parameter 
        Field Type, {Field Alias}, {Field Length}
        Reserved for numeric fields, either a sequence field or when the 
        primary field is numeric.
    prop_dtype1 : list[str, str, Any]
        Three elements of the AddFields field_description parameter 
        Field Type, {Field Alias}, {Field Length}
        Reserved for string class fields.

    Returns
    -------
    list[str]
        Fields found in the output summary table
    """
    try:
        sym_fld = col_n
        if arcpy.ListTables(f"{gdb_p}/{tab_n}"):
                arcpy.management.Delete(f"{gdb_p}/{tab_n}")
        arcpy.management.CreateTable(gdb_p, tab_n)

        field_desc = [
            ["AREASYMBOL", "TEXT", '', 20, '', ''],
            ["MUKEY"] + mk_dtype + ['', ''],
            ["COMPPCT_R", "SHORT", '', '', '', '']
        ]

        if lev1 == "interp": 
            #fuzzy_b and agg_meth in ('Least Limiting', 'Most Limiting'): #if 
            field_desc += [
                [f"class_{col_n}"] + prop_dtype0 + ['', ''],
                [f"val_{col_n}"] + prop_dtype1 + ['', '']
            ]
            arcpy.management.AddFields(
            in_table=f"{gdb_p}/{tab_n}",
            field_description=field_desc,
            template=None
            )
            fields = [
                'AREASYMBOL', 'MUKEY', 'COMPPCT_R', 
                'class_' + col_n, 'val_' + col_n
            ]
            sym_fld = 'class_' + col_n

        elif 'comonth' in tab_n:
            if ag_meth in ("Dominant Condition", "Median Frequency"):
                field_desc += [
                    [col_n] + prop_dtype0 + ['', ''],
                    [f"seq_{col_n}"] + prop_dtype1 + ['', '']
                ]
                fields = ['AREASYMBOL', 'MUKEY', 'COMPPCT_R', col_n, 
                          f"seq_{col_n}"]

            elif ag_meth in ('Highest Frequency', 'Lowest Frequency'):
                field_desc += [
                    [col_n] + prop_dtype0 + ['', ''],
                    [f"seq_{col_n}"] + prop_dtype1 + ['', ''],
                    ['occurrent_months', 'TEXT', '', 72, '', '']
                ]
                fields = ['AREASYMBOL', 'MUKEY', 'COMPPCT_R', col_n, 
                          f"seq_{col_n}", 'occurrent_months']

            elif ag_meth == 'Percent Present':
                field_desc += [[f"prop_{col_n}"] + prop_dtype0 + ['', '']]
                sym_fld = "COMPPCT_R"
                fields = ['AREASYMBOL', 'MUKEY', 'COMPPCT_R', f"prop_{col_n}"]
            elif ag_meth == 'Frequency Count':
                field_desc += [
                    [col_n] + prop_dtype0 + ['', ''],
                    ['mensual_count', 'SHORT', '', '', '', ''],
                    ['occurrent_months', 'TEXT', '', 72, '', '']
                ]
                fields = ['AREASYMBOL', 'MUKEY', 'COMPPCT_R', col_n, 
                          'mensual_count', 'occurrent_months']
                sym_fld = "mensual_count"
            arcpy.management.AddFields(
                in_table=f"{gdb_p}/{tab_n}",
                field_description= field_desc,
                template=None
            )

        elif prop_dtype == 'Text':
            field_desc += [
                [f"prop_{col_n}"] + prop_dtype0 + ['', ''],
                [f"seq_{col_n}"] + prop_dtype1 + ['', '']
            ]
            arcpy.management.AddFields(
                in_table=f"{gdb_p}/{tab_n}",
                field_description= field_desc,
                template=None
            )
            fields = [
                'AREASYMBOL', 'MUKEY', 'COMPPCT_R', 
                'prop_' + col_n, 'seq_' + col_n
            ]
            sym_fld = 'prop_' + col_n
        # Numeric
        else:
            field_desc += [[f"{col_n}"] + prop_dtype1 + ['', '']]
            arcpy.management.AddFields(
                in_table=f"{gdb_p}/{tab_n}",
                field_description=field_desc,
                template=None
            )
            fields = ['AREASYMBOL', 'MUKEY', 'COMPPCT_R', col_n]

        return sym_fld, fields
    
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return []
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return []
