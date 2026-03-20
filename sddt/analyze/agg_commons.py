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


import re
import sys

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
        gdb_p, att_col, ch_where, interpk, rulek, 
        comp_where1, comp_where2, tab_n, fields
    ):
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
