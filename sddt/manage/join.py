#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Join Aggregated Summaries
Join (~ full outer join) any map unit level tables on mukey 
into a new column bound table. Intended to join outputs from the Summarize
Soil Information tool

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 02/09/2026
    @by: Alexnder Stum
@version: 0.1
"""
v = '0.1'

import arcpy
import sys

from .. import pyErr
from .. import arcpyErr


def main(args) -> bool:
    try:
        arcpy.AddMessage("Join Aggregated Summaries, version: " + v)
        input_tabs = args[0]
        out_tab = args[1]

        # Start off with first table
        muk_f0 = ''
        tab = input_tabs[0]
        if 'Table' in str(type(tab)):
            tab_n = tab.name
        else:
            tab_n = tab.value
        field_names = [f.name for f in arcpy.ListFields(tab) 
               if f.type not in ['OID', 'Geometry'] 
               and not f.name.lower().startswith('shape_')]
        if 'mukey' in field_names:
            muk_f0 = 'mukey'
        elif 'MUKEY' in field_names:
            muk_f0 = 'MUKEY'
        else:
            for f in field_names:
                if 'mukey' in f.lower():
                    muk_f0 = f
                    break
            if not muk_f0:
                arcpy.AddError(f"mukey field not found in {tab_n}")
                return ''
        
        tab0 = arcpy.da.TableToArrowTable(
            in_table=tab,
            field_names=field_names
        )
        arcpy.AddMessage('Loaded' + tab_n)
        for ti, tab in enumerate(input_tabs[1:]):
            muk_f1 = ''
            if 'Table' in str(type(tab)):
                tab_n = tab.name
            else:
                tab_n = tab.value
            field_names = [f.name for f in arcpy.ListFields(tab) 
                if f.type not in ['OID', 'Geometry'] 
                and not f.name.lower().startswith('shape_')
                and not f.name.lower() == 'areasymbol']
            if 'mukey' in field_names:
                muk_f1 = 'mukey'
            elif 'MUKEY' in field_names:
                muk_f1 = 'MUKEY'
            else:
                for f in field_names:
                    if 'mukey' in f.lower():
                        muk_f1 = f
                        arcpy.AddWarning(
                            f'Using field {muk_f1} as key field '
                            'in table {tab.name}'
                        )
                        break
                if not muk_f1:
                    arcpy.AddError(
                        f"mukey field not found in {tab_n}"
                    )
                    return ''
            tab1 = arcpy.da.TableToArrowTable(
                in_table=tab,
                field_names=field_names,
            )
            arcpy.AddMessage('Loaded' + tab_n)
            # rename duplicative field names
            cur_fnames = tab0.column_names
            relab_b = False
            for f in field_names:
                if (f != muk_f1) and (f in cur_fnames):
                    fi = field_names.index(f)
                    field_names.pop(fi)
                    field_names.insert(fi, f'{f}_{ti}')
                    relab_b = True
            if relab_b:
                tab1 = tab1.rename_columns(field_names)

            tab0 = tab0.join(tab1, muk_f0, muk_f1, "full outer")
       
        arcpy.AddMessage(f"Completed: {out_tab.value}")
        # arcpy.AddMessage(out_tab.path)
        arcpy.management.CopyRows(tab0, out_tab)

    except arcpy.ExecuteError:
        arcpy.AddError(f"While working on {tab.value}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return []
    except:
        arcpy.AddError(f"While working on {tab.value}")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return []
    

if __name__ == '__main__':
    main(sys.argv[1:])
