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
@modified 02/12/2026
    @by: Alexnder Stum
@version: 0.2

# --- Updated, v 0.2
- Enabled the joining of tables with differing mukey data types which will
allow the joining of tables from gpkg's which have numeric mukey field which
also stands in as the OID field
"""
v = '0.2'

import arcpy
import sys
import pyarrow as pa

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
               if f.name == 'mukey' or (f.type not in ['OID', 'Geometry'] 
               and not f.name.lower().startswith('shape_'))]
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
        key_type = tab0.schema.field(muk_f0).type
        arcpy.AddMessage('\tLoaded ' + tab_n)

        ex_fields = ['areasymbol', 'musym', 'muname', 'areaname']
        for ti, tab in enumerate(input_tabs[1:]):
            muk_f1 = ''
            # Get table name, either layer or feature
            if 'Table' in str(type(tab)):
                tab_n = tab.name
            else:
                tab_n = tab.value

            # Curate fields
            field_names = [f.name for f in arcpy.ListFields(tab) 
                if f.name == 'mukey' or (f.type not in ['OID', 'Geometry'] 
               and not f.name.lower().startswith('shape_')
                and f.name.lower() not in ex_fields)]
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
            
            # Check key data type
            if key_type != tab1.schema.field(muk_f1).type:
                schema1 = tab1.schema
                fi = schema1.get_field_index(muk_f1)
                schema2 = schema1.set(fi, pa.field(muk_f1, key_type))
                tab1 = tab1.cast(schema2)
                
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
            arcpy.AddMessage('\tLoaded ' + tab_n)
       
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
