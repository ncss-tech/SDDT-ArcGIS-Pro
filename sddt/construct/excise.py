#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Emulates Cascade Delete to excise mapnuits and their child tables. It will 
also excise soil survey area parents if all mapunit children have been 
excised.

Work indirectly from a MUPOLYGON feature by setting Mukeys
Work directly from MUPOLYGON selected set from within FGDB
Work indirectly with a clipping geography

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@Version: 0.1

"""
version = "0.1"
import os
import sys

import arcpy

from .. import pyErr
from .. import arcpyErr
    

def createTableRelationships(gdb_p: str, val1_b: bool) -> bool:
    """Reestablishes relationships between the MUPOLYGON featurte and
     the mapunit, muaggatt, Valu1, and DominantComponent tables using arcpy
    CreateRelationshipClass function. 

    Parameters
    ----------
    gdb_p : str
        The path of the new geodatabase with the recently imported SSURGO 
        tables.
    val1_b : bool
        True Valu1 and DominantComponent tables are present in SSURGO FGDB. 
        If present, create relationship classes with these two tables too.

    Returns
    -------
    bool
        True if successful, False if unsuccessful.

    """
    try:
        arcpy.AddMessage(
            "\n\tRecreating table relationships for MUPOLYGON..."
        )
        arcpy.env.workspace = gdb_p
        if val1_b:
            relations = [('mapunit', 'MUPOLYGON', 'mukey', 'MUKEY'),
                        ('muaggatt', 'MUPOLYGON', 'mukey', 'MUKEY'),
                        ('Valu1', 'MUPOLYGON', 'mukey', 'MUKEY'),
                        ('DominantComponent', 'MUPOLYGON', 'mukey', 'MUKEY')]
        else:
            relations = [('mapunit', 'MUPOLYGON', 'mukey', 'MUKEY'),
                        ('muaggatt', 'MUPOLYGON', 'mukey', 'MUKEY')]
        
        for ltab, rtab, lcol, rcol in relations:
            rel_n = f"z_{ltab.lower()}_{rtab.lower()}"
            # create Forward Label i.e. "> Horizon AASHTO Table"
            fwdLabel = f"on {lcol}"
            # create Backward Label i.e. "< Horizon Table"
            backLabel = f"on {rcol}"
            arcpy.SetProgressorLabel(
                "Creating table relationship "
                f"between {ltab} and {rtab}"
            )
            arcpy.management.CreateRelationshipClass(
                f"{gdb_p}/{ltab}", f"{gdb_p}/{rtab}", rel_n,
                "SIMPLE", fwdLabel, backLabel, "NONE",
                "ONE_TO_MANY", "NONE", lcol, rcol
            )
        return True

    except arcpy.ExecuteError:
        try:
            del sCur
        except:
            pass
        arcpy.AddMessage(
            f"{gdb_p}/{rtab}, {gdb_p}/{ltab}, {rel_n}, SIMPLE, "
            f"{fwdLabel}, {backLabel}, NONE, ONE_TO_MANY, NONE, {rcol}, {lcol}"
        )
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        try:
            del sCur
        except:
            pass
        arcpy.AddMessage(
            f"{gdb_p}/{rtab}, {gdb_p}/{ltab}, {rel_n}, SIMPLE, "
            f"{fwdLabel}, {backLabel}, NONE, ONE_TO_MANY, NONE, {rcol}, {lcol}"
        )
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False
    

def main(args):
    try:
        arcpy.AddMessage(f"Excision version: {version}")
        # try:
        ssurgo_gdb = args[0]
        mu_sub = args[1]
        clip_f = args[2]
        rebuild_b = args[3]
        #     arcpy.AddMessage(f"{ssurgo_gdb=}: {type(ssurgo_f)}")
        # except:
        #     pass
        # ssurgo_gdb = arcpy.GetParameterAsText(0)
        # mu_sub = arcpy.GetParameter(1)
        # clip_f = arcpy.GetParameter(2)
        # rebuild_b = arcpy.GetParameter(3)

        arcpy.env.workspace = ssurgo_gdb

        if (f"{type(clip_f)}" == "<class 'arcpy._mp.Layer'>" or 
            (f"{type(clip_f)}" == "<class 'geoprocessing value object'" 
             and clip_f.value)):
            clip_b = True
        else:
            clip_b = False

        mu_sub_d = arcpy.Describe(mu_sub)
        mu_whole = f"{ssurgo_gdb}/MUPOLYGON"
        mu_whole_d = arcpy.Describe(mu_whole)

        key_d = {
            "lkey_p": set(),
            "lkey_a": set(),
            "mukey_a": set(),
            "mukey_p": set(),
            "mukey_t": {
                'muaggatt': [], 'muaoverlap': [], 'mucropyld': [], 'mutext': [],
                'component': ["cokey"]
            },
            "cokey_t": {
                'cocanopycover': [], 'cocropyld': [], 'codiagfeatures': [], 
                'coecoclass': [], 'coeplants': [], 'coerosionacc': [], 
                'coforprod': ["cofprodkey"], 'cogeomordesc': ["cogeomdkey"],
                'cohydriccriteria': [], 'cointerp': [], 
                'comonth': ["comonthkey"], 'copmgrp': ["copmgrpkey"],
                'copwindbreak': [], 'corestrictions': [], 'cosurffrags': [],
                'cotaxfmmin': [], 'cotaxmoistcl': [], 'cotext': [], 
                'cotreestomng': [], 'cotxfmother': [], 'chorizon': ["chkey"]
            },
            "cofprodkey_t": {'coforprodo': []},
            "copmgrpkey_t": {'copm': []},
            "comonthkey_t": {'cosoilmoist': [], 'cosoiltemp': []},
            "cogeomdkey_t": {
                'cosurfmorphgc': [], 'cosurfmorphhpp': [], 'cosurfmorphmr': [], 
                'cosurfmorphss': []
            },
            "chkey_t": {
                'chaashto': [], 'chconsistence': [], 'chdesgnsuffix': [], 
                'chfrags': [], 'chpores': [], 'chstructgrp': ["chstructgrpkey"], 
                'chtext': [], 'chtexturegrp': ["chtgkey"], 'chunified': []
            },
            "chstructgrpkey_t": {'chstruct': []},
            "chtgkey_t": {'chtexture': ["chtkey"]},
            "chtkey_t": {'chtexturemod': []},
            "lkey_t": {
                'legend': ["areasymbol"], 'laoverlap': [], 'legendtext': []
            },
            "areasymbol_t": {
                'sainterp': [], 'sacatalog': [], 'FEATPOINT': [], 
                'FEATLINE': [], 'SAPOLYGON': [], 'MUPOINT': [], 'MULINE': [],
            }
        }

        # If clipping geograpy and MUPOLYGON from input ssurgo_fgdb
            # Clip MUPOLYGON
            # Delete original
            # Rename clipped version as MUPOLYGON
        if clip_b and mu_sub_d.path == mu_whole_d.path:
            mu_rename = 'MUPOLYGON_xyz'
            arcpy.analysis.PairwiseClip(mu_sub, clip_f, mu_rename)
            arcpy.management.Delete(mu_whole)
            arcpy.management.Rename(mu_rename, 'MUPOLYGON')
            # From geodataset set mukeys present
            with arcpy.da.SearchCursor(mu_whole, 'MUKEY') as sCur:
                key_d['mukey_p'] = {mk for mk, in sCur}
            rerel_b = True
            # Update Map
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            map = aprx.activeMap
            if f"{type(mu_sub)}" == "<class 'arcpy._mp.Layer'>":
                map.removeLayer(mu_sub)
            mu_add = arcpy.management.MakeFeatureLayer(mu_whole, 'MUPOLYGON')
            map.addLayer(mu_add.getOutput(0))

        # Else If there is a clipping geography
            # Clip to memory and get MUKEY's
            # Add MUPOLYGON to "mukey_t" to include in Cursor cascade
        elif clip_b:
            mu_rename = 'MUPOLYGON_xyz'
            mu_lyr = 'MUPOLYGON_lyr'
            # From geodataset set mukeys present
            with arcpy.da.SearchCursor(mu_sub, 'MUKEY') as sCur:
                key_d['mukey_p'] = {mk for mk, in sCur}
            q = f"""MUKEY IN ('{"', '".join(key_d['mukey_p'])}')"""
            arcpy.management.MakeFeatureLayer(mu_whole, mu_lyr, q)
            arcpy.analysis.PairwiseClip(mu_lyr, clip_f, mu_rename)
            arcpy.management.Delete(mu_whole)
            arcpy.management.Rename(mu_rename, 'MUPOLYGON')
            # Run again as some map units may have been clipped out entirely
            with arcpy.da.SearchCursor(mu_sub, 'MUKEY') as sCur:
                key_d['mukey_p'] = {mk for mk, in sCur}
            rerel_b = True
            # Update Map
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            map = aprx.activeMap
            mu_add = arcpy.management.MakeFeatureLayer(mu_whole, 'MUPOLYGON')
            map.addLayer(mu_add.getOutput(0))
        # Else If MUPOLYGON layer is the MUPOLYGON from the input ssurgo_fgdb
            # Use Delete Features to maintain selected set
        elif mu_sub_d.path == mu_whole_d.path:
            arcpy.AddMessage('Subsetting MUPOLYGON')
            with arcpy.da.SearchCursor(mu_sub, 'MUKEY') as sCur:
                key_d['mukey_p'] = {mk for mk, in sCur}
            # switch to selection to delete
            arcpy.management.SelectLayerByAttribute(mu_sub, 'SWITCH_SELECTION')
            arcpy.management.DeleteFeatures(mu_sub)
            rerel_b = False
        # Else
            # Add MUPOLYGON to "mukey_t" to include in Cursor cascade
        else:
            with arcpy.da.SearchCursor(mu_sub, 'MUKEY') as sCur:
                key_d['mukey_p'] = {mk for mk, in sCur}
            key_d['mukey_t']['MUPOLYGON'] = []
            rerel_b = False

        # If Dominant Componet and Valu1 tables 
            # add them to "mukey_t" to include in Cursor cascade
            # Later rebuild relationship classes
        if arcpy.Exists(ssurgo_gdb + '/Valu1'):
            key_d['mukey_t']['Valu1'] = []
            key_d['mukey_t']['DominantComponent'] = []
            val1_b = True
        else:
            val1_b = False

        # From mapunit 
        with arcpy.da.UpdateCursor(
            ssurgo_gdb + '/mapunit', ['mukey', 'lkey']
        ) as uCur:
            for mk, lk in uCur:
            # set lkeys all and lkeys present that correspond to mukeys present
                # lkeys excise = lkeys all - lkeys present
            # set mukeys all
                # mukeys excise = mukeys all - mukeys present
                if mk in key_d['mukey_p']:
                    key_d['lkey_p'].add(lk)
                    key_d['mukey_p'].add(mk)
                else:
                    uCur.deleteRow()
                key_d['lkey_a'].add(lk)
                key_d['mukey_a'].add(mk)
        key_d['lkey_e'] = key_d['lkey_a'] - key_d['lkey_p']
        key_d['mukey_e'] = key_d['mukey_a'] - key_d['mukey_p']

        arcpy.AddMessage(
            f"\t{'mapunit:':.<20} excised {len(key_d['mukey_e']):,} "
            f"{len(key_d['mukey_a']):,} records"
        )

        keys = ['mukey', 'lkey']
        for K in keys:
            # arcpy.AddMessage(f"{K=}")
            q = f"""{K} IN ('{"', '".join(key_d[K + '_e'])}')"""
            # arcpy.AddMessage(f"{q=}")
            for table, ks in key_d[K + '_t'].items():
                count_i = int(arcpy.management.GetCount(
                    f"{ssurgo_gdb}/{table}"
                ).getOutput(0))
                # if table has children get table keys
                if ks:
                    # Add key name to keys list
                    keys += ks
                    with arcpy.da.UpdateCursor(
                        f"{ssurgo_gdb}/{table}", ks, q
                    ) as uCur:
                        # New set of parent keys to excise child records
                        key_d[ks[0] + '_e'] = set()
                        for count, kj in enumerate(uCur):
                            key_d[ks[0] + '_e'].add(kj[0])
                            uCur.deleteRow()
                else:
                    with arcpy.da.UpdateCursor(
                        f"{ssurgo_gdb}/{table}", K, q
                    ) as uCur:
                        for count, ki in enumerate(uCur):
                            uCur.deleteRow()
                table = table + ':'
                if count_i:
                    arcpy.AddMessage(
                        f"\t{table:.<20} excised {count + 1:,} "
                        f"of {count_i:,} records"
                    )
                else:
                    arcpy.AddMessage(
                        f"\t{table:.<20} excised {count:,} "
                        f"of {count_i:,} records"
                    )

        # Recalculate extents of SSURGO spatial features
        spaital_l = ['FEATPOINT', 'FEATLINE', 'SAPOLYGON', 'MUPOINT', 'MULINE',
                     'MUPOLYGON']
        for feat in spaital_l:
            feat_p = f"{ssurgo_gdb}/{feat}"   
            arcpy.management.RecalculateFeatureClassExtent(feat_p)

        if rerel_b:
            createTableRelationships(ssurgo_gdb, val1_b)

        if rebuild_b:
            arcpy.AddMessage('\nRebuilding MURASTER files\n')
            import rasterize_mupolygon  # sddt.construct.
            
            arcpy.env.workspace = ssurgo_gdb
            mur_l = arcpy.ListRasters('MURASTER*')
            for mur in mur_l:
                if '10m' in mur or '1_3as' in mur:
                    cell_size = 10
                elif '30m' in mur or '1as' in mur:
                    cell_size = 30
                elif '5m' in mur or '1_9as' in mur:
                    cell_size = 5
                elif '90m' in mur or '3as' in mur:
                    cell_size = 90
                else:
                    arcpy.AddMessage(
                        f"Raster {mur} isn't a standard SSURGO raster "
                        "cell size, can't rebuild."
                    )
                    continue
                rasterize_mupolygon.main(
                    [ssurgo_gdb,], 'MUPOLYGON', cell_size, False, 'CELL_CENTER',
                    os.path.dirname(__file__)
                )
            
        arcpy.Compact_management(ssurgo_gdb)

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