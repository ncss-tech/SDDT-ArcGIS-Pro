#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build Parrallel

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 02/05/2026
    @by: Alexnder Stum
@version: 0.3

# --- Update 02/05/2026; v 0.3
- Removed arcpyErr and pyErr functions, calling from sddt
# ---
Updated 10/11/22; v 0.2
- Added parameters with spatial reference information
"""


import arcpy
import gc
import os
    

def dissolve_ssa(mu_p: str, epsg: int, tm: str) -> list:
    """This function stages soil polygon features for insertion.
    It projects, repairs geometry (OGC), dissolves soil polygon features
    and returns them as a list rows to be later inserted.

    Parameters
    ----------
    mu_p : str
        Path to the soil polygon shapefile that is to be prepped for insertion.
    epsg : int
        The spatial reference epsg code
    tm : str
        The transformation to be used in projection

    Returns
    -------
    list
        A 'row' ready to be inserted:
        polgon geometry, area symbol , spatial version number, map unit symbol,
            map unit key
    """
    try:
        ssa = os.path.basename(mu_p)
        ssa = ssa[:-4]
        temp_p = 'memory/copy_' + ssa
        mudis_p = 'memory/dis_' + ssa

        arcpy.management.Project(mu_p, temp_p, epsg, tm)

        _ = arcpy.management.RepairGeometry(
            in_features=temp_p,
            delete_null="DELETE_NULL",
            validation_method="OGC"
        )
        # Dissolve
        fields = "SPATIALVER FIRST;AREASYMBOL FIRST;MUSYM FIRST"
        # arcpy.management.Dissolve(
        arcpy.analysis.PairwiseDissolve(
            in_features=temp_p,
            out_feature_class=mudis_p,
            dissolve_field="MUKEY",
            statistics_fields=fields,
            multi_part="MULTI_PART"
        )
        arcpy.Delete_management(temp_p)
        fields = [
            'SHAPE@', 'FIRST_AREASYMBOL', 'FIRST_SPATIALVER', 'FIRST_MUSYM',
            'MUKEY'
        ]
        with arcpy.da.SearchCursor(mudis_p, fields) as sCur:
            feat_l = [row for row in sCur]
        arcpy.Delete_management(mudis_p)
        gc.collect()
        return feat_l

    except arcpy.ExecuteError:
        #func = sys._getframe(  ).f_code.co_name
        func = 'build'
        msgs = arcpyErr(func)
        return [2, msgs]
    except:
        #func = sys._getframe(  ).f_code.co_name
        func = 'build'
        msgs = pyErr(func)
        return [3, msgs]


def append_ssa(feat_p: str, fields: list[str,], epsg: int, tm: str) -> list:
    """This function stages ssurgo features for insertion.
    It projects, repairs geometry (OGC), features
    and returns them as a list rows to be later inserted.

    Parameters
    ----------
    feat_p : str
        Path to the ssurgo shapefile that is to be prepped for insertion.
    fields : list[str,]
        A list of fields found in the ssurgo feature
    epsg : int
        The spatial reference epsg code
    tm : str
        The transformation to be used in projection

    Returns
    -------
    list
        A 'row' ready to be inserted:
        The geometry, as well as an element for each field specified.
    """
    try:
        ssa = os.path.basename(feat_p)
        ssa = ssa[:-4]
        temp_p = 'memory/copy_' + ssa

        arcpy.management.Project(feat_p, temp_p, epsg, tm)
        
        with arcpy.da.SearchCursor(temp_p, fields) as sCur:
            feat_l = [row for row in sCur]
        arcpy.Delete_management(temp_p)
        gc.collect()
        return feat_l

    except arcpy.ExecuteError:
        #func = sys._getframe(  ).f_code.co_name
        func = 'build'
        msgs = arcpyErr(func)
        return [2, msgs]
    except:
        #func = sys._getframe(  ).f_code.co_name
        func = 'build'
        msgs = pyErr(func)
        return [3, msgs]