#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_Infeat: gSSURGO Feature or Raster (optional) 
Parameter for Summarize Soil Information tool

This parameter is input SSURGO raster or vector feature to which the 
aggregation table will be joined to for convenience.


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 03/10/2026
    @by: Alexnder Stum
@version 1.0



"""
import arcpy
import os

from ... import pyErr


class Param_InFeat():
    def __init__(self):
        # layer name: Path
        self.paths = {}
        self. error = None

        self.param = arcpy.Parameter(
            displayName="gSSURGO Feature or Raster (optional)",
            name="inputSSURGO",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            multiValue=False
        )
        self.get_layers()
        self.param.filter.list = list(self.paths.keys())
        
    def get_layers(self):
        """Populates a dictionary of potential SSURGO features found in the ToC. 
        The dictionary is keyed by the layer name and contains the path of 
        the feature. These features are saved to self.paths
        """
        try:
            # Create list of SSURGO datasets present in map
            act_map = arcpy.mp.ArcGISProject("CURRENT").activeMap
            if act_map:
                # Find all fgdb data sources in TOC
                lyrs = act_map.listLayers()
                i = 1
                for lyr in lyrs:
                    if lyr.isRasterLayer or lyr.isFeatureLayer:
                        path = os.path.dirname(lyr.dataSource)
                        if os.path.splitext(path)[1] == '.gdb':
                            lyr_flds = arcpy.Describe(lyr).fields
                            for fld in lyr_flds:
                                if fld.name.lower() == 'mukey':
                                    lyr_ref = f"{i}: {lyr.name}"
                                    self.paths[lyr_ref] = lyr.dataSource
                                    i += 1
                                    break
            self.error = None
        except:
            self.error = pyErr('Param_InFeat')
            return {}