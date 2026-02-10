#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Create gSSURGO Raster
Intended for the Soil Data Development Toolbox for ArcGIS Pro

This tool creates a rasterized version of the soil polygon
MUPOLYGON feature


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 02/06/2026
    @by: Alexnder Stum
@version 1.0

"""
version = "1.0"

import os
import inspect

import arcpy

from ..construct.rasterize_mupolygon import main as rasterize_mupolygon


class Rasterize(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create gSSURGO Raster"
        self.description = ("Create gSSURGO, a raster version soil polygons.")
        self.category = '2) Construct Databases'


    def getParameterInfo(self):
        """Define parameter definitions"""
        # parameter 0
        params = [arcpy.Parameter(
            displayName="SSURGO Databases",
            name="inputFolders",
            direction="Input",
            parameterType="Required",
            datatype="DEWorkspace",
            multiValue=True)]
        params[0].filter.list = ["Local Database"]

        # parameter 1
        params.append(arcpy.Parameter(
            displayName="Soil Polygon Feature Name Pattern",
            name="mu",
            direction="Input",
            parameterType="Required",
            datatype="String"
        ))
        params[1].filter.list = []

        # parameter 2
        params.append(arcpy.Parameter(
            displayName="Output Cell Size",
            name="resolution",
            direction="Input",
            parameterType="Required",
            datatype="Long"
        ))
        params[2].filter.list = [5, 10, 30, 90]
        params[2].value = 10

        # parameter 3
        params.append(arcpy.Parameter(
            displayName="Create raster outside of geodatabase",
            name="external",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False
        ))
        params[3].value = False
        
    
        # parameter 4
        params.append(arcpy.Parameter(
            displayName="Cell assignment type",
            name="cell_ass",
            direction="Input",
            parameterType="Required",
            datatype="String",
            enabled=True
        ))
        params[-1].filter.list = ["CELL_CENTER", "MAXIMUM_AREA", 
                                  "MAXIMUM_COMBINED_AREA"]
        params[-1].value = "CELL_CENTER"
        return params


    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        # Input folder updated 
        if params[0].altered and params[0].value:
            wksp = params[0].values[0]
            # arcpy.env.workspace = wksp
            poly_w = arcpy.da.Walk(
                    wksp, datatype='FeatureClass', type='Polygon')
            poly_l = [filename
                      for dirpath, dirnames, filenames in poly_w
                      for filename in filenames 
                      ]
            params[1].filter.list = poly_l

            wksp_d = arcpy.Describe(wksp)
            # if wksp_d.extension == "gdb":
            #     params[3].enabled = True
            # else:
            #     params[3].enabled = False
        return


    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        params[1].clearMessage()
        params[0].clearMessage()
        if params[0].value and params[1].value:
            wksp_l = params[0].values
            mu_n = params[1].value
            name_flag = True
            wksp_flag = True
            for wksp in wksp_l:
                wksp_d = arcpy.Describe(wksp)
                poly_w = arcpy.da.Walk(
                    wksp, datatype='FeatureClass', type='Polygon')
                poly_l = [filename
                          for dirpath, dirnames, filenames in poly_w
                          for filename in filenames
                          ]
                if not poly_l:
                    name_flag = False
                if wksp_d.extension != 'gdb':
                    wksp_flag = False
            if not name_flag:
                params[1].setErrorMessage(
                    f"{params[1].value} is not found in every "
                    "selected database")
            if not wksp_flag:
                params[0].SetErrorMessage(
                    f"At this time only SSURGO File Geodatabase can be input.")
        return


    def execute(self, params, messages):
        """The source code of the tool."""
        arcpy.AddMessage(f"SDDT {version=}")
        
        wksp_l = [wksp for wksp in params[0].values]
        rasterize_mupolygon(*[
            wksp_l,
            params[1].valueAsText,
            params[2].value,
            params[3].value,
            params[4].valueAsText,
            os.path.dirname(inspect.getfile(rasterize_mupolygon))
            ])
        
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
    
