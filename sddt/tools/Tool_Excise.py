#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Subset gSSURGO Database
Intended for the Soil Data Development Toolbox for ArcGIS Pro

This tool will subset a File Geodatabase gSSURGO Database.
This tool is still under development


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 02/06/2026
    @by: Alexnder Stum
@version 0.1

"""
version = "0.1"

import arcpy

from ..construct.excise import main as excise


class excise(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Subset gSSURGO database"
        self.description = ("Removes map units and attendant parent/child "
                            "tables from gSSURGO database.")
        self.category = '2) Construct Databases'

    def getParameterInfo(self):
        """Define parameter definitions"""
        # parameter 0
        params= [arcpy.Parameter(
            displayName="Input SSURGO FGDB to be subset",
            name="input_gSSURGO",
            direction="Input",
            parameterType="Required",
            datatype="DEWorkspace"
        )]

        # parameter 1
        params.append(arcpy.Parameter(
            displayName="Subset MUPOLYGON feature",
            name="mu_lyr",
            direction="Input",
            parameterType="Required",
            datatype="GPFeatureLayer",
        ))
        params[-1].filter.list = ['Polygon']

        # parameter 2
        params.append(arcpy.Parameter(
            displayName="Clipping Feature",
            name="mu_lyr",
            direction="Input",
            parameterType="Optional",
            datatype="GPFeatureLayer",
        ))
        params[-1].filter.list = ['Polygon']

        # parameter 3
        params.append(arcpy.Parameter(
            displayName="Rebuild MURASTER?",
            name="rebuild",
            direction="Input",
            parameterType="Required",
            datatype="GPBoolean",
        ))
        params[-1].value = True
        return params

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return
    
    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return
    
    def execute(self, params, messages):
        """The source code of the tool."""
        arcpy.AddMessage(f"Tool_Excise {version=}")

        wksp_l = [wksp for wksp in params[0].values]
        excise(*[
            params[1].valueAsText,
            params[2].value,
            params[3].value,
            params[4].value,
            ])
        
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return