#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Create Valu1 and Dominant Component tables
Intended for the Soil Data Development Toolbox for ArcGIS Pro

This tool creates the valu1 and Dominant Component tables. The valu1 
is the aggregation various soil properties to the map unit level. The 
Dominant Component table identifies the component (by cokey) of 
greatest percent composition for each map unit


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

import arcpy
import os
import inspect

from ..construct.valu1 import main as valu1

class Valu1(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create Valu1 and Dominant Component tables"
        self.description = (
            "Create Valu1 Dominant Component tables "
            "for SSURGO file geodatabase."
        )
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
        arcpy.AddMessage(f"Tool_Valu1 {version=}")

        gdb_p = params[0].values
        complete_b = valu1([
            gdb_p,
            os.path.dirname(inspect.getfile(valu1))
        ])
        if complete_b:
            valu1_b = arcpy.Exists(gdb_p + "/Valu1")
            domcom_b = arcpy.Exists(gdb_p + "/DominantComponent")
            if valu1_b and domcom_b:
                arcpy.AddMessage(
                    "Both the Valu1 and DominantCompoent tables "
                    "were successfully completed."
                )
            elif not valu1_b:
                arcpy.AddMessage(
                    "The DominantComponent table successfully completed"
                )
                arcpy.AddError("The Valu1 table was not successfully completed")
            elif not domcom_b:
                arcpy.AddMessage(
                    "The Valu1 table successfully completed"
                )
                arcpy.AddError(
                    "The DominantComponent table "
                    "was not successfully completed"
                )
            else:
                arcpy.AddError(
                    "Neither the Valu1 and DominantCompoent tables "
                    "were successfully completed."
                )
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return