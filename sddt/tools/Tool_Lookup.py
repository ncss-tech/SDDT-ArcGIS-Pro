#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raster Lookup by Table
Intended for the Soil Data Development Toolbox for ArcGIS Pro

This tool reclassifies an integer raster with values from a lookup table

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

# from importlib import reload
import arcpy

from ..manage.lookup import main as lookup

arcpy.env.addOutputsToMap = True


class Lookup:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Raster Lookup by Table"
        self.description = (
            "This tool was designed with gSSURGO MURASTER in mind,"
            " where soil properties/interps aggregated by map unit could "
            "hardened instead of just joined to a RAT. "
            "This is particularly useful for use and visualization in QGIS "
            "or is optimal for certain ESRI Geoprocessing tools that don't "
            "work with joined fields in rasters or perform suboptimally.\n"
            "This script is similar to ESRI Reclass by Table geoprocessing "
            "tool, but allows the Value field to be float or integer and the "
            "lookup value to be either string or numeric. When it is String, "
            "it conserves this nominal class in the output's RAT. "
            "Also, in the case of using "
            "summary talbes from SDDT, you won't have to first make an integer "
            "field to hold the  mukey.\n"
            "This script is aslo similar to ESRI Lookup, but Lookup behaves "
            "very poorly with joined  fields, often failing to complete when "
            "the raster is modest in size."
        )

        self.category = '4) Manage Databases'

    def getParameterInfo(self):
        """Define the tool parameters."""
        # parameter 0
        params = [arcpy.Parameter(
            displayName="Input Raster to be Reclassified",
            name="raster_source",
            direction="Input",
            parameterType="Required",
            datatype="GPRasterLayer", #"DERasterDataset",
            multiValue=False
        )]

        # parameter 1
        params.append(arcpy.Parameter(
            displayName="Reclassified Output Raster (only as .tif)",
            name="raster_output",
            direction="Output",
            parameterType="Required",
            datatype="DERasterDataset",
            multiValue=False
        ))

        # parameter 2
        params.append(arcpy.Parameter(
            displayName="Lookup Table",
            name="lookup_table",
            direction="Input",
            parameterType="Required",
            datatype="GPTableView",
            multiValue=False
        ))

        # parameter 3
        params.append(arcpy.Parameter(
            displayName="Field with Current Raster Values (mukey)",
            name="mukey_fld",
            direction="Input",
            parameterType="Required",
            datatype="Field",
            multiValue=False
        ))
        params[-1].parameterDependencies = [params[2].name]
        # params[-1].schema.clone = True

        # parameter 4
        params.append(arcpy.Parameter(
            displayName="Field with New Raster Value/Class (property or interp)",
            name="prop_fld",
            direction="Input",
            parameterType="Required",
            datatype="Field",
            multiValue=False
        ))
        params[-1].parameterDependencies = [params[2].name]

         # parameter 5
        params.append(arcpy.Parameter(
            displayName="Field with Sequence or Key Index (optional)",
            name="seq_fld",
            direction="Input",
            parameterType="Optional",
            datatype="Field",
            multiValue=False
        ))
        params[-1].parameterDependencies = [params[2].name]

         # parameter 6
        params.append(arcpy.Parameter(
            displayName="Null Class Behavior (optional)",
            name="seq_type",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            multiValue=False,
            category="NoData",
            enabled=True #False
        ))
        params[-1].filter.list = ["NoData", "None"]
        params[-1].value = "NoData"

         # parameter 7
        params.append(arcpy.Parameter(
            displayName="Null Class Value (optional)",
            name="seq_nd",
            direction="Input",
            parameterType="Optional",
            datatype="GPLong",
            multiValue=False,
            category="NoData",
            enabled=False
        ))

        # parameter 8
        params.append(arcpy.Parameter(
            displayName="NoData Value (optional)",
            name="nd",
            direction="Input",
            parameterType="Optional",
            datatype="GPDouble",
            multiValue=False,
            category="NoData"
        ))

        return params

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        # if params[5].value and not params[5].hasBeenValidated:
        #     params[6].enabled = True
        # if not params[5].value:
        #     params[6].enabled = False
        #     params[7].enabled = False
        #     params[6].value = "NoData"
        #     params[7].value = None
        if params[6].value == "NoData":
            params[7].enabled = False
            params[7].value = None
        if params[6].value == "None":
            params[7].enabled = True

        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        params[1].clearMessage()
        if params[1].value:
            rast_p = params[1].valueAsText
            if '.gdb' in rast_p:
                params[1].setErrorMessage(
                    "Raster can't be save within a File Geodatabase. "
                    "Only as stand alone .tif."
                )
            elif '.tif' not in rast_p:
                params[1].setErrorMessage(
                    "Raster must be output as .tif file."
                )

        return

    def execute(self, params, messages):
        """The source code of the tool."""

        # import sddt.manage.lookup
        # reload(sddt.manage.lookup)
        d0 = arcpy.Describe(params[0].value)
        d2 = arcpy.Describe(params[2].value)

        # if not params[5].value:
        #     params[6].value = "NoData"
        #     params[7].value = None

        lookup(
            d0.catalogPath,
            params[1].valueAsText,
            d2.catalogPath,
            params[3].valueAsText,
            params[4].valueAsText,
            params[5].valueAsText,
            params[6].valueAsText,
            params[7].value,
            params[8].value,
            1
        )
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return