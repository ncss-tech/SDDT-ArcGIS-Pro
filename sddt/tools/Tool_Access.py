#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bulk SSURGO Download Tool
Intended for the Soil Data Development Toolbox for ArcGIS Pro

This tool is not complete. It is a draft tool for populating Access SSURGO
templates


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
import re
import os

# from sddt.download.query_download import main
from ..construct.access import main as access
# reload(..download.query_download)


class access_import(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Bulk Import into Access Template"
        self.description = ("Import selected SSURGO datasets into Access Template. "
                            "database. It is not dependent on arcpy")
        self.category = '2) Construct Databases'

    def getParameterInfo(self):
        """Define parameter definitions"""
        params = [arcpy.Parameter(displayName="Folder with SSURGO folders",
                                  name="inputFolder",
                                  direction="Input",
                                  parameterType="Required",
                                  datatype="Folder")
                  ]
        params.append(arcpy.Parameter(displayName="Soil Survey Directories",
                                      name="SSAs",
                                      direction="Input",
                                      parameterType="Required",
                                      datatype="String",
                                      multiValue=True
                                      )
                      )
        params.append(arcpy.Parameter(displayName="Import Strategy",
                                      name="strategy",
                                      direction="Input",
                                      parameterType="Required",
                                      datatype="String")
                      )
        params[2].filter.type = "ValueList"
        params[2].filter.list = [
            'Import into individual Default templates',
            'Import into specified central template',
            'Copy and import specified template in each'
        ]

        params.append(arcpy.Parameter(displayName="Template database",
                                      name="mdb",
                                      direction="Input",
                                      parameterType="Optional",
                                      datatype="DEFile"
                                      )
                      )
        params[3].filter.list = ['mdb']

        return params

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        if params[0].value is None:
            params[1].filter.list = []
            params[1].values = []
        elif params[0].altered and not params[0].hasBeenValidated:
            params[1].values = []
            path = params[0].valueAsText
            folders = list([f for f in os.listdir(path)
                            # if a directory
                            if os.path.isdir(os.path.join(path, f))
                            # and fits @@### pattern or soil_@@###
                            and re.match(r"[a-zA-Z]{2}[0-9]{3}", f.removeprefix('soil_'))])
            params[1].filter.list = folders

        if params[2].value == 'Import into individual Default templates':
            params[3].enabled = False
        else:
            params[3].enabled = True

        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        if (params[2].value != 'Import into individual Default templates') and \
                not params[3].value and params[0].value and params[2].value:
            params[3].setErrorMessage('A template database must be specified')
        else:
            params[3].clearMessage()

        return

    def execute(self, params, messages):
        """The source code of the tool."""
        arcpy.AddMessage(f"Access {version=}")
        access(params[0].valueAsText,
                                   params[1].values,
                                   params[2].value,
                                   params[3].valueAsText)
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return