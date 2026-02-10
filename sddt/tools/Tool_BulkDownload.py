#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bulk SSURGO Download Tool
Intended for the Soil Data Development Toolbox for ArcGIS Pro

This tool downloads SSURGO data from the USDA Web Soil Survey webpage


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
import json
import re

from urllib.request import urlopen

# from sddt.download.query_download import main
from ..download.query_download import main as query_download
# reload(..download.query_download)


class BulkDownload(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Bulk SSURGO Download"
        self.description = (
            "Bulk download of soil surveys from Web Soil Survey. User can "
            "Query with wildcards ('_', '*') to query databases by Areasymbol,"
            " provide soil survey layer, or geography with soil survey layer"
        )
        self.category = '1) Download'
        self.options = [
            'Query by Areasymbol',
            'Query by Survey Name',
            'By Soil Survey boundary layer',
            'By Geography'
        ]

    def getParameterInfo(self):
        """Define parameter definitions"""
        # parameter 0
        params = [arcpy.Parameter(
            displayName="Output Folder",
            name="outPath",
            direction="Input",
            parameterType="Required",
            datatype="Folder"
        )]

        # parameter 1
        params.append(arcpy.Parameter(
            displayName="Selection Method",
            name="option",
            direction="Input",
            parameterType="Required",
            datatype="String"
        ))
        params[1].filter.type = "ValueList"
        params[1].filter.list = self.options

        # parameter 2
        params.append(arcpy.Parameter(
            displayName="Areasymbol Search Criteria",
            name="SSA_q",
            direction="Input",
            parameterType="Optional",
            datatype="String",
        ))

        # parameter 3
        params.append(arcpy.Parameter(
            displayName="Survey Name Search Criteria",
            name="Areaname_q",
            direction="Input",
            parameterType="Optional",
            datatype="String",
        ))

        # parameter 4
        params.append(arcpy.Parameter(
            displayName="Soil Surveys",
            name="SSAs",
            direction="Input",
            parameterType="Optional",
            datatype="String",
            multiValue=True
        ))

        # parameter 5
        params.append(arcpy.Parameter(
            displayName="Auto-selected Soil Surveys Areas",
            name="display",
            direction="Output",
            parameterType="Optional",
            datatype="String",
              enabled=False,
        ))
        params[-1].value = "None"

        # parameter 6
        params.append(arcpy.Parameter(
            displayName="Reference Geography Layer",
            name="geog_lyr",
            direction="Input",
            parameterType="Optional",
            datatype="GPFeatureLayer",
            enabled=False
        ))
        params[-1].filter.list = ["Polygon"]

        # parameter 7
        params.append(arcpy.Parameter(
            displayName="Soil Survey Boundary Layer",
            name="ssa_lyr",
            direction="Input",
            parameterType="Optional",
            datatype="GPFeatureLayer",
            enabled=False
        ))
        params[7].filter.list = ["Polygon"]

        # parameter 8
        params.append(arcpy.Parameter(
            displayName="Include Access Template with Download",
            name="AccessBool",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=True,
        ))
        params[-1].value = False

        # parameter 9
        params.append(arcpy.Parameter(
            displayName="Overwrite Existing",
            name="OverwriteBool",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=True,
        ))
        params[-1].value = False

        # Store choice lists when selection methods are changed
        params.append(arcpy.Parameter(
            displayName="by_sym",
            name="by_sym",
            direction="Input",
            parameterType="Optional",
            datatype="String",
            multiValue=True,
            enabled=False
        ))
        params.append(arcpy.Parameter(
            displayName="by_name",
            name="by_name",
            direction="Input",
            parameterType="Optional",
            datatype="String",
            multiValue=True,
            enabled=False
        ))

        return params

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        # Choice selection made
        # Choice to provide Query
        if params[1].altered and (params[1].value == self.options[0]):
            params[2].enabled = True
            params[3].enabled = False
            params[4].enabled = True
            params[6].enabled = False
            params[7].enabled = False
        elif params[1].altered and (params[1].value == self.options[1]):
            params[2].enabled = False
            params[3].enabled = True
            params[4].enabled = True
            params[6].enabled = False
            params[7].enabled = False

        # Choice to use Soil Survey layer
        elif params[1].value == self.options[2]:
            for i in range(2, 6):
                params[i].enabled = False
            params[6].enabled = False
            params[7].enabled = True

        # Choice to use a geography
        elif params[1].valueAsText == self.options[3]:
            for i in range(2, 5):
                params[i].enabled = False
            params[6].enabled = True
            params[7].enabled = True
        else:
            for i in range(2, 8):
                params[i].enabled = False

        # Clear survey choice list
        clearChoices = False
        # Recreate survey choice list from query
        refreshChoices = False

        # If a query option selected and no query criteria specified
        if ((params[1].value == self.options[0] and params[2].value is None) 
            or (params[1].value == self.options[1] and params[3].value is None)
            ):
            clearChoices = True
            refreshChoices = False
        else:
            # If new Areasymbol query entered
            if (params[1].value == self.options[0]
                and params[2].altered and not params[2].hasBeenValidated):
                clearChoices = True
                refreshChoices = True
            # If new Areaname query entered
            elif (params[1].value == self.options[1]
                and params[3].altered and not params[3].hasBeenValidated):
                clearChoices = True
                refreshChoices = True
        # User switched option back to query, restore form choice list
        if params[1].altered and not params[1].hasBeenValidated:
            if params[1].value == self.options[0] and params[2].value:
                params[4].filter.list = params[10].filter.list
            if params[1].value == self.options[1] and params[3].value:
                params[4].filter.list = params[11].filter.list

        if clearChoices:
            # Clear the choice list
            params[4].filter.list = []
            params[4].values = []

        if refreshChoices:
            # Clear the choice list and create a new one
            params[4].filter.list = []
            params[4].values = []
            if params[1].value == self.options[0]:
                query_f = "AREASYMBOL"
                query = params[2].value
            else:
                query_f = "AREANAME"
                query = params[3].value
            # Create empty value list
            if query == "*":
                # No filters at all
                sQuery = (
                    "SELECT AREASYMBOL, AREANAME, CONVERT(varchar(10), "
                    "[SAVEREST], 126) AS SAVEREST FROM SASTATUSMAP "
                    "ORDER BY AREASYMBOL"
                )
            else:
                # areasymbol filter
                wc = query.replace('*', '%')
                p1 = r"\w+\%\w+\*"  # wild in middle and end
                p2 = r"\%\w+\%w+"  # wild beginning and middle
                p3 = r"\%\w+\%"  # sandwiched by wild
                p4 = r"\w+\%\w+"  # wild in the middle
                p5 = r"\w+[%]"  # wild at the end
                p6 = r"[%]?\w+"  # wild at beginning
                p7 = r"\w+'"  # just a word
                pattern = '|'.join([p1, p2, p3, p4, p5, p6, p7])
                wcc = re.findall(pattern, wc)

                trunk = ("SELECT AREASYMBOL, AREANAME, CONVERT(varchar(10), "
                         "[SAVEREST], 126) AS SAVEREST FROM SASTATUSMAP WHERE "
                         f"{query_f} LIKE '{wcc[0]}'")

                tail = " ORDER BY AREASYMBOL"

                for ssa in wcc[1:]:
                    trunk += f" OR {query_f} LIKE '{ssa}'"
                sQuery = trunk + tail

            url = r'https://SDMDataAccess.sc.egov.usda.gov/Tabular/post.rest'

            # Create request using JSON, return data as JSON
            dRequest = dict()
            dRequest["format"] = "JSON"
            dRequest["query"] = sQuery
            jData = json.dumps(dRequest)

            # Send request to SDA Tabular service using urllib2 library
            jData = jData.encode('ascii')
            response = urlopen(url, jData)
            jsonString = response.read()

            # Convert the returned JSON string into a Python dictionary.
            data = json.loads(jsonString)
            del jsonString, jData, response

            # Find data section (key='Table')
            value_l = list()

            if "Table" in data:
                # Data as a list of lists. All values come back as string.
                dataList = data["Table"]

                # Iterate through dataList, 
                # reformat to create the menu choicelist
                for rec in dataList:
                    areasym, areaname, date = rec
                    if not date is None:
                        date = date.split(" ")[0]
                    else:
                        date = "None"
                    value_l.append(f"{areasym},  {date},  {areaname}")
            else:
                # No data returned for this query
                pass

            # populate survey areas choicelist
            if len(value_l) > 300:
                params[4].enabled = False
                params[5].enabled = True
                params[4].value = value_l
            else:
                params[4].enabled = True
                params[5].enabled = False
            params[5].value = '\n'.join(value_l)
            params[4].filter.list = value_l
            if params[1].value == self.options[0]:
                params[10].filter.list = value_l
            if params[1].value == self.options[1]:
                params[11].filter.list = value_l

        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""

        for i in range(10):
            params[i].clearMessage()

        # Check that crtical parameters are populated per the selected option
        # Query criteria required and soil surveys selection made
        if params[1].value == self.options[0]:
            if not params[2].value:
                params[2].setErrorMessage(
                        'Must provide a query statement to create '
                        'choice list of soil surveys.'
                    )
            if not params[4].value:
                params[4].setErrorMessage(
                    'Must make selection of soil surveys'
                )
        if params[1].value == self.options[1]:
            if not params[3].value:
                params[3].setErrorMessage(
                        'Must provide a query statement to create '
                        'choice list of soil surveys.'
                    )
            if not params[4].value:
                params[4].setErrorMessage(
                    'Must make selection of soil surveys'
                )
        # Provide ssa lyr
        if (params[1].value != self.options[0] 
            and params[1].value != self.options[1]):
            if not params[7].value:
                params[7].setErrorMessage(
                        'Must select a Soil Survey Boundary Layer.'
                    )
        # Provide geography
        if params[1].value == self.options[3]:
            if not params[6].value:
                params[6].setErrorMessage(
                    'Must select a Reference Geography Layer.'
                )

        # Notifying user how many surveys have been auto-selected
        if not params[4].enabled:
            params[5].setWarningMessage((f"{len(params[4].filter.list)} "
                                         "Soil Surveys selected"))
        else:
            params[5].clearMessage()

        return

    def execute(self, params, messages):
        """The source code of the tool."""
        arcpy.AddMessage(f"Tool_BulkDownload {version=}")
        # by query
        if (params[1].value == self.options[0]
            or params[1].value == self.options[1]):
            option = 1
            ssa_l = params[4].values
            ssa_lry = None
            geog_lyr = None
        # by ssa lry
        elif params[1].value == self.options[2]:
            option = 2
            ssa_l = None
            ssa_lry = params[7].value
            geog_lyr = None
        # by geog
        else:
            option = 3
            ssa_l = None
            ssa_lry = params[7].value
            geog_lyr = params[6].value

        query_download([
            params[0].valueAsText,
            option,
            ssa_l,
            ssa_lry,
            geog_lyr,
            params[8].value,
            params[9].value
        ])
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return