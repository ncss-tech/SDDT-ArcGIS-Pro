#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@version 0.2
"""
# https://pro.arcgis.com/en/pro-app/latest/arcpy/geoprocessing_and_python/a-template-for-python-toolboxes.htm
import arcpy
import json
import os
from urllib.request import urlopen
import re
from importlib import reload
from itertools import groupby
from arcpy.da import SearchCursor


def byKey(x, i: int=0):
    """Helper function that returns ith element from a Sequence

    Parameters
    ----------
    x : Sequence
        Any indexable Sequence
    i : int, optional
        Index of element to be returned, by default 0

    Returns
    -------
    Any
        ith element from Sequence
    """
    return x[i]

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        # self.label = "SDDT_test"
        self.label = "SDDT"
        self.alias = 'Soil Data Development Toolbox'

        # List of tool classes associated with this toolbox
        self.tools = [BulkD, buildFGDB, rasterize, valu1] #, aggregator]


class BulkD(object):
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
        # paramter 0
        params = [arcpy.Parameter(
            displayName="Output Folder",
            name="outPath",
            direction="Input",
            parameterType="Required",
            datatype="Folder"
        )]

        # paramter 1
        params.append(arcpy.Parameter(
            displayName="Selection Method",
            name="option",
            direction="Input",
            parameterType="Required",
            datatype="String"
        ))
        params[1].filter.type = "ValueList"
        params[1].filter.list = self.options

        # paramter 2
        params.append(arcpy.Parameter(
            displayName="Areasymbol Search Criteria",
            name="SSA_q",
            direction="Input",
            parameterType="Optional",
            datatype="String",
        ))

        # paramter 3
        params.append(arcpy.Parameter(
            displayName="Survey Name Search Criteria",
            name="Areaname_q",
            direction="Input",
            parameterType="Optional",
            datatype="String",
        ))

        # paramter 4
        params.append(arcpy.Parameter(
            displayName="Soil Surveys",
            name="SSAs",
            direction="Input",
            parameterType="Optional",
            datatype="String",
            multiValue=True
        ))

        # paramter 5
        params.append(arcpy.Parameter(
            displayName="Auto-selected Soil Surveys Areas",
            name="display",
            direction="Output",
            parameterType="Optional",
            datatype="String",
              enabled=False,
        ))
        params[5].value = "None"

        # parameter 6
        params.append(arcpy.Parameter(
            displayName="Reference Geography Layer",
            name="geog_lyr",
            direction="Input",
            parameterType="Optional",
            datatype="GPFeatureLayer",
            enabled=False
        ))
        params[6].filter.list = ["Polygon"]

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

        # paramter 8
        params.append(arcpy.Parameter(
            displayName="Include Access Template with Download",
            name="AccessBool",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=True,
        ))
        params[8].value = False

        # paramter 9
        params.append(arcpy.Parameter(
            displayName="Overwrite Existing",
            name="OverwriteBool",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=True,
        ))
        params[9].value = False

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
            params[6].enabled = True

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

                # Iterate through dataList, reformat to create the menu choicelist
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

        # from sddt.download.query_download import main
        import sddt.download.query_download
        reload(sddt.download.query_download)
        sddt.download.query_download.main([
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
        # from sddt.download.query_download import main
        import sddt.construct.access
        reload(sddt.construct.access)
        sddt.construct.access.main(params[0].valueAsText,
                                   params[1].values,
                                   params[2].value,
                                   params[3].valueAsText)
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return


class buildFGDB(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create gSSURGO File Geodatabase"
        self.description = (
            "Create gSSURGO File Geodatabase from downloaded. SSURGO data"
        )
        self.category = '2) Construct Databases'
        self.options = [
            'Select from downloaded SSURGO datasets',
            'By State(s)',
            'By selected features in Soil Survey boundary layer',
            'By Geography',
            'Build CONUS database'
        ]
        self.regions = [
            "Alaska", "Hawaii", "Lower 48 States",
            "Pacific Islands Area", "Puerto Rico and U.S. Virgin Islands",
            "World"
        ]
        # self.ssurgo_dirs = None

    def isSSURGO(self, path):
        # get list of directories in `path`
        # which also fit @@### pattern or soil_@@###
        # And contain a tabular and spatial sub-directory
        dirs = [d.name.removeprefix('soil_')
                for d in os.scandir(path)
                if (d.is_dir()
                    and re.match(
                        r"[a-zA-Z]{2}[0-9]{3}",
                        d.name.removeprefix('soil_')
                        )
                    and os.path.exists(f"{d.path}/tabular")
                    and os.path.exists(f"{d.path}/spatial")
                    )]
        return dirs

    def getParameterInfo(self):
        """Define parameter definitions"""
        # parameter 0
        params = [arcpy.Parameter(
            displayName="Folder with SSURGO Datasets",
            name="inputFolder",
            direction="Input",
            parameterType="Required",
            datatype="Folder"
        )]

        # paramter 1
        params.append(arcpy.Parameter(
            displayName="Select Build Option",
            name="option",
            direction="Input",
            parameterType="Required",
            datatype="String"
        ))
        params[1].filter.type = "ValueList"
        params[1].filter.list = self.options

        # parameter 2
        params.append(arcpy.Parameter(
            displayName="Select Soil Surveys",
            name="ssa_l",
            direction="Input",
            parameterType="Optional",
            datatype="String",
            multiValue=True,
            enabled=False
        ))

        # parameter 3
        params.append(arcpy.Parameter(
            displayName="Select State(s)",
            name="state_l",
            direction="Input",
            parameterType="Optional",
            datatype="String",
            multiValue=True,
            enabled=False
        ))

        # parameter 4
        params.append(arcpy.Parameter(
            displayName="Soil Survey Boundary Layer",
            name="ssa_lyr",
            direction="Input",
            parameterType="Optional",
            datatype="GPFeatureLayer",
            enabled=False
        ))
        params[4].filter.list = ["Polygon"]

        # parameter 5
        params.append(arcpy.Parameter(
            displayName="Reference Geography Layer",
            name="geog_lyr",
            direction="Input",
            parameterType="Optional",
            datatype="GPFeatureLayer",
            enabled=False
        ))
        params[5].filter.list = ["Polygon"]

        # parameter 6
        params.append(arcpy.Parameter(
            displayName="Geographical Unit Symbol Field",
            name="geog_fld",
            direction="Input",
            parameterType="Optional",
            datatype="Field",
            enabled=False
        ))
        params[6].parameterDependencies = [params[5].name]

        # parameter 7
        params.append(arcpy.Parameter(
            displayName="Create FGDBs for these Geographical Units",
            name="geog_l",
            direction="Input",
            parameterType="Optional",
            datatype="String",
            multiValue=True,
            enabled=False
        ))

        # parameter 8
        params.append(arcpy.Parameter(
            displayName="Geographical Place Label",
            name="geog_label",
            direction="Input",
            parameterType="Optional",
            datatype="String",
            enabled=False
        ))

        # parameter 9
        params.append(arcpy.Parameter(
            displayName="Clip to selected geographies",
            name="clip_b",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False
        ))
        params[9].value = False

        # parameter 10
        params.append(arcpy.Parameter(
            displayName="Output SSURGO FGDB",
            name="gdb_p",
            direction="Output",
            parameterType="Optional",
            datatype="DEWorkspace",
            enabled=False
        ))
        params[10].filter.list = ["Local Database"]

        # parameter 11
        params.append(arcpy.Parameter(
            displayName="Output Folder",
            name="out_p",
            direction="Input",
            parameterType="Optional",
            datatype="Folder",
            enabled=False
        ))

        # parameter 12
        params.append(arcpy.Parameter(
            displayName="Geographical Region",
            name="proj_aoi",
            direction="Input",
            parameterType="Required",
            datatype="String"
        ))
        params[12].filter.type = "ValueList"
        params[12].filter.list = self.regions

        # parameter 13
        params.append(arcpy.Parameter(
            displayName="Create Valu1 & DominantComponent tables",
            name="value1_b",
            direction="Input",
            parameterType="Required",
            datatype="GPBoolean",
            enabled=True
        ))
        params[13].value = True

        # parameter 14
        params.append(arcpy.Parameter(
            displayName="Concise gSSURGO",
            name="concise_b",
            direction="Input",
            parameterType="Required",
            datatype="GPBoolean",
            enabled=True
        ))
        params[14].value = True

        # parameter 15
        params.append(arcpy.Parameter(
            displayName="gSSURGO Version",
            name="gSSURGO_v",
            direction="Input",
            parameterType="Required",
            datatype="String",
            enabled=True
        ))
        params[15].filter.type = "ValueList"
        params[15].filter.list = ["gSSURGO traditional", "gSSURGO 2.0"]

        return params

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        # Input folder updated 
        if params[0].altered:# and not params[0].hasBeenValidated:
            ssurgo_dirs = self.isSSURGO(params[0].valueAsText)
            # self.ssurgo_dirs = self.isSSURGO(params[0].valueAsText)

            # If Select by Download the survey choice needs updating
            # if params[1].value == self.options[0]:
            params[2].filter.list = ssurgo_dirs
            # If by State, list of available states needs updating
            # else:
            states = {ssa[0:2].upper() for ssa in ssurgo_dirs}
            states = list(states)
            # Add PRVI if both present
            if 'PR' in states and 'VI' in states:
                states.append('PRVI')
            states.sort()
            params[3].filter.list = states

        # Choice selection made
        # Choice to Select from downloaded SSURGO data
        if params[1].altered and (params[1].value == self.options[0]):
            for i in range(3, 10):
                params[i].enabled = False
            params[2].enabled = True
            params[10].enabled = True
            params[11].enabled = False

        # Choice to Select State(s)
        elif params[1].altered and (params[1].value == self.options[1]):
            for i in range(2, 11):
                params[i].enabled = False
            params[3].enabled = True
            params[11].enabled = True

        # Choice to use Soil Survey layer
        elif params[1].value == self.options[2]:
            for i in range(2, 10):
                params[i].enabled = False
            params[4].enabled = True
            params[10].enabled = True
            params[11].enabled = False
            
        # Choice to use a geography
        elif params[1].valueAsText == self.options[3]:
            for i in range(2, 4):
                params[i].enabled = False
            for i in range(4, 9): # update to 10 if clip functionality added
                params[i].enabled = True
            params[11].enabled = True
            params[10].enabled = False
            # params[7].filter.list = []
        
        # Choice build CONUS
        elif params[1].valueAsText == self.options[4]:
            for i in range(2, 10):
                params[i].enabled = False
            params[10].enabled = True
            params[11].enabled = False
            params[12].value = "Lower 48 States"
        else:
            for i in range(2, 10):
                params[i].enabled = False
        if params[6].altered and not params[6].hasBeenValidated:
              params[7].value = []
              sCur = SearchCursor(
                  params[5].valueAsText, 
                  [params[6].valueAsText]
                  )
              geogs = list({str(g) for g, in sCur})
              params[7].filter.list = geogs
              params[8].value = params[6].valueAsText
              del sCur

        # Enforce File gdb with gdb extension
        if params[10].altered and not params[10].hasBeenValidated:
              db_p = arcpy.Describe(params[10].value).CatalogPath
              path, ext = os.path.splitext(db_p)
              params[10].value = path + '.gdb'

        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        for i in range(11):
            params[i].clearMessage()
        # Filter features to have AREASYMBOL field
        if (params[4].value and params[1].value == self.options[2] 
            and not arcpy.ListFields(params[4].value, 'AREASYMBOL', 'String')):
            params[4].setErrorMessage(f"'{params[4].value.name}' does not have "
                                       "an 'AREASYMBOL' field or it isn't a "
                                       "String data type")

        # Selected SSURGO folder has no valid datasets
        if params[0].value and not params[2].filter.list:
            params[0].setErrorMessage("No valid SSURGO datasets found in "
                                      f"{params[0].valueAsText}")
            if params[1].value == self.options[0]:
                params[2].setWarningMessage("No options, try another folder.")
            if params[1].value == self.options[1]:
                params[3].setWarningMessage("No options, try another folder.")

        # Check that crtical parameters are populated per the selected option
        if params[1].value == self.options[0]:
            if not params[2].value:
                if params[2].message != "No options, try another folder.":
                    params[2].setErrorMessage(
                        'Must select at least one Soil Survey.'
                    )
            if not params[10].value:
                params[10].setErrorMessage('Must specify an output FGDB')
        if params[1].value == self.options[1]:
            if not params[3].value:
                if params[3].message != "No options, try another folder.":
                    params[3].setErrorMessage(
                        'Must select at least one Soil Survey.'
                    )
            elif not params[11].value:
                params[11].setErrorMessage('Must specify an Output location')
        if params[1].value == self.options[2]:
            if not params[4].value:
                params[4].setErrorMessage(
                    'Must select a Soil Survey Boundary Layer.'
                )
            if not params[10].value:
                params[10].setErrorMessage('Must specify an output FGDB')
        if params[1].value == self.options[3]:
            if not params[4].value:
                params[4].setErrorMessage(
                    'A Soil Survey reference layer needed.'
                )
            if not params[5].value:
                params[5].setErrorMessage(
                    'Must select a Reference Geography Layer.'
                )
            elif not params[6].value:
                params[6].setErrorMessage(
                    'Must select a Geographical Unit Field'
                )
            elif not params[7].value:
                params[7].setErrorMessage(
                    'Must select at least one Geographical Unit'
                )
            elif not params[11].value:
                params[11].setErrorMessage('Must specify an Output location')
        if params[1].value == self.options[4]:
            if not params[10].value:
                params[10].setErrorMessage('Must specify an output FGDB')
        # Have they downloaded all selected surveys?
        # Future warning
        # Make sure gdb isn't being specified within an exising gdb
        if (params[10].value
            and 'gdb' in arcpy.Describe(params[10].value).path):
            params[10].setErrorMessage("Can't put a gdb in an existing gdb.")
            
        return

    def execute(self, params, messages):
        """The source code of the tool."""
        # from sddt.download.query_download import main
        if params[1].value == 'By State(s)':
            option = 1
            path = params[11].valueAsText
        elif params[1].value == 'By Geography':
            option = 3
            path = params[11].valueAsText
        elif 'SSURGO' in params[1].value:
            option = 0
            path = params[10].valueAsText
        elif 'By selected features' in params[1].value:
            option = 2
            path = params[10].valueAsText
        # build CONUS
        else:
            option = 4
            path = params[10].valueAsText

        import sddt.construct.fgdb
        reload(sddt.construct.fgdb)
        gdb_l = sddt.construct.fgdb.main([
            params[0].valueAsText, # 0: input folder
            option, # 1: option
            params[2].valueAsText, # 2: survey list
            params[3].valueAsText, # 3: state list
            params[4].value, # 4: soil survey layer
            params[5].valueAsText, # 5: geography layer
            params[6].valueAsText, # 6: geography field
            params[7].valueAsText, # 7: selected geographies
            params[8].valueAsText, # 8: gdb label
            params[9].value, # 9: Clip boolean
            path, # 10: output path
            params[12].valueAsText, # 11: AOI
            params[14].value, # 12: Create Concise version boolean
            params[15].value, # 13: SSURGO version
            os.path.dirname(sddt.__file__) + "/construct" # 14: module path
        ])
        
        # 12: Create Valu1 and Dominant Component tables
        arcpy.AddMessage('\nBuilding Valu1 and Dominant Component tables.')
        if params[13].value:
            import sddt.construct.valu1
            reload(sddt.construct.valu1)
            v_success = {'both': [], 'dc': [], 'v': [], 'neither': []}
            for gdb_p in gdb_l:
                complete_b = sddt.construct.valu1.main([
                    gdb_p,
                    os.path.dirname(sddt.__file__) + "/construct"
                ])
                if complete_b:
                    valu1_b = arcpy.Exists(gdb_p + "/Valu1")
                    domcom_b = arcpy.Exists(gdb_p + "/DominantComponent")
                    if valu1_b and domcom_b:
                        v_success['both'].append(gdb_p)
                    elif not valu1_b:
                        v_success['dc'].append(gdb_p)
                    elif not domcom_b:
                        v_success['v'].append(gdb_p)
                    else:
                        v_success['neither'].append(gdb_p)
                else:
                    v_success['neither'].append(gdb_p)
            nt = '\n\t'
            if (both := v_success['both']):
                arcpy.AddMessage(
                            "Both the Valu1 and DominantCompoent tables "
                            "were successfully created for these FGDBs:\n\t"
                            f"{nt.join(both)}"
                        )
            if (dc := v_success['dc']):
                arcpy.AddWarning(
                            "The Valu1 table successfully created but "
                            "the DominantComponent table "
                            "was not successfully created for these FGDBs:"    
                        )
                arcpy.AddMessage(f"\n\t{nt.join(dc)}")
            if (v := v_success['v']):
                arcpy.AddWarning(
                            "The DominantComponent table successfully created "
                            "but the Valu1 table "
                            "was not successfully created for these FGDBs:"
                        )
                arcpy.AddMessage(f"\n\t{nt.join(v)}")
            if (neither := v_success['neither']):
                arcpy.AddWarning(
                            "Neither the DominantComponent or the Valu1 tables "
                            "were successfully created for these FGDBs:"
                        )
                arcpy.AddMessage(f"\n\t{nt.join(neither)}")

        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return


class valu1(object):
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
        import sddt.construct.valu1
        reload(sddt.construct.valu1)
        gdb_p = params[0].values
        complete_b = sddt.construct.valu1.main([
            gdb_p,
            os.path.dirname(sddt.__file__) + "/construct"
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


class rasterize(object):

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

        # paramter 1
        params.append(arcpy.Parameter(
            displayName="Soil Polygon Feature Name Pattern",
            name="mu",
            direction="Input",
            parameterType="Required",
            datatype="String"
        ))
        params[1].filter.list = []

        # paramter 2
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
        # from sddt.download.query_download import main
        import sddt.construct.rasterize_mupolygon
        reload(sddt.construct.rasterize_mupolygon)
        wksp_l = [wksp for wksp in params[0].values]
        sddt.construct.rasterize_mupolygon.main(*[
            wksp_l,
            params[1].valueAsText,
            params[2].value,
            params[3].value,
            os.path.dirname(sddt.__file__) + "/construct"
            ])
        
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
    

class aggregator(object):
    cats = dict() # SDV Folder key: SDV Category
    atts = dict() # SDV Attribute key: SDV Attribute
    cross = dict() # SDV Attribute key: SDV Folder key
    dir_paths = dict() # feature: path
    tabs = {'Component': 'component'} # table Label: Physical Name
    # Column Label: [Column Physical Name, Logical data type, Unit of measure]
    cols = dict()
    # Primary & Secondary 
        # Attribute: {Primary Value: [secondary values]}
    # Primary:
        # # Attribute: [Primary values]}
    sdv_con = dict()
    # Was an SDV attribute selected?
    sdv_b = False
    # SDV Attribute: [Entire row from SDV Attribute]
    sdv_att = {}
    states = [
        '(AK)', '(AL)', '(AR)', '(AS)', '(AZ)', '(CA)', '(CO)', '(CT)', '(DC)',
        '(DE)', '(FL)', '(GA)', '(GU)', '(HI)', '(IA)', '(ID)', '(IL)', '(IN)',
        '(KS)', '(KY)', '(LA)', '(MA)', '(MD)', '(ME)', '(MI)', '(MN)', '(MO)',
        '(MP)', '(MS)', '(MT)', '(NC)', '(ND)', '(NE)', '(NH)', '(NJ)', '(NM)',
        '(NV)', '(NY)', '(OH)', '(OK)', '(OR)', '(PA)', '(PR)', '(RI)', '(SC)', 
        '(SD)', '(TN)', '(TX)', '(UT)', '(VA)', '(VI)', '(VT)', '(WA)', '(WI)',
        '(WV)', '(WY)'
        ]
    months = [
        'January', 'February', 'March', 'April', 'May', 'June', 'July',
        'August', 'September', 'October', 'November', 'December'
    ]

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Summarize Soil Information"
        self.description = (
            "Summarizes soil information by map unit key. "
            "It provides several aggreation methods and "
            "outputs a table that can be joined to soil "
            "features and rasters."
        )
        self.category = '3) Analyze Databases'

        self.paths = dict()
        act_map = arcpy.mp.ArcGISProject("CURRENT").activeMap
        lyrs = act_map.listLayers()
        i = 0
        for lyr in lyrs:
            if lyr.isRasterLayer or lyr.isFeatureLayer:
                lyr_flds = arcpy.Describe(lyr).fields
                for fld in lyr_flds:
                    if fld.name == 'MUKEY':
                        lyr_ref = f"{lyr.name} [map: {i}]"
                        self.paths[lyr_ref] = os.path.dirname(lyr.dataSource)
                        i += 1
                        break       


    def getParameterInfo(self):
        """Define parameter definitions"""
        # parameter 0
        params = [arcpy.Parameter(
            displayName="SSURGO Feature or Raster",
            name="inputSSURGO",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            multiValue=False
        )]
        params[0].filter.list = list(self.paths.keys())

        # parameter 1
        params.append(arcpy.Parameter(
            displayName="SSURGO Database",
            name="inputFolder",
            direction="Input",
            parameterType="Required",
            datatype="DEWorkspace",
            multiValue=False
        ))
        params[1].filter.list = ["Local Database"]

        # parameter 2
        params.append(arcpy.Parameter(
            displayName="Filters",
            name="filters",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
        ))
        params[-1].filter.list = [
            "Most Common as List (all SDV)", "By Table",
             "Most Common Grouped (SDV Categories)"#, "Soil Interpretation",
             #"Soil Property", "Soil Class/Index", "State Interpretations"
        ]

        # parameter 3
        params.append(arcpy.Parameter(
            displayName="SDV Category",
            name="SDVcat1",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))

        # parameter 4
        params.append(arcpy.Parameter(
            displayName="Select Table",
            name="tables",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))
        params[-1].filter.list = list(aggregator.tabs.keys())

        # parameter 5
        params.append(arcpy.Parameter(
            displayName="Soil Attribute",
            name="attribute",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            multiValue=True,
            enabled=False
        ))

        # parameter 6
        params.append(arcpy.Parameter(
            displayName="Aggregation Method",
            name="agmeth",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))
        
        # parameter 7
        params.append(arcpy.Parameter(
            displayName="Primary Constraint",
            name="primary",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))

        # parameter 8
        params.append(arcpy.Parameter(
            displayName="Secondary Constraint",
            name="secondary",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))

        # parameter 9
        params.append(arcpy.Parameter(
            displayName="Depth Ranges (cm)",
            name="depths",
            direction="Input",
            parameterType="Optional",
            datatype="GPValueTable",
            multiValue=True,
            enabled=False
        ))
        params[-1].columns = [["GPLong", "Top"], ["GPLong", "Bottom"]]
        params[-1].filters[0].type = "Range"
        params[-1].filters[1].type = "Range"
        params[-1].filters[0].list = [0, 499]
        params[-1].filters[1].list = [1, 500]

        # parameter 10
        params.append(arcpy.Parameter(
            displayName="Beginning Month",
            name="month1",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))
        params[-1].filter.list = aggregator.months

        # parameter 11
        params.append(arcpy.Parameter(
            displayName="Ending Month",
            name="month2",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))
        params[-1].filter.list = aggregator.months

        # parameter 12
        params.append(arcpy.Parameter(
            displayName="Tie Break Rule",
            name="tie",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))
        params[-1].filter.list = ['Higher', 'Lower']

        # parameter 13
        params.append(arcpy.Parameter(
            displayName="Treat Null entries as Zero",
            name="null",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False
        ))
        params[-1].value = False

        # parameter 14
        params.append(arcpy.Parameter(
            displayName="Component Percent Cutoff",
            name="comp_pct",
            direction="Input",
            parameterType="Optional",
            datatype="GPLong",
            enabled=False
        ))

        # parameter 15
        params.append(arcpy.Parameter(
            displayName="Map Interp Fuzzy Values",
            name="fuzzy",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False
        ))
        params[-1].value = False

        # parameter 16
        params.append(arcpy.Parameter(
            displayName="Include Null Rating Values",
            name="rating_null",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False
        ))
        params[-1].value = False

        # parameter 17
        params.append(arcpy.Parameter(
            displayName="Property Range Value",
            name="range_value",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))
        params[-1].filter.list = ["Low", "Representative", "High"]
        params[-1].value = "Representative"

        return params

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        # If feature has been selected
        if (feat := params[0].value) and not params[0].hasBeenValidated:
            # set database from selected feature
            params[1].value = self.paths[feat]

        # if a database has been selected
        elif params[1].value and not params[1].hasBeenValidated:
            arcpy.env.workspace = str(params[1].value)
            lyrs2 = arcpy.ListDatasets()
            lyrs2.extend(arcpy.ListFeatureClasses())
            # Scrub feature directory
            aggregator.dir_paths.clear()
            # Create list of directory features
            for lyr in lyrs2:
                lyr_path = f"{arcpy.env.workspace}/{lyr}"
                lyr_flds = arcpy.Describe(lyr_path).fields
                for fld in lyr_flds:
                    if fld.name.lower() == 'mukey':
                        lyr_ref = f"{lyr} [dir]"
                        aggregator.dir_paths[lyr_ref] = arcpy.env.workspace
                        break
            params[0].filter.list = list(self.paths.keys()) \
                + list(aggregator.dir_paths.keys())
            # verify that selected feature is in database
            if (params[0].value 
                and set(self.paths.values())
                & set(aggregator.dir_paths.values())
                ):
                params[0].value = None

        # Don't display further optoins until database selected
        # "By Table", 
        #             "Soil Interpretation", "Soil Property", "Soil Class/Index"
        if params[1].value:
            params[2].enabled = True
            path = params[1].value
        else: # Otherwise, shut down all subsequent options
            for i in range(2, len(params)):
                params[i].enabled = False
            return
        
        # if a filter type has been selected
        if (filt := params[2].value) and not params[2].hasBeenValidated:
            if ("Most Common Grouped (SDV Categories)" == filt
                or "Most Common as List (all SDV)" == filt):
                aggregator.sdv_b = True
                params[4].enabled = False
                # prime values once
                if not aggregator.cats:
                    # Get SDV Categories
                    db_p = f"{path}/sdvfolder"
                    with (SearchCursor(
                        db_p, ['foldername', 'folderkey'],
                        sql_clause=[None, "ORDER BY foldersequence ASC"]) 
                    as sCur):
                        aggregator.cats.update(dict(sCur))

                    # Get key cross-walk
                    db_p = f"{path}/sdvfolderattribute"
                    with (SearchCursor(
                        db_p, ['folderkey', 'attributekey'],
                        sql_clause=[None, "ORDER BY folderkey ASC"]
                        )
                    as sCur):
                        # folder key: [(folder key, attribute key), ...]
                        aggregator.cross.update({
                            fk: list(zip(*ak))[1]
                            for fk, ak in groupby(sCur, byKey)
                        })
                    # Get SDV Attributes
                    db_p = f"{path}/sdvattribute"
                    with (SearchCursor(
                        db_p, ["attributekey", "attributename"],# 'attributetablename', 'attributelogicaldatatype'],
                        sql_clause=[None, "ORDER BY attributekey ASC"]) 
                    as sCur):
                        aggregator.atts.update(dict(sCur))

                params[3].filter.list = list(aggregator.cats.keys())
                if "Most Common Grouped (SDV Categories)" == filt:
                    params[3].enabled = True # turn on SDV Category
                    params[4].enabled = False # Turn off Select Table
                    params[5].enabled = False # Turn off Soil Attributes
                    params[3].value = None
                else:
                    params[3].enabled = False # turn off SDV Category
                    params[4].enabled = False # Turn off Select Table
                    params[5].enabled = True # Turn on Soil Attributes
                    params[5].filter.list = list(aggregator.atts.values())
                    params[5].value = None

            # if a Table filter option selected
            elif "By Table" == filt:
                params[3].enabled = False # turn off SDV Category
                params[4].enabled = True
                params[5].enabled = False # Turn off Soil Attributes
                # Without a selection being made, validation isn't 
                # triggered and Soil Attributes won't be displayed
                params[4].value = None  
                aggregator.sdv_b = False

            for i in range(6, len(params)):
                params[i].enabled = False

        # if a SDV category selected
        if (cat := params[3].value) and not params[3].hasBeenValidated:
            fold_k = aggregator.cats[cat]
            att_keys = aggregator.cross[fold_k]
            params[5].enabled = True
            params[5].filter.list = [aggregator.atts[ak] for ak in att_keys]
            params[5].value = None

        # if a Table has been selected
        if (table_lab := params[4].value) and not params[4].hasBeenValidated:
            table = aggregator.tabs[table_lab]
            # prime values once
            if not aggregator.cols.get(table):
                path = params[1].value
                # Get SDV Categories
                db_p = f"{path}/mdstattabcols"
                with (SearchCursor(
                    db_p, 
                    ['collabel', 'colphyname', 'logicaldatatype', 'uom'],
                    where_clause=f"tabphyname = '{table}'",
                    sql_clause=[None, "ORDER BY colsequence ASC"]) 
                as sCur):
                    aggregator.cols.update({table: {
                        col[0].replace(' - Representative Value', ''): col[1:] 
                        for col in sCur 
                        if (col[1][-2:] != '_l') and (col[1][-2:] != '_h')
                    }})
            # remove last two key fields
            params[5].filter.list = list(aggregator.cols.keys())[:-2]
            params[5].enabled = True
            params[5].value = None

        # if a Soil Attribute has been selected
        if not params[5].hasBeenValidated and (att := params[5].values[0]):
            if aggregator.sdv_b:
                # If SDV, read in more details from SDV attribute table
                # Get SDV Attributes
                if not aggregator.sdv_att.get(att):
                # strings with spaces were getting double bagged with ''
                    q = f"attributename = '{att}'"
                    q = q.replace("''", "'")
                    db_p = f"{path}/sdvattribute"
                    with (SearchCursor(
                        db_p, "*",
                        where_clause=q
                    ) 
                    as sCur):
                        # aggregator.sdv_att.clear()
                        aggregator.sdv_att.update({
                            att: dict(zip(sCur.fields, next(sCur)))
                        })
                table = aggregator.sdv_att[att]['attributetablename']

                # Primary & Secondary Constraints
                if ((p_col := aggregator.sdv_att[att]["primaryconcolname"])
                    and (s_col := aggregator.sdv_att[att]["secondaryconcolname"])):
                    prim_d = aggregator.sdv_con.get(str(att))
                    if not prim_d:
                        db_p = f"{path}/{table}"
                        with SearchCursor(
                            db_p, [p_col, s_col]
                        ) as sCur:
                            aggregator.sdv_con.update({att:{
                                veg: list(zip(*unit))[1]
                                for veg, unit in groupby(sCur, byKey)
                            }})
                        prim_d = aggregator.sdv_con.get(att)

                    params[7].filter.list = sorted(prim_d.keys())
                    params[7].enabled = True
                    crop = params[7].filter.list[0]
                    params[7].value = crop
                    params[8].filter.list = sorted(prim_d[crop])
                    params[8].enabled = True
                    unit = params[8].filter.list[0]
                    params[8].value = unit

                # Primary Constraints Only
                elif (p_col := aggregator.sdv_att[att]["primaryconcolname"]):
                    prim_d = aggregator.sdv_con.get(att)
                    if not prim_d:
                        db_p = f"{path}/{table}"
                        with SearchCursor(
                            db_p, p_col
                        ) as sCur:
                            # There are restrictions that are not defined
                            prim_l = list({str(p) for p, in sCur})
                            if 'None' in prim_l:
                                prim_l.remove('None')
                                prim_l.append('Not Specified')
                            aggregator.sdv_con.update({att: prim_l})

                    params[7].filter.list = sorted(prim_l)
                    params[7].enabled = True
                    # Set default constraining for eco-sites
                    if (params[5].value 
                        in ['Ecological Site ID', 'Ecological Site Name'] 
                        and 'NRCS Rangeland Site' in prim_l):
                        self.params[7].value = 'NRCS Rangeland Site'
                    else:
                        feat = params[7].filter.list[0]
                        params[7].value = feat
                    params[8].filter.list = []
                    params[8].value = None
                    params[8].enabled = False
                    
                else:
                    params[7].filter.list = []
                    params[7].value = None
                    params[7].enabled = False
                    params[8].filter.list = []
                    params[8].value = None
                    params[8].enabled = False

                # Tiebreaker Parameter (is this SDV specific?)
                if aggregator.sdv_att[att]["tiebreakrule"] == -1:
                    aggregator.sdv_att[att]["tiebreakrule"] = 0
                    params[12].enabled = False
                else:
                    params[12].enabled = True
            if aggregator.sdv_b:
                dSDV = aggregator.sdv_att[att]
            else:
                dSDV = None

            # Set Aggregation Method
            # SDV attribute and Percent Present algorithm
            params[6].enabled = True # Turn on aggregation method
            if (aggregator.sdv_b
                and dSDV["algorithmname"] == "percent present"):
                params[6].filter.list = ["Percent Present"]
                params[15].enabled = False # Turn off Fuzzy
                params[16].enabled = False # Turn off Null rating
                params[17].enabled = False # Turn off RV
            # SDV attribute and No Aggregation Necessary
            elif (aggregator.sdv_b 
                and dSDV["algorithmname"] == "No Aggregation Necessary"):
                params[6].filter.list = "No Aggregation Necessary"
                for i in range(7, len(params)):
                    params[i].enabled = False
            # An interpretation
            elif table == 'cointerp':
                params[15].enabled = True # Turn on Fuzzy
                params[16].enabled = True # Turn on Null rating
                params[17].enabled = False # Turn off RV
                if not params[15].value:
                    if (aggregator.sdv_b and 
                        dSDV["attributename"] != dSDV["nasisrulename"]):
                        params[6].filter.list = [
                            "Dominant Condition", "Dominant Component",
                            "Least Limiting", "Most Limiting"
                        ]
                        params[6].value = dSDV["algorithmname"]
                    else:
                        params[6].filter.list = [
                            "Dominant Condition", "Dominant Component"
                        ]
                else:
                    if (aggregator.sdv_b and 
                        dSDV["attributename"] != dSDV["nasisrulename"]):
                        params[6].filter.list = [
                            "Weighted Average", "Least Limiting", 
                            "Most Limiting", "Dominant Component"
                        ]
                        params[6].value = dSDV["algorithmname"]
                    else:
                        params[6].filter.list = [
                            "Weighted Average", "Dominant Component"
                        ]
                    params[6].value = "Weighted Average"

            # Numeric Soil Attributes
            elif (aggregator.sdv_b and dSDV["effectivelogicaldatatype"] 
                  in ["integer", "float"]):
                params[17].enabled = True # Turn on hi/rv/lo
                params[16].enabled = False # Turn off Null rating
                params[15].enabled = False # Turn off Interp Fuzzy Values
                
                # Horizons level
                if dSDV["horzlevelattribflag"] == 1:
                    params[6].filter.list = [
                        "Dominant Component", "Minimum or Maximum", 
                        "Weighted Average"
                    ]
                    params[6].value = dSDV["algorithmname"]
                # Component level
                elif dSDV["complevelattribflag"] == 1:
                    params[6].filter.list = [
                        "Dominant Condition", "Dominant Component", 
                        "Minimum or Maximum", "Weighted Average"
                    ]
                    params[6].value = dSDV["algorithmname"]
            # Mapunit level
            elif aggregator.sdv_b and dSDV["mapunitlevelattribflag"] == 1:
                params[6].filter.list = ["No Aggregation Necessary"]
                for i in range(7, len(params)):
                    params[i].enabled = False
            elif aggregator.sdv_b and dSDV["tiebreakdomainname"]:
                params[6].filter.list = [
                    "Dominant Condition", "Dominant Component",
                    "Minimum or Maximum"
                ]
                params[6].value = dSDV["algorithmname"]
            else:
                params[6].filter.list = [
                    "Dominant Condition", "Dominant Component"
                ]
                params[3].value = dSDV["algorithmname"]

            # Set for all SDV or Table options
            if str(table).startswith('ch'):
                params[9].enabled = True
            else:
                params[9].enabled = False
            
            





            # Table specific tasks
            # Read in table columns



        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, params, messages):
        """The source code of the tool."""
    # If Dominant Component, call Valu1 table
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
