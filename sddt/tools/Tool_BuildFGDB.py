#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Create gSSURGO File Geodatabase
Intended for the Soil Data Development Toolbox for ArcGIS Pro

This tool imports a selection of downloaded SSURGO packages, as downloaded
from WSS, into a templated ESRI File Geodatabase


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

import re
import os
import inspect

from importlib import reload
from .. import construct
reload(construct)

import arcpy
from arcpy.da import SearchCursor

from ..construct.fgdb import main as fgdb
from ..construct.valu1 import main as valu1


class BuildFGDB(object):
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

        # parameter 1
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
            displayName="Concise cointerp table (recommended)",
            name="concise_b",
            direction="Input",
            parameterType="Required",
            datatype="GPBoolean",
            enabled=True
        ))
        params[14].value = True

        # parameter 15
        params.append(arcpy.Parameter(
            displayName="Single Part MUPOLYGON",
            name="diss_b",
            direction="Input",
            parameterType="Required",
            datatype="GPBoolean",
            enabled=True
        ))
        params[-1].value = True

        # parameter 16
        params.append(arcpy.Parameter(
            displayName="gSSURGO Version",
            name="gSSURGO_v",
            direction="Input",
            parameterType="Required",
            datatype="String",
            enabled=True
        ))
        params[-1].filter.type = "ValueList"
        params[-1].filter.list = ["gSSURGO traditional", "gSSURGO 2.0"]

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
        if params[10].enabled and (gdb_p := params[10].valueAsText):
            gdb = os.path.basename(gdb_p).strip('.gdb') 
            if 'gdb' in arcpy.Describe(params[10].value).path:
                params[10].setErrorMessage(
                    "Can't put a gdb in an existing gdb."
                )
            elif not re.match('^\w+$', gdb):
                params[10].setErrorMessage(
                    "GDB name can only have alphanumeric, can include _"
                )
        return

    def execute(self, params, messages):
        arcpy.AddMessage(f"Tool_BuildFGDB version: {version}")
        
        # import sddt.construct.fgdb
        # reload(sddt.construct.fgdb)
        # import sddt.construct.valu1
        # reload(sddt.construct.valu1)
        # from sddt.construct.fgdb import main as fgdb
        # from sddt.construct.valu1 import main as valu1
        """The source code of the tool."""
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

        gdb_l = fgdb([
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
            params[14].value, # 12: Create Concise cointerp boolean
            params[15].value, # 13: Single Part MUPOLYGON boolean
            params[16].value, # 14: SSURGO version
            os.path.dirname(inspect.getfile(fgdb)) # 14: module path
        ])
        
        # 12: Create Valu1 and Dominant Component tables
        arcpy.AddMessage('\nBuilding Valu1 and Dominant Component tables.')
        if params[13].value:
    
            v_success = {'both': [], 'dc': [], 'v': [], 'neither': []}
            for gdb_p in gdb_l:
                complete_b = valu1([
                    gdb_p,
                    os.path.dirname(inspect.getfile(fgdb))
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
                            "Both the Valu1 and DominantComponent tables "
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
