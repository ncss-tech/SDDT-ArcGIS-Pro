#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Join Aggregated Summaries
Intended for the Soil Data Development Toolbox for ArcGIS Pro

Join (~ full outer join) any map unit level tables on mukey 
into a new column bound table. Intended to join outputs from the Summarize
Soil Information tool

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 02/09/2026
    @by: Alexnder Stum
@version: 0.1

# --- Update

"""
v = "0.1"

import arcpy

from ..manage.join import main as join

class Join(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Join Aggregated Summaries"
        self.description = (
            "Join (~ full outer join) any map unit level tables on mukey "
            "into a new column bound table. Intended to join outputs from "
            "the Summarize Soil Information tool"
        )
        self.category = '4) Manage Databases'


    def getParameterInfo(self):
        """Define parameter definitions"""
        params = [arcpy.Parameter(
            displayName="Input tables to be joined on mukey",
            name="inputTables",
            direction="Input",
            parameterType="Required",
            datatype="GPTableView",
            multiValue=True)]
        
        params.append(arcpy.Parameter(
            displayName="Output table",
            name="outputTable",
            direction="Output",
            parameterType="Required",
            datatype="DETable"
        ))

        return params


    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
                
        return
    

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""

        params[0].clearMessage()
        if (tabs := params[0].values) and not params[0].hasBeenValidated:
            tab_str = params[0].valueAsText
            tabs = tab_str.split(';')
            sans_mk = []
            for i, tab in enumerate(tabs):
                d = arcpy.Describe(tab)
                no_mukey = True
                for f in d.fields:
                    if 'mukey' in f.name.lower():
                        no_mukey = False
                if no_mukey:
                    sans_mk.append(tabs[i])
            if sans_mk:
                params[0].setErrorMessage(
                    f"Input table(s) missing an mukey field: {sans_mk}"
                )
        
        return


    def execute(self, params, messages):
        """The source code of the tool."""
        arcpy.AddMessage(f"Tool_Join {v=}")
        # import sddt.manage.join
        # reload(sddt.manage.join)
        # from sddt.manage.join import main as join
        join([params[0].values, params[1].value])
        return


    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return