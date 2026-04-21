#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_Infeat: Choice List Filter 
Parameter for Summarize Soil Information tool

This parameter collates avialble soil properties and interps into general
categories


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 03/23/2026
    @by: Alexnder Stum
@version 1.1.1

# --- Update, v. 1.1.1
- Added 'INTERP_OFF' option 
If a new DB is selected and interps was selected, flush out interps as 
interps can be DB specific.
# --- Update, v. 1.1
- Tweaked to fix dead end if user selected another DB


"""
import arcpy

from ... import pyErr


class Param_Filter():
    def __init__(self):
        self.filt = "By Table"
        self.filters = ["By Table"]
        self.error = None

        #"Most Common as List (all SDV)", 
        # "Most Common Grouped (SDV Categories)",
        # "All Fields"
        self.param = arcpy.Parameter(
            displayName="Choice List Filter",
            name="filter",
            direction= "Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        )

        #'All', 'Soil Data Viewer Categories', 'Soil Data Viewer List'
        self.param.filter.list = self.filters
        self.param.value = self.filters[0]

    
    def update(self, filt):
        try:
            if filt != self.filt:
                self.filt = filt
            if self.filt == "By Table":
                params_d = {'INTERP_OFF': 0}
                params_d[2] = [True, "By Table", '*', ["By Table"]]
                params_d[4] = [True, '*', '*', '*']
                return params_d
            # elif ("Most Common Grouped (SDV Categories)" == filt
            #         or "Most Common as List (all SDV)" == filt):

            self.error = None
        except:
            self.error = pyErr('Param_Filter')
            # return {}