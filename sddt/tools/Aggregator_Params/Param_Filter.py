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
@modified 03/10/2026
    @by: Alexnder Stum
@version 1.0



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
                params_d = {i: [False, '*', '*', '*'] for i in range(5, 23)}
                params_d[4] = [True, '*', '*', '*']
                return params_d
            # elif ("Most Common Grouped (SDV Categories)" == filt
            #         or "Most Common as List (all SDV)" == filt):

            self.error = None
        except:
            self.error = pyErr('Param_Filter')
            # return {}