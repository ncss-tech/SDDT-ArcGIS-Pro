#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_Infeat: Secondary Table
Parameter for Summarize Soil Information tool

A filter parameter to select a secondary SSURGO table from which an attribute
can be selected.


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


class Param_SecTab():
    def __init__(self):
        self.tab_lab = None
        self.error = None

        self.param = arcpy.Parameter(
            displayName="Secondary Table",
            name="sectab",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False,
            category="Secondary"
        )


    def update(self, tab_lab, cols, prim_tab_lab):
        try:
            params_d = {}
            if tab_lab != self.tab_lab:
                self.tab = tab_lab

            if(prim_tab_lab in ('Component Crop Yield: Irrigated', 
                            'Component Crop Yield: Nonirrigated')):
                return params_d

            # Get attributes with domains
            atts = [k for k, v in cols.items() if v[5]]

            params_d[10] = [True, None, '*', atts]
            params_d[11] = [False, None, '*', []]

            self.error = None
            return params_d
        except:
            self.error = pyErr('Param_SecTab')
            return {}