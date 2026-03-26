#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_ComType: Which Components
Parameter for Summarize Soil Information tool

This parameter determines which components will be used: Dominant Component,
Major Components, or All Components


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 03/24/2026
    @by: Alexnder Stum
@version 1.0



"""
import arcpy

from ... import pyErr


class Param_ComType():
    def __init__(self):
        self.error = None
        self.comtype = None
        self.master_method = []
        self.att = None

        self.param = arcpy.Parameter(
            displayName="Which Components",
            name="comtype",
            direction="Input",
            parameterType="Required",
            datatype="GPString",
            enabled=False
        )
        self.param.filter.list = ["All Components", "Dominant Component", 
                                  "Major Components"]
        
    def update(self, comtype, method_l, att):
        try:
            param_d = {}
            # User selected a new attribute, refresh master
            if att != self.att:
                self.att = att
                self.master_method = method_l.copy()
            # otherwise reset method list
            else:
                method_l = self.master_method.copy()

            if comtype != self.comtype:
                self.comtype = comtype
            
            if self.comtype == "Dominant Component":
                # Percent cutoff not relavent
                param_d[15] = [False, '*', '*', '*']
                # carve out exception for month tables
                if "Absolute Minimum" in method_l:
                    method_l = ["Weighted Average", 
                                "Absolute Minimum", "Absolute Maximum"]
                    param_d[7] = [True, None, '*', method_l]
                else:
                    method_l = []
                    param_d[7] = [False, None, '*', method_l]
            else:
                param_d[7] = [True, None, '*', method_l]
                param_d[15] = [True, 0, '*', '*']

            self.error = None
            return param_d
        except:
            self.error = pyErr('Param_ComType')
            return {}