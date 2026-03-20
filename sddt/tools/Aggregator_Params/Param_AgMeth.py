#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_AgMeth: Aggregation Method
Parameter for Summarize Soil Information tool

Identify method to be used to aggrgeate soil information


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


class Param_AgMeth():
    def __init__(self):
        self.method = ''
        self.error = None

        self.param = arcpy.Parameter(
            displayName="Aggregation Method",
            name="agmeth",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        )


    def update(self, method, tab_lab, tab_n, dom_n, doms):
        try:
            param_d = {}

            if method != self.method:
                self.method = method

            if method == "Dominant Component":
                # Percent cutoff not relavent
                param_d[14] = [False, '*', '*', '*']
                param_d[19] = [False, '*', '*', '*']
            else:
                param_d[14] = [True, '*', '*', '*']
                param_d[19] = [True, '*', '*', '*']

            if(tab_lab in ('Component Crop Yield: Irrigated', 
                            'Component Crop Yield: Nonirrigated')):
                # Already established by Param_PrimTab
                return
            if method == "Dominant Condition":
                # Tiebreak relavent
                param_d[13] = [True, '*', '*', '*']
            else:
                param_d[13] = [False, '*', '*', '*']
            
            if method == "Percent Present":
                dom_l = sorted(doms[dom_n])
                col_dom2 = [choice for _, choice in dom_l]
                param_d[6] = [True, None, '*', col_dom2]
                param_d[20] = [True, '*', '*', '*']
            else:
                param_d[6] = [False, None, '*', []]
                param_d[20] = [False, '*', '*', '*']

            # Horizon tables, provide option find absolute max/min value
            if(tab_n.startswith('ch') 
            and ((method == "Maximum") or (method == "Minimum"))):
                param_d[22] = [True, '*', '*', '*']
            else:
                param_d[22] = [False, '*', '*', '*']

            # Map Interp Fuzzy Values parameter
            if(tab_lab == 'Interpretations' 
            and method in ("Least Limiting", "Most Limiting")):
                param_d[16] = [True, '*', '*', '*']
            else:
                param_d[16] = [False, '*', '*', '*']
            
            self.error = None
            return param_d
        except:
            self.error = pyErr('Param_AgMeth')
            return {}