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

            # if method == "Dominant Component":
            #     # Percent cutoff not relavent
            #     param_d[14] = [False, '*', '*', '*']
            #     param_d[19] = [False, '*', '*', '*']
            # else:
            #     param_d[14] = [True, '*', '*', '*']
            #     param_d[19] = [True, '*', '*', '*']

            if(tab_lab in ('Component Crop Yield: Irrigated', 
                            'Component Crop Yield: Nonirrigated')):
                # Already established by Param_PrimTab
                return
            if tab_lab == 'Flooding & Ponding':
                if 'flooding' in dom_n:
                    dom_n2 = 'flooding_duration_class'
                    fld2 = 'Flooding Duration'
                else:
                    dom_n2 = 'ponding_duration_class'
                    fld2 = 'Ponding Duration'
                dom_l = sorted(doms[dom_n2])
                col_dom2 = [choice for _, choice in dom_l]

                param_d[10] = [False, None, '*', [fld2]]
                param_d[11] = [False, None, '*', col_dom2]
                param_d[13] = [True, '*', '*', '*']

                if method in ['Percent Present', 'Frequency Count']:
                    param_d[9] = [True, None, '*', ['Flooding & Ponding']]
                else:
                    param_d[9] = [False, None, '*', ['Flooding & Ponding']]

                if method in ["Dominant Condition", "Median Frequency"]:
                    param_d[14] = [True, 'Higher', '*', '*']
                else:
                    param_d[14] = [False, '*', '*', '*']
                    
            if method in ["Dominant Condition", "Median Frequency"]:
                # Tiebreak relavent
                param_d[14] = [True, 'Higher', '*', '*']
            else:
                param_d[14] = [False, '*', '*', '*']
            
            if method in ["Percent Present", "Frequency Count"]:
                dom_l = sorted(doms[dom_n])
                col_dom2 = [choice for _, choice in dom_l]
                param_d[8] = [True, None, '*', col_dom2]
                param_d[20] = [True, '*', '*', '*']
            else:
                param_d[8] = [False, None, '*', []]
                param_d[20] = [False, '*', '*', '*']

            # # Horizon tables, provide option find absolute max/min value
            # if(tab_n.startswith('ch') 
            # and ((method == "Maximum") or (method == "Minimum"))):
            #     param_d[22] = [True, '*', '*', '*']
            # else:
            #     param_d[22] = [False, '*', '*', '*']

            # Map Interp Fuzzy Values parameter
            if(tab_lab == 'Interpretations' 
            and method in ("Least Limiting", "Most Limiting")):
                param_d[17] = [True, '*', '*', '*']
            else:
                param_d[17] = [False, '*', '*', '*']
            
            self.error = None
            return param_d
        except:
            self.error = pyErr('Param_AgMeth')
            return {}