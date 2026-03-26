#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_SecAtt: Secondary Soil Attribute
Parameter for Summarize Soil Information tool

Identify the secondary attribute by which the primary contraint will be
summarized by. Or in the case of Crop Yield, the units by which the primary
constratint will be summarized by


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


class Param_SecAtt():
    def __init__(self):
        self.sec_att = None
        self.error = None

        self.param = arcpy.Parameter(
            displayName="Secondary Soil Attribute",
            name="sattribute",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False,
            category="Secondary"
        )


    def update(self, sec_att, cols, doms, prim_tab_lab):
        try: 
            params_d = {}
            if sec_att != self.sec_att:
                self.sec_att = sec_att

            if(prim_tab_lab in ('Component Crop Yield: Irrigated', 
                           'Component Crop Yield: Nonirrigated')):
                # params_d[10] = [True, '*', '*', crp_units]
                self.error = None
                return params_d

            col_prop = cols[sec_att]
            dom_n = col_prop[5]
            dom_l = sorted(doms[dom_n])
            col_dom2 = [choice for _, choice in dom_l]
            params_d[11] = [True, None, '*', col_dom2]
            params_d[21] = [True, False, '*', '*']
        
            self.error = None
            return params_d
        except:
            self.error = pyErr('Param_SecAtt')
            return {}