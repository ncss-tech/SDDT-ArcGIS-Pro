#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_PrimAtt: Primary Soil Attribute
Parameter for Summarize Soil Information tool

Identify the primary attribute that will be summarized


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


class Param_PrimAtt():
    def __init__(self):
        self.att = ''
        self.col_prop = []
        # primary table: [available secondary tables]
        self.s_tabs = {
            'Component': ['Component',],
            'Horizon': ['Horizon', 'Component']
            #diagnostic horizons, landform, geomorph, ecosite, soil moisture
        }
        self.error = None

        self.param = arcpy.Parameter(
            displayName="Primary Soil Attribute (select one)",
            name="primatt",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            # Set True to enable searching, but only 1 choice allowed
            multiValue=True,
            enabled=False
        )
    

    def update(self, param, tab_lab, tab_n, cols, RV):
        try:
            param_d = {}

            # Already been set Crop Name
            if(tab_lab in ('Component Crop Yield: Irrigated', 
                            'Component Crop Yield: Nonirrigated')):
                self.att = 'Crop Name'
                return param_d

            param_d[6] = [True, '*', '*', '*']
            # constraning to one selection
            vl = param.values
            vt = param.value
            if 'table' in str(type(vt)).lower():
                rc = vt.rowCount
            elif vt:
                rc = len(vt)
            else:
                rc = 0
            if 'table' in str(type(vl)).lower():
                lc = vl.rowCount
            elif vl:
                lc = len(vl)
            else:
                lc = 0

            # # if no selections or more than 1
            # if rc + lc != 1:
            #     return {'ALL_OFF': 6}

            # if row count and param 5 is a value table
            if rc and 'table' in str(type(vt)).lower():
                att = vt.getValue(0, 0)
            # if list and param 5 is a value table
            elif lc and 'table' in str(type(vl)).lower():
                att = vl.getValue(0, 0)
            # if not value table but something in list
            elif lc:
                att = vl[0]
            elif vt:
                att = vt[0]
            # else:
            #     att = vt
            
            if att != self.att:
                self.att = att
            # else:
            #     # Everything should be set
            #     return {}
            
            col_prop = cols[tab_n][att]
            dtype = col_prop[1]
            itype = col_prop[2]
            col_dom = col_prop[5]

            if tab_lab ==  'Interpretations':
                if itype == 'class':
                    method_l = ["Dominant Condition"]
                    # method_l = ["Dominant Condition", "Dominant Component"]
                else:
                    # method_l = ["Dominant Component", "Dominant Condition",
                    #     "Least Limiting", "Most Limiting", "Weighted Average"]
                    method_l = ["Dominant Condition",
                        "Least Limiting", "Most Limiting", "Weighted Average"]
                param_d[7] = [False, None, '*', method_l]
                # Turn off constraints and secondary
                param_d[8] = [False, '*', '*', '*']
                param_d[9] = [False, '*', '*', '*']
                param_d[10] = [False, '*', '*', '*']
                param_d[11] = [False, '*', '*', '*']
                param_d[12] = [False, '*', '*', '*']
                param_d[13] = [False, '*', '*', '*']
                param_d[16] = [False, '*', '*', '*']
                param_d['ALL_OFF'] = 20

                return param_d
            # Floding and Ponding
            elif tab_lab == 'Flooding & Ponding':
                method_l = ['Dominant Condition', 'Median Frequency',
                            'Highest Frequency', 'Lowest Frequency', 
                            'Percent Present', 'Frequency Count']
                 
                param_d[7] = [False, None, '*', method_l]
                param_d[8] = [False, None, '*', '*']
                param_d['ALL_OFF'] = 9

            # If Integer w/ unit or Float
            elif(((dtype == "Integer") and itype) 
            or (dtype == "Float")):
                # method_l = ["Dominant Component", "Minimum", "Maximum", 
                #             "Weighted Average"]
                method_l = ["Weighted Average", "Minimum", "Maximum"]
                if tab_n.startswith('ch'):
                    method_l += ["Absolute Minimum", "Absolute Maximum"]
                param_d[7] = [False, None, '*', method_l]
                # Turn on secondary options. May need to limit this
                param_d[8] = [False, '*', '*', '*']
                param_d[9] = [True, None, '*', self.s_tabs[tab_lab]]
                param_d[10] = [False, '*', '*', '*']
                param_d[11] = [False, '*', '*', '*']
                param_d[13] = [False, '*', '*', '*']
                param_d[20] = [False, '*', '*', '*']
                param_d[21] = [False, '*', '*', '*']

            # If there is a domain
            elif col_dom:
                # method_l = ["Dominant Condition", "Dominant Component", 
                #     "Percent Present"]
                method_l = ["Dominant Condition", "Percent Present"]
                param_d[7] = [False, None, '*', method_l]
                param_d[8] = [False, '*', '*', '*']
                param_d[9] = [False, '*', '*', '*']
                param_d[10] = [False, '*', '*', '*']
                param_d[11] = [False, '*', '*', '*']
                param_d[13] = [False, '*', '*', '*']
                param_d[20] = [False, '*', '*', '*']
                param_d[21] = [False, '*', '*', '*']

            elif att:
                method_l = ["Dominant Condition"]
                param_d[7] = [False, "Dominant Condition", '*', method_l]
                param_d[8] = [False, '*', '*', '*']
                param_d[9] = [False, '*', '*', '*']
                param_d[10] = [False, '*', '*', '*']
                param_d[11] = [False, '*', '*', '*']
                param_d[13] = [False, '*', '*', '*']
                param_d[20] = [False, '*', '*', '*']
                param_d[21] = [False, '*', '*', '*']
            else:
                return {'ALL_OFF': 6}

            if tab_n.startswith('ch'):
                param_d[12] = [True, '*', '*', '*']
            else:
                param_d[12] = [False, '*', '*', '*']
            if RV.get(att):
                param_d[16] = [True, '*', '*', '*']
            else:
                param_d[16] = [False, '*', '*', '*']

            self.error = None
            return param_d
        except:
            self.error = pyErr('Param_PrimAtt')
            return {}
