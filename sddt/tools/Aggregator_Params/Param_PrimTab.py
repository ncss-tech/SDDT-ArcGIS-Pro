#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_Infeat: Primary Table
Parameter for Summarize Soil Information tool

A filter parameter to select a primary SSURGO table from which an attribute
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


class Param_PrimTab():
    def __init__(self):
        self.tab_lab = '' 
        self.tabs = {
        'Component': 'component', 'Horizon': 'chorizon', 
        'Interpretations': 'cointerp', 'Flooding & Ponding': 'comonth',
        'Component Crop Yield: Irrigated': 'cocropyld',
        'Component Crop Yield: Nonirrigated': 'cocropyld'
        } 
        self.error = None

        self.param = arcpy.Parameter(
            displayName="Primary Table",
            name="primtab",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        )
        self.param.filter.list = list(self.tabs.keys())


    def update(self, tab_lab, cols, doms):
        try:
            if tab_lab != self.tab_lab:
                self.tab = tab_lab

            tab_n = self.tabs[tab_lab]
            params_d = {5: [True, None, None, []]}
            
            if tab_lab == 'Interpretations':
                # Set filter list to available interps
                params_d[5][3] = list(cols[tab_n].keys())
                params_d['ALL_OFF'] = 6

            elif tab_lab == 'Flooding & Ponding':
                flds = ['Flooding Frequency', 'Ponding Frequency']
                params_d[5] = [True, None, '*', flds]
                params_d[13] = [False, '*', '*', '*']
                # params_d['ALL_OFF'] = 6

            elif(tab_lab in ('Component Crop Yield: Irrigated', 
                            'Component Crop Yield: Nonirrigated')):
                col_prop = cols[tab_n]['Crop Name']

                col_dom = col_prop[5]
                dom_l = sorted(doms[col_dom])
                plants = list(list(zip(*dom_l))[1])

                meth_l = ["Weighted Average"]

                params_d[5] = [True, 'Crop Name', '*', ['Crop Name',]]
                params_d[6] = [True, 'All Components', '*', '*']
                params_d[8] = [True, None, None, plants]
                params_d[7] = [True, meth_l[0], '*', meth_l]
                params_d[9] = [True, tab_lab, '*', [tab_lab,]]
                params_d[10] = [True, 'Units', '*', ['Units',]]
                params_d[11] = [True, None, '*', []]
                params_d['ALL_OFF'] = 11

            else:
                params_d['ALL_OFF'] = 6
                params_d[5][3] = list(cols[tab_n].keys())

            self.error = None
            return params_d
        except:
            self.error = pyErr('Param_PrimTab')
            return {}
