#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_Infeat: SDV Category
Parameter for Summarize Soil Information tool

This parameter is a sub-filter option when SDV Category filter is selected


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 03/10/2026
    @by: Alexnder Stum
@version 0.0

#### DRAFT ####

"""
import arcpy
from itertools import groupby

from ... import byKey

class Param_SDVCat():
    def __init__(self):
         # Primary & Secondary 
            # Attribute: {Primary Value: [secondary values]}
        # Primary:
            # # Attribute: [Primary values]}
        self.sdv_con = dict()
        # SDV Attribute: [Entire row from SDV Attribute]
        self.sdv_att = {}

        self.cats = dict() # SDV Folder key: SDV Category
        self.atts = dict() # SDV Attribute key: SDV Attribute
        self.cross = dict() # SDV Attribute key: SDV Folder key
        self.param = arcpy.Parameter(
            displayName="SDV Category",
            name="SDVCat",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        )


    def updateSDV(self, params, att):
        """SDV Filter was selected and SDV options need to be populated"""
        filt = params[2].value
        params[4].enabled = False

        params[3].filter.list = list(self.cats.keys())
        if "Most Common Grouped (SDV Categories)" == filt:
            params[3].enabled = True # turn on SDV Category
            params[4].enabled = False # Turn off Select Table
            params[5].enabled = False # Turn off Soil Attributes
            params[3].value = None
        else:
            params[3].enabled = False # turn off SDV Category
            params[4].enabled = False # Turn off Select Table
            params[5].enabled = True # Turn on Soil Attributes
            params[5].filter.list = sorted(list(
                self.atts.values()
            ))
            params[5].value = None

        def updateAttribute_sdv(self, params):
            params[5].values.clear()
            att = params[5].values[-1]
            params[5].value = att
        
        self.updateSDV_Aggregation(params=params)
        dSDV = self.sdv_att[att]

        # Set Aggregation Method
        # SDV attribute and Percent Present algorithm
        params[7].enabled = True # Turn on aggregation method
        if (dSDV["algorithmname"] == "percent present"):
            params[7].filter.list = ["Percent Present"]
            params[7].value = "Percent Present"
            params[17].enabled = False # Turn off Fuzzy
            params[18].enabled = False # Turn off Null rating
            params[16].enabled = False # Turn off RV
        # SDV attribute and No Aggregation Necessary
        elif (dSDV["algorithmname"] == "No Aggregation Necessary"):
            for i in range(6, len(params)):
                params[i].enabled = False
        # An interpretation
        # elif self.tabs[params[4].value] == 'cointerp':
        #     # Leaving off the Map Interp Fuzzy Values off for now
        #     params[16].enabled = False # Turn on Fuzzy
        #     # inactive for now
        #     params[17].enabled = False # Turn on Null rating
        #     params[15].enabled = False # Turn off RV
        #     ruledesign = dSDV['ruledesign']
        #     if ruledesign == 3:
        #         # These interpretation types are class indices
        #         # Weighted average isn't appropriate
        #         params[6].filter.list = [
        #             "Dominant Condition", "Dominant Component",
        #             "Least Limiting", "Most Limiting"
        #         ]
        #         params[6].value = dSDV["algorithmname"]
        #     else:
        #         # ruledesign 1 and 2
        #         params[6].filter.list = [
        #             "Dominant Condition", "Dominant Component",
        #             "Least Limiting", "Most Limiting", "Weighted Average"
        #         ]
        #         params[6].value = dSDV["algorithmname"]

        # Numeric Soil Attributes
        elif dSDV["effectivelogicaldatatype"] in ["Integer", "Float"]:
            params[16].enabled = True # Turn on hi/rv/lo
            params[16].value = 'Representative'
            params[18].enabled = False # Turn off Null rating
            params[17].enabled = False # Turn off Interp Fuzzy Values
            
            # Horizons level
            if dSDV["horzlevelattribflag"] == 1:
                # AASHTO is an ordinal index wiht h/rv/l
                if att == 'AASHTO Group Index':
                    params[7].filter.list = [
                        "Dominant Condition", "Dominant Component",
                        "Minimum", "Maximum"
                    ]
                else:
                    params[7].filter.list = [
                        "Dominant Component", "Minimum", "Maximum", 
                        "Weighted Average"
                    ]
                params[7].value = dSDV["algorithmname"]
            # Component level
            elif dSDV["complevelattribflag"] == 1:
                params[7].filter.list = [
                    "Dominant Condition", "Dominant Component", 
                    "Minimum", "Maximum", "Weighted Average"
                ]
                params[7].value = dSDV["algorithmname"] 
        # Mapunit level
        elif dSDV["mapunitlevelattribflag"] == 1:
            params[7].filter.list = ["No Aggregation Necessary"]
            for i in range(7, len(params)):
                params[i].enabled = False
        # Ordinal classes
        elif dSDV["tiebreakdomainname"]:
            params[16].enabled = False # Turn off hi/rv/lo
            params[7].filter.list = [
                "Dominant Condition", "Dominant Component",
                "Minimum", "Maximum"
            ]
            params[7].value = dSDV["algorithmname"]

        # Nominal classes
        else:
            params[16].enabled = False # Turn off hi/rv/lo
            params[7].filter.list = [
                "Dominant Condition", "Dominant Component"
            ]
            params[7].value = dSDV["algorithmname"]

        # Set month table
        if dSDV["monthrangeoptionflag"] == 1:
            params[13].enabled = True
        else:
            params[13].enabled = False

        # Set for all SDV or Table options
        table = self.sdv_att[att]['attributetablename']
        if str(table).startswith('ch'):
            params[12].enabled = True
        else:
            params[12].enabled = False
    

    def updateSDV_Aggregation(self, params, att):
        path = params[0].value
        # If SDV, read in more details from SDV attribute table
        # Get SDV Attributes
        if not self.sdv_att.get(att):
        # strings with spaces were getting double bagged with ''
            q = f"attributename = '{att}'"
            q = q.replace("''", "'")
            db_p = f"{path}/sdvattribute"
            with (arcpy.da.SearchCursor(
                db_p, "*",
                where_clause=q
            ) 
            as sCur):
                # self.sdv_att.clear()
                self.sdv_att.update({
                    att: dict(zip(sCur.fields, next(sCur)))
                })
        table = self.sdv_att[att]['attributetablename']

        # Primary & Secondary Constraints
        if ((p_col := self.sdv_att[att]["primaryconcolname"])
            and 
            (s_col := self.sdv_att[att]["secondaryconcolname"])):
            prim_d = self.sdv_con.get(str(att))
            if not prim_d:
                db_p = f"{path}/{table}"
                with arcpy.da.SearchCursor(
                    db_p, [p_col, s_col]
                ) as sCur:
                    self.sdv_con.update({att:{
                        veg: list(zip(*unit))[1]
                        for veg, unit in groupby(sCur, byKey)
                    }})
                prim_d = self.sdv_con.get(att)

            params[6].filter.list = sorted(prim_d.keys())
            params[6].enabled = True
            #crop = params[6].filter.list[0]
            #params[6].value = crop
            #params[10].filter.list = sorted(prim_d[crop])
            params[11].enabled = True
            unit = params[10].filter.list[0]
            params[11].value = unit

        # Primary Constraints Only
        elif (p_col := self.sdv_att[att]["primaryconcolname"]):
            prim_d = self.sdv_con.get(att)
            if not prim_d:
                db_p = f"{path}/{table}"
                with arcpy.da.SearchCursor(
                    db_p, p_col
                ) as sCur:
                    # There are restrictions that are not defined
                    prim_l = list({str(p) for p, in sCur})
                    if 'None' in prim_l:
                        prim_l.remove('None')
                        prim_l.append('Not Specified')
                    self.sdv_con.update({att: prim_l})

            params[6].filter.list = sorted(prim_l)
            params[6].enabled = True
            # Set default constraining for eco-sites
            if (params[5].value 
                in ['Ecological Site ID', 'Ecological Site Name'] 
                and 'NRCS Rangeland Site' in prim_l):
                self.params[6].value = 'NRCS Rangeland Site'
            else:
                feat = params[6].filter.list[0]
                params[6].value = feat
            params[11].filter.list = []
            params[11].value = None
            params[11].enabled = False
            
        else:
            params[6].filter.list = []
            params[6].value = None
            params[6].enabled = False
            params[11].filter.list = []
            params[11].value = None
            params[11].enabled = False

        # Tiebreaker Parameter (is this SDV specific?)
        if self.sdv_att[att]["tiebreakrule"] == -1:
            self.sdv_att[att]["tiebreakrule"] = 0
            params[14].enabled = False
        else:
            params[14].enabled = True