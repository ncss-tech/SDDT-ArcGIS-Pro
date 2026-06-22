#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Soil Data Development Toolbox for ArcGIS Pro

This is an ESRI Python toolbox designed to process and manage SSURGO downloads 
from  Web Soil Survey and use them to create gSSURGO databases. 
Also includes tools for aggregating soil's information to the map unit
level (mukey).


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 06/22/2026
    @by: Alexnder Stum
@version 1.5.5

# --- Updated 5/22/2026, v 1.5.5
- Seem to have identified logic error that flipped post execute error
# --- Updated 5/22/2026, v 1.5.4
- fixed logic error that prevented user from specifying secondary constraints
with continuous properties
# --- Updated 4/24/2026, v 1.5.3
- Crop yield parameters were getting flipped off
# --- Updated 4/24/2026, v 1.5.2
- When the tool is run the first time in a session, it failed to properly
name and symbolize. Should be fixed now.
# --- Updated 4/24/2026, v 1.5.2
- Enabled joins with raster layers, ironed out issues with joining and 
symbolizing mulitple times to the same vector layer. 
-But Pro hangs after the Execute function even though all lines have 
executed successfully. Added a sys.exit() argument which unhangs it but results
in failed signal. 
# --- Updated 4/20/2026, v 1.5
- Added Flooding and Ponding frequency
- Added new parameter Which Components which enables for a selection of
 aggregations to be used with Dominant Component
 - Fixed issues with symbolizing of joined features. Issues still remain
 when the user runs the two twice in a row or more.
- Aboslute min/max switch no longer needed as Absolute Min/Max is now a 
distinct aggregation method now that Which Components allows for more 
operation stratification
# --- Updated 3/26/2026, v 1.4
- Revamped aggregator to include Which Components parameter, removed Absolute
Min/Max boolean parameter and the Major boolean paramter
# --- Updated 3/20/2026, v 1.3.2
- If a new DB is selected and interps was selected, flush out interps as 
interps can be DB specific.
# --- Updated 3/20/2026, v 1.3.1
- Tweeked to fix deadend if user selected another DB
# --- Updated 3/20/2026, v 1.3
- Modulated the Aggregator tool into collection of custom class objects for 
each parameter that requires any specialized methods.
# --- Updated 03/05/2026, v 1.2
- Renamed Join as Merge
- Added new tool Raster Lookup by Table as Lookup
- Adds parent directory to sys.path
# --- Updated 02/20/2026, v 1.1
- Aggregator: Fixed issues with symbolizing vector by join field
- Aggregator: When user selected Component Crop Yield there were issues with 
it not resetting when a different crop was selected
- Aggregator: Moved the adding and symbolizing of data to the 
PostExecute function teh Aggregator class
# --- Updated 02/06/2026, v 1.0
- All tool Class objects have been sent to individual sub-modules within the
tools subpackage of the sddt package.
- Added Join tool

"""
version = "1.5.5"

import logging
import sys
import os
from importlib import reload
import gc

pyt_path = os.path.abspath(__file__)
sys.path.append(pyt_path)

#try:
import sddt
#except:
    # reload(sddt)

# download
BulkDownload = sddt.tools.Tool_BulkDownload.BulkDownload
# construct
BuildFGDB = sddt.tools.Tool_BuildFGDB.BuildFGDB
Rasterize = sddt.tools.Tool_Rasterize.Rasterize
Valu1 = sddt.tools.Tool_Valu1.Valu1
Lookup = sddt.tools.Tool_Lookup.Lookup
# analyze
# Aggregator = sddt.tools.Tool_Aggregator.Aggregator
# manage
Merge = sddt.tools.Tool_Merge.Merge

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        # self.label = "SDDT_test"
        self.label = "SDDT"
        self.alias = 'Soil Data Development Toolbox'
        # BulkDownload = sddt.tools.Tool_BulkDownload.BulkDownload
        # BuildFGDB = sddt.tools.Tool_BuildFGDB.BuildFGDB
        
        
        # List of tool classes associated with this toolbox
        self.tools = [BulkDownload, BuildFGDB, Rasterize, Valu1, Merge, 
                      Aggregator, Lookup]
        # self.tools = [BulkDownload, BuildFGDB, Rasterize, Valu1, 
        #               Aggregator]

# https://pro.arcgis.com/en/pro-app/latest/arcpy/geoprocessing_and_python/a-template-for-python-toolboxes.htm


import os
import inspect

import arcpy

# from ..analyze.aggregator import main as aggregator
# from .. import pyErr
# from .. import byKey
from sddt import pyErr

from sddt.tools.Aggregator_Params import *
from sddt.tools.Aggregator_Params import Param_AllOthers

arcpy.env.addOutputsToMap = True


class Aggregator(object):
    # output table
    error = ''
    ag_out = ''
    in_feat = ''
    post_exe = False
    att = ''

    agg_d = {
            "Dominant Condition": 'DCD', "Dominant Component": 'DCP', 
            "Minimum": 'MIN', "Maximum": 'MAX', "Weighted Average": 'WTA', 
            "Percent Present": 'PP', 'Least Limiting': 'LL', 
            'Most Limiting': 'ML', "Absolute Minimum": "AMIN", 
            "Absolute Maximum": "AMAX", "Median Frequency": "MFREQ", 
            "Highest Frequency": "HFREQ", "Lowest Frequency": "LFREQ",
            "Frequency Count": "FREQC"
        }

    param_indb = Param_InDB()
    param_filter = Param_Filter()
    param_sdvcat = Param_SDVCat()
    param_primtab = Param_PrimTab()
    param_primatt = Param_PrimAtt()
    param_comtype = Param_ComType()
    param_primcon = Param_PrimCon()
    param_agmeth = Param_AgMeth()
    param_sectab = Param_SecTab()
    param_secatt = Param_SecAtt()


    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Summarize Soil Information"
        self.description = (
            "Summarizes soil information by map unit key. "
            "It provides several aggreation methods and "
            "outputs a table that can be joined to soil "
            "features and rasters."
        )
        self.category = '3) Analyze Databases'
        self.param_infeat = Param_InFeat()


    def param_updater(self, params, params_d):
        # params_d: param id: [enabled, value, values, filter list]

        #### ALL_NONE ####

        if 'INTERP_OFF' in params_d:
            if params[4].value == 'Interpretations':
                params_d['ALL_OFF'] = 5
            params_d.pop('INTERP_OFF')
        if 'ALL_OFF' in params_d:
            for i in range(params_d['ALL_OFF'], len(params)):
                params[i].enabled = False
            params_d.pop('ALL_OFF')

        if not params_d:
            return
        for i, param_settings in params_d.items():
            params[i].enabled = param_settings[0]
            if param_settings[1] != '*':
                params[i].value = param_settings[1]
            if param_settings[2] != '*':
                params[i].values = param_settings[2]
            if param_settings[3] != '*':
                params[i].filter.list = param_settings[3]


    def getParameterInfo(self):
        """Define parameter definitions"""
        # parameter 0: gSSURGO Database
        params = [Aggregator.param_indb.param]

        # parameter 1: gSSURGO Feature or Raster
        params.append(self.param_infeat.param)

        # parameter 2: Choice List Filter
        params.append(Aggregator.param_filter.param)

        # parameter 3: SDV Category
        params.append(Aggregator.param_sdvcat.param)

        # parameter 4: Primary Table
        params.append(Aggregator.param_primtab.param)

        # parameter 5: Primary Soil Attribute
        params.append(Aggregator.param_primatt.param)

        # parameter 6: Which Components
        params.append(Aggregator.param_comtype.param)

        # parameter 7: Aggregation Method
        params.append(Aggregator.param_agmeth.param)

        # parameter 8: Primary Constraint
        params.append(Aggregator.param_primcon.param)

        # parameter 9: Secondary Table
        params.append(Aggregator.param_sectab.param)

        # parameter 10: Secondary Attribute
        params.append(Aggregator.param_secatt.param)

        # parameter 11: Secondary Constraint
        params.append(Param_AllOthers.param11())

        # parameter 12: Depth Ranges
        params.append(Param_AllOthers.param12())

        # parameter 13: Select Annual or month(s)
        params.append(Param_AllOthers.param13())

        # parameter 14: Tie Break Rule
        params.append(Param_AllOthers.param14())

        # parameter 15: Component Percent Cutoff
        params.append(Param_AllOthers.param15())

        # parameter 16: Property Range Value
        params.append(Param_AllOthers.param16())

        # parameter 17: Map Interp Fuzzy Values
        params.append(Param_AllOthers.param17())

        # parameter 18: Include Null Rating Values
            # It was inactivated in ArcMap SDDT with this note: 
            # Need to validate this parameter and its relationship to p-11
            # Check this box to include NULL rating values 
            # This parameter will remain inactive and set to False
        params.append(Param_AllOthers.param18())

        # parameter 19: Treat Null entries as Zero
            # this parameter is obsolete as SDV attributes
            # interpnullsaszerooptionflag and interpnullsaszeroflag are
            # always equal. Merits further investigation
        params.append(Param_AllOthers.param19())

        # parameter 20: Invert Primary Constraint to NOT equal
        params.append(Param_AllOthers.param20())

        # parameter 21: Invert Secondary Constraint to NOT equal
        params.append(Param_AllOthers.param21())

        return params


    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        # --- Gateway: first 2 parameters
        # User just selected a db set layers to db contents
        try:
            if params[0].value and not params[0].hasBeenValidated:
                params[1].filter.list = Aggregator.param_indb.update(
                    params[0].valueAsText
                )
                # is current feature in db?
                if params[1].value not in params[1].filter.list:
                    params[1].value = None
                # is it in a gSSURGO FGDB
                if not Aggregator.param_indb.is_ssurgo:
                    params[0].value = None
                    # params[1].value = None
                params_d = Aggregator.param_filter.update("By Table")
                self.param_updater(params, params_d)
                
            # User just blanked db reset ToC layers
            if not params[0].value and not params[0].hasBeenValidated:
                params[1].value = None
                params[1].filter.list = list(self.param_infeat.paths.keys())
                self.param_updater(params, {'ALL_OFF': 2})
                return
            
            # User just selected a layer, set db to match
            if (lyr_sel := params[1].value) and not params[1].hasBeenValidated:
                if not params[0].value: 
                    # Get path of select feature from ToC
                    params[0].value = os.path.dirname(
                        self.param_infeat.paths[lyr_sel]
                    )
                    # Update filter list to reflect db
                    params[1].filter.list = Aggregator.param_indb.update(
                        params[0].valueAsText
                    )
                    # if not gSSURGO FGDB
                    if not Aggregator.param_indb.is_ssurgo:
                        params[0].value = None
                        params[1].value = None
                        return
                    # Get feature name
                    feat_p = self.param_infeat.paths.get(lyr_sel)
                    # lyr_sel = lyr_sel[lyr_sel.index(':') + 2:]
                    if feat_p:
                        feat_n = os.path.basename(feat_p)
                        params[1].value = feat_n
                    params[2].enabled = True
                
                ### Temp ###
                params_d = Aggregator.param_filter.update("By Table")
                self.param_updater(params, params_d)

            if not params[0].value:
                return

            #######
            # Place holder for param[2]: param_filter, for now locked as "By Table"
            # ############ Temporary till SDV cats enabled ############
            # if params[2].value:
            #     params[4].enabled = True 
            
            # if (filt := params[2].value) and not params[2].hasBeenValidated:
            #     params_d = Aggregator.param_filter.update(filt)
            #     self.param_updater(params, params_d)
            ####### 

            # Primary Table hass be selected
            if (tab_lab := params[4].value) and not params[4].hasBeenValidated:
                if not Aggregator.param_indb.cols.get(tab_lab):
                    Aggregator.param_indb.update(params[0].valueAsText)
                params_d = Aggregator.param_primtab.update(
                    tab_lab,
                    Aggregator.param_indb.cols, Aggregator.param_indb.doms
                )
                self.param_updater(params, params_d)
            # elif not params[4].value:
            #     self.param_updater(params, {'ALL_OFF': 5})
            #     return

            # Primary Attribute selected
            elif not params[5].hasBeenValidated:
                tab_n = Aggregator.param_primtab.tabs[tab_lab]
                params_d = Aggregator.param_primatt.update(
                    params[5], tab_lab, tab_n, 
                    Aggregator.param_indb.cols, Aggregator.param_indb.RV
                )
                self.param_updater(params, params_d)
                # which components specified
                if params[6].value:
                    params_d = Aggregator.param_comtype.update(
                        params[6].value, params[7].filter.list, 
                        params[5].valueAsText
                    )
                    self.param_updater(params, params_d)

            # Component Crop Yield
            elif(params[4].value and 'Component Crop Yield' in params[4].value 
            and params[8].value and not params[8].hasBeenValidated):
                crop = params[8].values[0]
                unit_l = Aggregator.param_indb.crp_units[crop]
                params_d = Aggregator.param_primcon.update(unit_l)
                self.param_updater(params, params_d)

            # Component type selection
            elif params[6].value and not params[6].hasBeenValidated:
                params_d = Aggregator.param_comtype.update(
                    params[6].value, params[7].filter.list, params[5].valueAsText
                )
                self.param_updater(params, params_d)

            # Aggregation method selected, activate secondary constraints? 
            elif params[7].value and not params[7].hasBeenValidated:
                tab_n = Aggregator.param_primtab.tabs[tab_lab]
                att = att = Aggregator.param_primatt.att
                dom_n = Aggregator.param_indb.cols[tab_n][att][5]

                params_d = Aggregator.param_agmeth.update(
                    params[7].value, tab_lab, tab_n, 
                    dom_n, Aggregator.param_indb.doms
                )
                self.param_updater(params, params_d)

            # A secondary table was selected
            if (stab_lab := params[9].value) and not params[9].hasBeenValidated:
                stab_n = Aggregator.param_primtab.tabs[stab_lab]
                cols = Aggregator.param_indb.cols[stab_n]
                params_d = Aggregator.param_sectab.update(
                    stab_lab, cols, tab_lab, Aggregator.param_primatt.att
                )
                self.param_updater(params, params_d)

            elif (not (params[4].value 
                       and 'Component Crop Yield' in params[4].value) and
                       not params[9].value):
                params_d = {10: [False, None, '*', '*']}
                params_d[11] = [False, None, '*', '*']
                self.param_updater(params, params_d)

            # A secondary attribute selected
            if (sec_att := params[10].value) and not params[10].hasBeenValidated:
                stab_n = Aggregator.param_primtab.tabs[stab_lab]
                cols = Aggregator.param_indb.cols[stab_n]
                params_d = Aggregator.param_secatt.update(
                    sec_att, cols, Aggregator.param_indb.doms, tab_lab
                )
                self.param_updater(params, params_d)
              
            return
        except:
            # arcpy.AddError(f"{comps_p}")
            func = sys._getframe().f_code.co_name
            Aggregator.error = pyErr(func)
            return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        # params[1].clearMessage()
        # params[1].SetWarningMessage(
        #         f"{self.param_indb.path}: {self.param_indb.is_ssurgo}"
        #     )
        if not Aggregator.param_indb.is_ssurgo:
            params[0].SetWarningMessage(
                f"{Aggregator.param_indb.path} is not a gSSURGO FGDB"
            )
            params[1].SetWarningMessage(
                f"{Aggregator.param_indb.path} is not a gSSURGO FGDB"
            )

        # Parameter interal error messages
        if (err:= self.param_indb.error):
            params[0].setErrorMessage(err)
        if (err:= self.param_infeat.error):
            params[1].setErrorMessage(err)
        if (err:= Aggregator.param_filter.error):
            params[2].setErrorMessage(err)
        if (err:= Aggregator.param_primtab.error):
            params[4].setErrorMessage(err)
        if (err:= Aggregator.param_primatt.error):
            params[5].setErrorMessage(err)
        if (err:= Aggregator.param_comtype.error):
            params[6].setErrorMessage(err)
        if (err:= Aggregator.param_agmeth.error):
            params[7].setErrorMessage(err)
        if (err:= Aggregator.param_primcon.error):
            params[8].setErrorMessage(err)
        if (err:= Aggregator.param_sectab.error):
            params[9].setErrorMessage(err)
        if (err:= Aggregator.param_secatt.error):
            params[10].setErrorMessage(err)

        if params[7].enabled and not params[7].value:
            params[7].setErrorMessage("An Aggregation Method must be selected")

        vl = params[5].values
        vt = params[5].value
        if params[5].enabled and (vl or vt):
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
            if rc > 1 or lc > 1:
                params[5].setErrorMessage(
                    "Select only one Primary Soil Attribute"
                )

        if(params[7].value in ['Percent Present', 'Frequency Count'] 
           and not params[8].value):
            params[8].setErrorMessage(
                "A Primary Constraint required"
            )
        if params[10].enabled and params[9].value and not params[10].value:
            params[10].setErrorMessage(
                "A Secondary Attribute must be specified with Secondary Table"
            )
        if params[11].enabled and params[10].value and not params[11].value:
            params[11].setErrorMessage(
                "A Secondary Constraint must be specified with Secondary Table"
            )
        
        return
    
    def execute(self, params, messages):
        """The source code of the tool."""
        arcpy.AddMessage(f"Tool_Aggregator {version=}")
        Aggregator.post_exe = False

        import sddt.analyze.aggregator
        reload(sddt.analyze.aggregator)
        # from sddt.analyze.aggregator import main as aggregator
        
        if params[2].value == "By Table":
            tab_lab = params[4].value
            if 'Component Crop Yield' in tab_lab:
                if tab_lab == 'Component Crop Yield: Irrigated':
                    att_col = 'irryield'
                    att = 'Irr Yield'
                else:   
                    att_col = 'nonirryield'
                    att = 'Nirr Yield'
            else:
                att = params[5].values[-1]
            tab_n = Aggregator.param_primtab.tabs[tab_lab]
            if not Aggregator.param_indb.cols.keys():
                Aggregator.param_indb.update(
                params[0].valueAsText
            )
            sdv_row = Aggregator.param_indb.cols[tab_n][att]
            custom_b = True
            att_col = sdv_row[0]
        else:
            att = params[5].values[-1]
            ### likely not correct ###
            sdv_row = Aggregator.param_sdvcat[att]
            tab_lab = sdv_row['attributetablename']
            custom_b = False

        #Property Range lo/RV/hi
        if Aggregator.param_indb.RV.get(att):
            if lorvhi := params[16].value:
                if lorvhi == 'Low':
                    att_col += '_l'
                elif lorvhi == 'High':
                    att_col += '_h'
                else:
                    att_col += '_r'
        Aggregator.att = att_col
        if tab_n.startswith('ch'):
            depths = params[12].value
            # abs_mm = params[22].value
        else:
            depths = None
            # abs_mm = False

        agg_meth = params[7].value

        if params[8].enabled and params[8].value:
            prim_con = params[8].valueAsText
        else:
            prim_con = ''

        # Is there Secondary constraint?
        if params[9].enabled and params[9].valueAsText:
            sec_tab_lab = params[9].valueAsText # 6: Secondary Table
            sec_att_lab = params[10].valueAsText # 7: Secondary Attribute
            sec_con = params[11].valueAsText # 8: Secondary Constratint

            sec_tab = Aggregator.param_primtab.tabs[sec_tab_lab]
            sec_sdv_row = Aggregator.param_indb.cols[sec_tab][sec_att_lab]
            sec_att = sec_sdv_row[0]
        else:
            sec_tab = None
            sec_att = None
            sec_con = None

        if params[13].value:
            months = params[13].valueAsText
        else:
            months = None

        # Tab name, column name
        ag_out = sddt.analyze.aggregator.main([
            params[0].valueAsText, # 0: SSURGO database
            tab_n, # 1: SSURGO source table
            att_col, # 2: SSURGO attribute source column
            params[6].value, # 3: Component type
            agg_meth, # 4: Aggregation method
            prim_con, # 5: Primary Constraint
            sec_tab, # 6: Secondary Table
            sec_att, # 7: Secondary Attribute
            sec_con, # 8: Secondary Constratint
            # Change when multiple depth ranges ready
            depths, # 9: depth ranges
            months, # 10: months
            params[14].valueAsText, # 11: Tiebreak for dominant condition
            None, # 12: Null = 0
            params[15].value, # 13: Component % cutoff
            params[17].value, # 14: fuzzy map
            None, # 15: Null rating
            sdv_row, # 16: SDV attribute row
            custom_b, # 17: custom or SDV 
            params[20].value, # 18: Primary NOT
            params[21].value, # 19: Secondary NOT
            # 20: module path
            os.path.dirname(inspect.getfile(sddt.analyze.aggregator)), 
        ])

        if ag_out:
            gdb_p = params[0].valueAsText
            output = f"{gdb_p}\\{ag_out[0]}"
            Aggregator.ag_out = ag_out
            arcpy.AddMessage(f"Summary table has been created: {output}")

            arcpy.env.addOutputsToMap = True
            arcpy.env.workspace = gdb_p
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            map = aprx.activeMap
            tab_n = Aggregator.ag_out[0]
            
            if (in_feat := params[1].valueAsText):
                in_feat_p = f"{gdb_p}\\{in_feat}"
                dtype = arcpy.Describe(in_feat_p).datasetType

                if dtype == 'FeatureClass':
                    join_lyr = arcpy.management.AddJoin(
                        in_layer_or_view=in_feat,
                        in_field='MUKEY',
                        join_table=tab_n,
                        join_field='MUKEY'
                    ).getOutput(0)
                    arcpy.AddMessage(f"{tab_n} has been joined to {in_feat}")

                else:
                    # Current versions of Pro error and/or crash when joins
                    # are scripted between text fields
                    arcpy.management.AddField(
                        in_table=tab_n,
                        field_name="muint",
                        field_type="LONG"
                    )
                    arcpy.management.CalculateField(
                        in_table=tab_n,
                        field="muint",
                        expression="!MUKEY!",
                        expression_type="PYTHON3"
                    )

                    # arcpy.SetProgressor('Creating join')
                    join_lyr = arcpy.management.AddJoin(
                        in_layer_or_view=in_feat,
                        in_field='Value',
                        join_table=tab_n,
                        join_field='muint'
                    ).getOutput(0)
                    arcpy.AddMessage(f"{tab_n} has been joined to {in_feat}")
                    arcpy.AddMessage(
                        "\nYou can manually symbolize on "
                        f"{tab_n}.{Aggregator.ag_out[1]} to see result"
                    )
                
                map.addLayer(join_lyr)
                
                self.postExecute(params)
                Aggregator.post_exe = True
            else: # Just add table
                tab_p = os.path.normpath(f"{gdb_p}\\{tab_n}")
                tab_mp = arcpy.mp.Table(tab_p)
                map.addTable(tab_mp)
            arcpy.SetSeverityLevel(0)
            # try:
            #     raise
            #     #sys.exit(0)
            # except:
                # arcpy.AddMessage(
                #     "\n****Ignore failed notification****\n"
                #     "If there are no printed Error messages "
                #     "and a new soils layer was added to the map, "
                #     "Soil Data Development completed successfully "
                # )
                # sys.exit(0)
            

    def postExecute(self, params):
        """This method takes place after outputs are processed and
        added to the display."""
        # Add table to map
        arcpy.SetSeverityLevel(0)
        try:
            if Aggregator.post_exe:
                return
            logger = logging.getLogger(__name__)
            file_path = os.path.abspath(__file__)
            path = os.path.dirname(file_path) + '/post.log'
            # logger = logging.getLogger(__name__)
            logging.basicConfig(
                filename=path, level=logging.DEBUG, filemode='w',
                format='%(asctime)s - %(levelname)s - %(message)s'
            )
            logger.info(f"{Aggregator.param_primatt.att= }")
            aprx = arcpy.mp.ArcGISProject("CURRENT")

            if Aggregator.ag_out:
                gdb_p = params[0].valueAsText
                tab_n = Aggregator.ag_out[0]

                map = aprx.activeMap
                # Add Soil Map
                if (in_feat := params[1].valueAsText):
                    arcpy.env.workspace = gdb_p
                    in_feat_p = f"{gdb_p}\\{in_feat}"
                    tab_lab = params[4].value

                    # Create layer label                    
                    if params[2].valueAsText == "By Table":
                        if 'Component Crop Yield' in tab_lab:
                            if tab_lab == 'Component Crop Yield: Irrigated':
                                att = 'Irr Yield'
                            else:   
                                att = 'Nirr Yield'
                        else:
                            att = Aggregator.att
                    else:
                        att = Aggregator.att
                        ## self not available
                        sdv_row = Aggregator.param_sdvcat[att] 
                        tab_lab = sdv_row['attributetablename']

                    agg_meth = params[7].value

                    if tab_n.startswith('ag_ch'):
                        depths = params[12].value
                    else:
                        depths = None
                    sym_fld = f'{tab_n}.{Aggregator.ag_out[1]}'
                    logger.info(f"{sym_fld= }")
                    # Create layer name
                    if depths:
                        dmin = depths[0][0]
                        dmax = depths[0][1]
                        d_cat = f"{dmin} to {dmax} cm"
                    else:
                        d_cat = ''

                    if not params[7].enabled:
                        agg_meth = 'Dominant Component'
                    soil_map_n = f"{att} {d_cat} {Aggregator.agg_d[agg_meth]}"
                    dtype = arcpy.Describe(in_feat_p).datasetType
                    logger.info(f"{soil_map_n= }")

                    if dtype == 'FeatureClass':
                        # # This layer is not added to ToC
                        # soil_lyr = map.addDataFromPath(in_feat_p)
                        lyr = map.listLayers()[0]
                        lyr.name = soil_map_n
                        soil_sym = lyr.symbology

                        # Data type of symbology field
                        fld = arcpy.ListFields(lyr, sym_fld)[0]
                        logger.info(f"{fld.name= }")
                        if fld.type == 'String':
                            arcpy.SetProgressor('Applying Symbology')
                            soil_sym.updateRenderer('UniqueValueRenderer')
                            base_fld = Aggregator.ag_out[1].lstrip('prop_')
                            seq_fld = f'{tab_n}.seq_{base_fld}'
                            soil_sym.renderer.fields = [seq_fld, sym_fld]
                        else:
                            arcpy.SetProgressor('Applying Symbology')
                            soil_sym.updateRenderer("GraduatedColorsRenderer")
                            soil_sym.renderer.classificationField = fld.name
                            soil_sym.renderer.breakCount = 6

                        lyr.symbology = soil_sym
                    # Raster
                    else:
                        # pass
                        lyr = map.listLayers(in_feat)[0]
                        lyr.name = soil_map_n
                        # soil_sym = lyr.symbology
                        # fld = arcpy.ListFields(lyr, sym_fld)[0]

                        # soil_sym.updateColorizer('RasterClassifyColorizer')
                        # soil_sym.colorizer.classificationField = fld
                        # soil_sym.colorizer.breakCount = 6
                        # soil_sym.colorizer.colorRamp = aprx.listColorRamps(
                        #     'Distance'
                        # )[0]
                        # lyr.symbology = soil_sym
            # logging.shutdown()  
            return
        except:
            
            
            # logger.info('Started')
            gc.collect()
            logger.exception(pyErr('postExecute'))
            # logging.shutdown()
            return
            # arcpy.AddError(pyErr('postExecute'))
        
