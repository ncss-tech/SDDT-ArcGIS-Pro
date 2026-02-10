#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Summarize Soil Information
Intended for the Soil Data Development Toolbox for ArcGIS Pro

Summarizes soil information by map unit key. It provides several 
aggreation methods and outputs a table that can be joined to soil
features and rasters.


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 02/06/2026
    @by: Alexnder Stum
@version 1.0

"""
version = "1.0"

import re
import os
import inspect
from itertools import groupby

import arcpy
from arcpy.da import SearchCursor

from ..analyze.aggregator import main as aggregator
from .. import pyErr
from .. import byKey


class Aggregator(object):
    tabs = {
        'Component': 'component', 'Horizon': 'chorizon', 
        'Interpretations': 'cointerp', 
        'Component Crop Yield: Irrigated': 'cocropyld',
        'Component Crop Yield: Nonirrigated': 'cocropyld'
    } 
    # primary table: [available secondary tables]
    s_tabs = {
        'Component': ['Component',],
         'Horizon': ['Horizon', 'Component'] #diagnostic horizons, landform, geomorph, ecosite, soil moisture
    }
    above_comp = {'mapunit', 'muaggatt'}
    states = [
        '(AK)', '(AL)', '(AR)', '(AS)', '(AZ)', '(CA)', '(CO)', '(CT)', '(DC)',
        '(DE)', '(FL)', '(GA)', '(GU)', '(HI)', '(IA)', '(ID)', '(IL)', '(IN)',
        '(KS)', '(KY)', '(LA)', '(MA)', '(MD)', '(ME)', '(MI)', '(MN)', '(MO)',
        '(MP)', '(MS)', '(MT)', '(NC)', '(ND)', '(NE)', '(NH)', '(NJ)', '(NM)',
        '(NV)', '(NY)', '(OH)', '(OK)', '(OR)', '(PA)', '(PR)', '(RI)', '(SC)', 
        '(SD)', '(TN)', '(TX)', '(UT)', '(VA)', '(VI)', '(VT)', '(WA)', '(WI)',
        '(WV)', '(WY)'
        ]
    months = [
        'Annual', 'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
    agg_d = {
            "Dominant Condition": 'DCD', "Dominant Component": 'DCP', 
            "Minimum": 'MIN', "Maximum": 'MAX', "Weighted Average": 'WTA', 
            "Percent Present": 'PP', 'Least Limiting': 'LL', 
            'Most Limiting': 'ML'
        }
    attributes = dict()
    prim_att = None
    ordinal = [
        'rupresblkmst', 'rupresblkdry', 'rupresblkcem', 'rupresplate', 
        'mannerfailure','stickiness', 'plasticity', 'kwfact', 'kffact', 
        'excavdifcl', 'excavdifms', 'hydriccriterion', 'flodfreqcl',
        'floddurcl', 'pondfreqcl', 'ponddurcl', 'runoff', 'weg', 'erocl',
        'hydricon', 'hydricrating', 'drainagecl', 'nirrcapcl' 'nirrcapscl',
        'irrcapcl', 'irrcapscl', 'soilslippot', 'frostact', 'hydgrp', 'corcon',
        'corsteel', 'taxtempcl', 'taxmoistscl', 'taxtempregime', 'flhe',
        'flphe', 'flsoilleachpot', 'flsoirunoffpot', 'misoimgmtgrp',
        'vasoimgtgrp', 'reshard', 'soimoiststat', 'taxmoistcl', 'taxceactcl',
        'taxreaction', 'mustatus', 'farmlndcl', 'muhelcl', 'muwathelcl', 
        'muwndhelcl', 'invesintens', 'flodfreqdcd', 'flodfreqmax',
        'pondfreqprs', 'drclassdcd', 'drclasswettest', 'hydgrpdcd', 'iccdcd', 
        'niccdcd', 'hydclprs'
    ]

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

        self.d_pop = False
        self.cats = dict() # SDV Folder key: SDV Category
        self.atts = dict() # SDV Attribute key: SDV Attribute
        self.cross = dict() # SDV Attribute key: SDV Folder key
        self.dir_paths = dict() # feature: path
        self.paths = dict() # feature: path
        # Physical Name:
        # Column Label: 
        # [Column Physical Name, Logical data type, 
        # Unit of measure, field size, domain name]
        self.cols = dict()
        # Domains domain name: [sequence, choice]
        self.doms = dict()
        # Crop Yield units by crop
        self.crp_units = dict()
        # lo/RV/hi flag for each column
        self.RV = dict()
        # Primary & Secondary 
            # Attribute: {Primary Value: [secondary values]}
        # Primary:
            # # Attribute: [Primary values]}
        self.sdv_con = dict()
        # SDV Attribute: [Entire row from SDV Attribute]
        self.sdv_att = {}

        # Create list of SSURGO datasets present in map
        act_map = arcpy.mp.ArcGISProject("CURRENT").activeMap
        lyrs = act_map.listLayers()
        i = 0
        for lyr in lyrs:
            if lyr.isRasterLayer or lyr.isFeatureLayer:
                try:
                    lyr_flds = arcpy.Describe(lyr).fields
                    for fld in lyr_flds:
                        if fld.name == 'MUKEY':
                            lyr_ref = f"{lyr.name} [map: {i}]"
                            self.paths[lyr_ref] = \
                                os.path.dirname(lyr.dataSource)
                            i += 1
                            break
                except:
                    pass      


    def getParameterInfo(self):
        """Define parameter definitions"""
        # parameter 0
        params = [arcpy.Parameter(
            displayName="gSSURGO Feature or Raster",
            name="inputSSURGO",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            multiValue=False
        )]
        params[0].filter.list = list(self.paths.keys())

        # parameter 1
        params.append(arcpy.Parameter(
            displayName="gSSURGO Database",
            name="inputFolder",
            direction="Input",
            parameterType="Required",
            datatype="DEWorkspace",
            multiValue=False
        ))
        params[1].filter.list = ["Local Database"]

        # parameter 2
        params.append(arcpy.Parameter(
            displayName="Choice List Filters",
            name="filters",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
        ))
        params[-1].filter.list = ["By Table"] #'Soil Data Viewer Categories', 'Soil Data Viewer List'
        params[-1].value = "By Table"

        # parameter 3
        params.append(arcpy.Parameter(
            displayName="SDV Category",
            name="SDVcat1",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))

        # parameter 4
        params.append(arcpy.Parameter(
            displayName="Primary Table",
            name="tables",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))
        params[-1].filter.list = list(Aggregator.tabs.keys())

        # parameter 5
        params.append(arcpy.Parameter(
            displayName="Primary Soil Attribute (select one)",
            name="attribute",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            # Set True to enable searching, but only 1 choice allowed
            multiValue=True,
            enabled=False
        ))
        params[-1].value = None
        # params[-1].filter.list = []

        # parameter 6
        params.append(arcpy.Parameter(
            displayName="Primary Constraint",
            name="primary",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False,
            multiValue=True
        ))

        # parameter 7
        params.append(arcpy.Parameter(
            displayName="Aggregation Method",
            name="agmeth",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False
        ))

        # parameter 8
        params.append(arcpy.Parameter(
            displayName="Secondary Table",
            name="stables",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False,
            category="Secondary"
        ))
        #params[-1].filter.list = list(aggregator.s_tabs.keys())

        # parameter 9
        params.append(arcpy.Parameter(
            displayName="Secondary Soil Attribute",
            name="sattribute",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False,
            category="Secondary"
        ))

        # parameter 10
        params.append(arcpy.Parameter(
            displayName="Secondary Constraint",
            name="secondary",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False,
            multiValue=True,
            category="Secondary"
        ))

        # parameter 11
        params.append(arcpy.Parameter(
            displayName="Depth Ranges (cm)",
            name="depths",
            direction="Input",
            parameterType="Optional",
            datatype="GPValueTable",
            # multiValue=True, # Make future version multivalue
            enabled=False
        ))
        params[-1].columns = [["GPLong", "Top"], ["GPLong", "Bottom"]]
        params[-1].filters[0].type = "Range"
        params[-1].filters[1].type = "Range"
        params[-1].filters[0].list = [0, 499]
        params[-1].filters[1].list = [1, 500]

        # parameter 12
        params.append(arcpy.Parameter(
            displayName="Timespan",
            name="month1",
            direction="Input",
            parameterType="Optional",
            datatype="GPValueTable",
            enabled=False
        ))
        params[-1].columns = [["GPString", "Beginning"], ["GPString", "End"]]
        params[-1].filters[0].list = Aggregator.months
        params[-1].filters[1].list = Aggregator.months

        # parameter 13
        params.append(arcpy.Parameter(
            displayName="Tie Break Rule",
            name="tie",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False,
            category='Optional'
        ))
        params[-1].filter.list = ['Higher', 'Lower']
        params[-1].value = 'Higher'

        # parameter 14
        params.append(arcpy.Parameter(
            displayName="Component Percent Cutoff",
            name="comp_pct",
            direction="Input",
            parameterType="Required",
            datatype="GPLong",
            enabled=False,
            category='Optional'
        ))
        params[-1].value = 0

        # parameter 15
        params.append(arcpy.Parameter(
            displayName="Property Range Value",
            name="range_value",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False,
            category='Optional'
        ))
        params[-1].filter.list = ['Representative', 'Low', 'High']
        params[-1].value = 'Representative'

        # parameter 16
        # Not relevant till mapping functionality encoded
        params.append(arcpy.Parameter(
            displayName="Map Interp Fuzzy Values",
            name="fuzzy",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False,
            category='Optional'
        ))
        params[-1].value = False

        # parameter 17 It was inactivated in ArcMap SDDT with this note: 
            # Need to validate this parameter and its relationship to p-11
            # Check this box to include NULL rating values 
        # This parameter will remain inactive and set to False
        params.append(arcpy.Parameter(
            displayName="Include Null Rating Values",
            name="rating_null",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False
        ))
        params[-1].value = False

        # parameter 18
        # this parameter is obsolete as SDV attributes
        # interpnullsaszerooptionflag and interpnullsaszeroflag are
        # always equal. Merits further investigation
        params.append(arcpy.Parameter(
            displayName="Treat Null entries as Zero",
            name="null",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False
        ))
        params[-1].value = False

        # parameter 19
        params.append(arcpy.Parameter(
            displayName="Only consider Major components",
            name="major",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False,
            category='Optional'
        ))
        params[-1].value = False

        # parameter 20
        params.append(arcpy.Parameter(
            displayName="Invert Primary Constraint to NOT equal",
            name="primaryNOT",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False,
            category='Optional'
        ))
        params[-1].value = False

        # parameter 21
        params.append(arcpy.Parameter(
            displayName="Invert Secondary Constraint to NOT equal",
            name="secondaryNOT",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False,
            category='Optional'
        ))
        params[-1].value = False

        # parameter 22
        params.append(arcpy.Parameter(
            displayName="Find absoluste Horizon Min or Max",
            name="abs_min_max",
            direction="Input",
            parameterType="Optional",
            datatype="GPBoolean",
            enabled=False,
            category='Optional'
        ))
        params[-1].value = False

        return params

    def updateInterps(self, path):
        db_p = f"{path}/sainterp"
        nccpis = [
            'NCCPI - NCCPI Cotton Submodel (II)',
            'NCCPI - NCCPI Soybeans Submodel (I)',
            'NCCPI - NCCPI Small Grains Submodel (II)',
            'NCCPI - NCCPI Corn Submodel (I)',
            ('NCCPI - National Commodity Crop Productivity Index '
            '(Ver 3.0)')
        ]
        # Add interpdesc for metadata?
        with SearchCursor(db_p, ['interpname', 'interptype']) as sCur:
            tab_d = {
                name: [name, 'String', itype, 254, '', None]
                for name, itype in sCur
            }
        for nccpi in nccpis:
            tab_d[nccpi] = [nccpi, 'String', 'suitability', 254, '', None]
        # interp name: interp name, 'String', interp type, 254, '']
        self.cols['cointerp'] = tab_d
        # self. variables not available at Execute
        Aggregator.attributes.update(self.cols)

        # Constrain crop domain to those present in DB and add Units
        db_p = f"{path}/cocropyld"
        with (SearchCursor(db_p, ['cropname', 'yldunits']) as sCur):
            for crop, unit in sCur:
                if crop in self.crp_units:
                    self.crp_units[crop].add(unit)
                else:
                    if crop:
                        self.crp_units[crop]= {unit,}
        crops = [[i, k] for i, k in enumerate(sorted(self.crp_units.keys()))]
        self.doms['crop_name'] = crops


    def updateDictionaries(self, path):
        # Update Domains
        db_p = f"{path}/mdstatdomdet"
        with (SearchCursor(
            db_p, ['domainname', 'choicesequence', 'choice']) as sCur
        ):
            for dom_n, seq, choice in sCur:
                if dom_n in self.doms:
                    self.doms[dom_n].append([seq, choice])
                else:
                    self.doms[dom_n] = [[seq, choice],]

        # Update Attributes (columns)
        db_p = f"{path}/mdstattabcols"
        with (SearchCursor(
            db_p, 
            ['tabphyname', 'collabel', 'colphyname', 'logicaldatatype', 'uom', 
            'fieldsize', 'precision', 'domainname'],
            sql_clause=[None, "ORDER BY tabphyname ASC, colsequence ASC"]) 
        as sCur):
            # dictionary of table columns
            tab_d = dict()
            tab = None
            # strip RV and exclude hi/lo versions
            for col in sCur:
                # If onto a another table, reset tab_d
                if tab and col[0] != tab:
                    self.cols[tab] = tab_d.copy()
                    tab_d.clear()
                    tab = col[0]
                tab = col[0]
                k = col[1].replace(' - Representative Value', '')
                # strings with leading '#' mess up filter lists. Add 'sieve'
                k = re.sub(r'#(\d+)', 'sieve #' + r'\1', k)
                # if column name is not low or high
                if (col[2][-2:] != '_l') and (col[2][-2:] != '_h'):
                    tab_d[k] = col[2:]
                else:
                    k = k.replace(' - Low Value', '')
                    k = k.replace(' - High Value', '')
                    self.RV[k] = True
        Aggregator.attributes.update(self.cols)
        # Get SDV Attributes
        db_p = f"{path}/sdvattribute"
        with (SearchCursor(
            db_p, ["attributekey", "attributename"],# 'attributetablename', 'attributelogicaldatatype'],
            sql_clause=[None, "ORDER BY attributekey ASC"]) 
        as sCur):
            self.atts.update(dict(sCur))

        db_p = f"{path}/sdvfolder"
        with (SearchCursor(
            db_p, ['foldername', 'folderkey'],
            sql_clause=[None, "ORDER BY foldersequence ASC"]) 
        as sCur):
            self.cats.update(dict(sCur))

        # Get key cross-walk
        db_p = f"{path}/sdvfolderattribute"
        with (SearchCursor(
            db_p, ['folderkey', 'attributekey'],
            sql_clause=[None, "ORDER BY folderkey ASC"]
            )
        as sCur):
            # folder key: [(folder key, attribute key), ...]
            self.cross.update({
                fk: list(zip(*ak))[1]
                for fk, ak in groupby(sCur, byKey)
            })
        self.d_pop = True
        

    def updateDatabases(self, params):
        arcpy.env.workspace = params[1].valueAsText
        lyrs2 = arcpy.ListDatasets()
        lyrs2.extend(arcpy.ListFeatureClasses())
        # Scrub feature directory
        self.dir_paths.clear()
        # Create list of directory features
        for lyr in lyrs2:
            lyr_path = f"{arcpy.env.workspace}/{lyr}"
            lyr_flds = arcpy.Describe(lyr_path).fields
            for fld in lyr_flds:
                if fld.name.lower() == 'mukey':
                    lyr_ref = f"{lyr} [dir]"
                    self.dir_paths[lyr_ref] = params[1].valueAsText
                    break
        params[0].filter.list = list(self.paths.keys()) \
            + list(self.dir_paths.keys())
        # verify that selected feature is in database
        # if params[0].value not in set(self.dir_paths.keys()):
            # params[0].value = None

    
    def byTable_p_tab(self, params):
        table_lab = params[4].value
        tab_n = self.tabs[table_lab]
        params[5].value = None
        params[5].filter.list = []
        params[5].enabled = True
        # if params[5].value:
        
        if table_lab == 'Interpretations':
            params[5].filter.list = list(self.cols[tab_n].keys())
            for i in range(6, len(params)):
                params[i].enabled = False
        elif(table_lab in ('Component Crop Yield: Irrigated', 
                           'Component Crop Yield: Nonirrigated')):
            col_prop = self.cols[tab_n]['Crop Name']
            col_dom = col_prop[5]
            params[5].values = None
            params[5].value = 'Crop Name'
            params[6].enabled = True
            dom_l = sorted(self.doms[col_dom])
            plants = list(list(zip(*dom_l))[1])
            params[6].filter.list = plants
            params[7].enabled = True
            params[7].filter.list = ["Weighted Average", "Dominant Component"]
            params[7].value = "Weighted Average"
            params[8].enabled = True
            params[8].filter.list = [table_lab,]
            params[8].value = table_lab
            params[9].enabled = True
            params[9].filter.list = ['Units',]
            params[9].value = 'Units'
            params[10].enabled = True
            params[10].filter.list = []
            params[10].value = None
            params[11].enabled = False
            params[20].value = False
            params[20].enabled = False
            params[21].value = False
            params[21].enabled = False
            params[15].enabled = True
            return
        else:
            params[5].filter.list = list(self.cols[tab_n].keys())[:-2]
            for i in range(6, len(params)):
                params[i].enabled = False
        if tab_n == 'Component Month':
            params[12].enabled = True
        else:
            params[12].enabled = False


    def byTable_p_att(self, params):
        """Primary Attribute has been selected"""
        table_lab = params[4].value
        if(table_lab in ('Component Crop Yield: Irrigated', 
                           'Component Crop Yield: Nonirrigated')):
            return
        tab_n = self.tabs[table_lab]

        # constraning to one selection
        vl = params[5].values
        vt = params[5].value
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

        # if all selections were removed or more than 1
        # if (not rc and not lc) or (rc > 1 and lc > 1):
        #     for i in range(6, len(params)):
        #         params[i].enabled = False
        #     return
        # if row count and param 5 is a value table
        if rc and 'table' in str(type(vt)).lower():
            params[21].enabled = True
            att = vt.getValue(0, 0)
        # if list and param 5 is a value table
        elif lc and 'table' in str(type(vl)).lower():
            params[20].enabled = True
            att = vl.getValue(0, 0)
        # if not value table but something in list
        elif lc:
            params[19].enabled = True
            att = vl[0]
        else:
            params[19].enabled = False
            att = vt[0]
        
        params[7].enabled = True
        col_prop = self.cols[tab_n][att]
        dtype = col_prop[1]
        itype = col_prop[2]
        col_dom = col_prop[5]

        if table_lab ==  'Interpretations':
            if itype == 'class':
                params[7].filter.list = [
                    "Dominant Condition", "Dominant Component"
                ]
            else:
                params[7].filter.list = [
                    "Dominant Component", "Dominant Condition",
                    "Least Limiting", "Most Limiting", "Weighted Average"
                ]
                params[7].value = None
            # Turn off constraints and secondary
            params[6].enabled = False
            params[8].enabled = False
            params[9].enabled = False
            params[10].enabled = False
            params[11].enabled = False
            params[15].enabled = False
            params[20].enabled = False
            params[21].enabled = False
            return
        # If Integer w/ unit or Float
        elif(((dtype == "Integer") and itype) 
           or (dtype == "Float")):
            params[7].filter.list = [
                    "Dominant Component", 
                    "Minimum", "Maximum", "Weighted Average"
                ]
            params[7].value = None
            # Turn on secondary options. May need to limit this
            params[6].enabled = False
            params[8].value = None
            params[8].filter.list = Aggregator.s_tabs[table_lab]
            params[8].enabled = True
            params[9].value = None
            params[9].filter.list = []
            params[9].enabled = True
            params[10].values = None
            params[10].filter.list = []
            params[10].enabled = True
            params[20].enabled = False
            params[21].enabled = True
        # If there is a domain
        elif col_dom:
            params[7].filter.list = [
                "Dominant Condition", "Dominant Component", "Percent Present"
            ]
            params[7].value = None
            params[8].enabled = False
            params[9].enabled = False
            params[10].enabled = False
            params[20].enabled = False
            params[21].enabled = False
        elif att:
            params[7].filter.list = [
                "Dominant Condition", "Dominant Component"
            ]
            params[7].value = None
            params[8].enabled = False
            params[9].enabled = False
            params[10].enabled = False
            params[20].enabled = False
            params[21].enabled = False
        else:
            for i in range(7, len(params)):
                params[i].enabled = False

        if tab_n.startswith('ch'):
            params[11].enabled = True
        else:
           params[11].enabled = False
        if self.RV.get(att):
            params[15].enabled = True
        else:
            params[15].enabled = False
        

    def byTable_agg(self, params):
        table_lab = params[4].value
        tab_n = self.tabs[table_lab]
        method = params[7].value
        if method == "Dominant Component":
            # Percent cutoff not relavent
            params[14].enabled = False
            params[19].enabled = False
        else:
            params[14].enabled = True
            params[19].enabled = True
        if(table_lab in ('Component Crop Yield: Irrigated', 
                           'Component Crop Yield: Nonirrigated')):
            return
        if method == "Dominant Condition":
            # Tiebreak relavent
            params[13].enabled = True
            # Populate domain list
            att = params[5].values[0]
            col_prop = self.cols[tab_n][att]
            dom_n = col_prop[5]
            # domain_l = (sorted(self.doms[dom_n]))
            # col_dom2 = [choice for seq, choice in domain_l]
        else:
            params[13].enabled = False
        if method == "Percent Present":
            att = params[5].values[0]
            col_prop = self.cols[tab_n][att]
            dom_n = col_prop[5]
            dom_l = sorted(self.doms[dom_n])
            col_dom2 = [choice for seq, choice in dom_l]
            params[6].enabled = True
            params[6].value = None
            params[6].filter.list = col_dom2
            params[20].enabled = True
        else:
            params[6].enabled = False
            params[6].value = None
            params[20].enabled = False
        # Horizon tables, provide option find absolute max/min value
        if(tab_n.startswith('ch') 
           and ((method == "Maximum") or (method == "Minimum"))):
            params[22].enabled = True
        else:
            params[22].enabled = False
        if(table_lab == 'Interpretations' 
           and method in ("Least Limiting", "Most Limiting")):
            params[16].enabled = True
        else:
            params[16].enabled = False

    def byTable_s_tab(self, params):
        if(params[4].value in ('Component Crop Yield: Irrigated', 
                           'Component Crop Yield: Nonirrigated')):
            return
        table_lab = params[8].value
        tab_n = self.tabs[table_lab]
        # Get attributes with domains
        atts = [k for k, v in self.cols[tab_n].items() if v[5]]
        params[9].filter.list = atts
        params[9].value = None
        params[10].filter.list = []
        params[10].value = None


    def byTable_s_att(self, params):
        if(params[4].value in ('Component Crop Yield: Irrigated', 
                           'Component Crop Yield: Nonirrigated')):
            crop = params[6].values[0]
            params[10].filter.list = list(self.crp_units[crop])
            return
        table_lab = params[8].value
        tab_n = self.tabs[table_lab]
        att = params[9].value
        col_prop = self.cols[tab_n][att]
        dom_n = col_prop[5]
        dom_l = sorted(self.doms[dom_n])
        col_dom2 = [choice for seq, choice in dom_l]
        params[10].filter.list = col_dom2
        params[10].value = None


    def updateSDV(self, params):
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
        
        self.updateSDV_Aggregation(params=params, att=att)
        dSDV = self.sdv_att[att]

        # Set Aggregation Method
        # SDV attribute and Percent Present algorithm
        params[7].enabled = True # Turn on aggregation method
        if (dSDV["algorithmname"] == "percent present"):
            params[7].filter.list = ["Percent Present"]
            params[7].value = "Percent Present"
            params[16].enabled = False # Turn off Fuzzy
            params[17].enabled = False # Turn off Null rating
            params[15].enabled = False # Turn off RV
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
            params[15].enabled = True # Turn on hi/rv/lo
            params[15].value = 'Representative'
            params[17].enabled = False # Turn off Null rating
            params[16].enabled = False # Turn off Interp Fuzzy Values
            
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
            params[15].enabled = False # Turn off hi/rv/lo
            params[7].filter.list = [
                "Dominant Condition", "Dominant Component",
                "Minimum", "Maximum"
            ]
            params[7].value = dSDV["algorithmname"]

        # Nominal classes
        else:
            params[15].enabled = False # Turn off hi/rv/lo
            params[7].filter.list = [
                "Dominant Condition", "Dominant Component"
            ]
            params[7].value = dSDV["algorithmname"]

        # Set month table
        if dSDV["monthrangeoptionflag"] == 1:
            params[12].enabled = True
        else:
            params[12].enabled = False

        # Set for all SDV or Table options
        table = self.sdv_att[att]['attributetablename']
        if str(table).startswith('ch'):
            params[11].enabled = True
        else:
            params[11].enabled = False
    

    def updateSDV_Aggregation(self, params, att):
        path = params[1].value
        # If SDV, read in more details from SDV attribute table
        # Get SDV Attributes
        if not self.sdv_att.get(att):
        # strings with spaces were getting double bagged with ''
            q = f"attributename = '{att}'"
            q = q.replace("''", "'")
            db_p = f"{path}/sdvattribute"
            with (SearchCursor(
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
                with SearchCursor(
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
            params[10].enabled = True
            unit = params[10].filter.list[0]
            params[10].value = unit

        # Primary Constraints Only
        elif (p_col := self.sdv_att[att]["primaryconcolname"]):
            prim_d = self.sdv_con.get(att)
            if not prim_d:
                db_p = f"{path}/{table}"
                with SearchCursor(
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
            params[10].filter.list = []
            params[10].value = None
            params[10].enabled = False
            
        else:
            params[6].filter.list = []
            params[6].value = None
            params[6].enabled = False
            params[10].filter.list = []
            params[10].value = None
            params[10].enabled = False

        # Tiebreaker Parameter (is this SDV specific?)
        if self.sdv_att[att]["tiebreakrule"] == -1:
            self.sdv_att[att]["tiebreakrule"] = 0
            params[13].enabled = False
        else:
            params[13].enabled = True

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        if not self.d_pop and params[1].value:
            self.updateDictionaries(path=params[1].value)
            self.updateInterps(path=params[1].value)
        
        # If feature has been selected
        if (feat := params[0].value) and not params[0].hasBeenValidated:
            # set database from selected feature
            if '[dir]' not in feat and params[1].value != self.paths.get(feat):
                params[1].value = self.paths.get(feat)
                self.updateDatabases(params=params)
                self.updateInterps(path=params[1].value)
                
            # else:
            #     params[1].value = self.dir_paths.get(feat)
            # self.updateInterps(path=params[1].value)

        # if a database has been selected
        elif params[1].value and not params[1].hasBeenValidated:
            feat = params[0].value
            curdir = repr(params[1].valueAsText)
            if(curdir != self.paths.get(feat) 
               and curdir != self.dir_paths.get(feat)):
                self.updateDatabases(params=params)
                self.updateInterps(path=params[1].value)
                params[0].value = None
        
            # self.updateInterps(path=params[1].value)

        # Don't display further options until database selected
        if params[1].value:
            params[2].enabled = True
        else: # Otherwise, shut down all subsequent options
            for i in range(2, len(params)):
                params[i].enabled = False

        ############ Temporary till SDV cats enabled ############
        if params[2].value:
            params[4].enabled = True
            

        if (filt := params[2].value) and not params[2].hasBeenValidated:
            # params[5].filter.list = []
            # if params[5].value:
            #     params[5].value = None
            # SDV filter selected
            if ("Most Common Grouped (SDV Categories)" == filt
                or "Most Common as List (all SDV)" == filt):
                self.updateSDV(params=params)

            # Update Table filters
            elif "By Table" == filt:
                params[3].enabled = False # turn off SDV Category
                params[4].enabled = True
                # Turn off Soil Attributes
                params[5].enabled = False 
                #params[4].value = None

            for i in range(6, len(params)):
                params[i].enabled = False
            return

        # Update SDV Soil Attribute Filter
        # if (cat := params[3].value) and not params[3].hasBeenValidated:
        #     params[5].filter.list = []
        #     params[5].value = None
        #     fold_k = self.cats[cat]
        #     att_keys = self.cross[fold_k]
        #     params[5].enabled = True
        #     params[5].filter.list = sorted([
        #         self.atts[ak] for ak in att_keys
        #     ])
        #     params[5].value = None
        if not params[4].value:
            return

        # Primary Table hass be selected
        if params[4].value and not params[4].hasBeenValidated:
            self.byTable_p_tab(params=params)
        # Primary Attribute selected, provide aggregation methods
        # Open other parameters
        elif  not params[5].hasBeenValidated: #(params[5].value or params[5].values) and
            self.byTable_p_att(params=params)
        # Component Crop Yield
        elif('Component Crop Yield' in params[4].value 
           and params[6].value and not params[6].hasBeenValidated):
            crop = params[6].values[0]
            params[10].filter.list = list(self.crp_units[crop])
        # Aggregation method selected, activate secondary constraints? 
        elif params[7].value and not params[7].hasBeenValidated:
            self.byTable_agg(params=params)
        # A secondary table was selected
        elif params[8].value and not params[8].hasBeenValidated:
            self.byTable_s_tab(params=params)
        # A secondary attribute selected and Component Crop Yield
        elif params[9].value and not params[9].hasBeenValidated:
            self.byTable_s_att(params=params)

            
        
        
        # #### Primary and Secondary constrainsts
        # # Crop has been selected, offer respective units
        # if not params[6].hasBeenValidated and params[6].values[-1]:
        #     if params[4].value == 'Component Crop Yield':
        #         params[10].filter.list = self.crp_units[params[6].values[0]]

        # # Activate tiebreaker for Dominant Condition
        # if params[7].enabled and params[7].value == "Dominant Condition":
        #     params[13].enabled = True
        # else:
        #     params[13].enabled = False
        # Activate Component Percent Cutoff and Consider Majors
        # if not Dominant Component
        # if params[7].enabled and params[7].value != "Dominant Component":
        #     params[14].enabled = True
        #     params[19].enabled = True
        # else:
        #     params[14].enabled = False
        #     params[14] = 0
        #     params[19].enabled = False
        

        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        for i in range(5, 11):
            params[i].clearMessage()
        if params[7].enabled and not params[7].value:
            params[7].setErrorMessage("Select an aggregation method")

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
        if params[7].value == "Percent Present" and not params[6].value:
            params[6].setErrorMessage(
                "A Primary Constraint required when Percent Present selected"
            )
        if params[8].enabled and params[8].value and not params[9].value:
            params[9].setErrorMessage(
                "A Secondary Attribute must be specified with Secondary Table"
            )
        if params[8].enabled and params[8].value and not params[10].value:
            params[10].setErrorMessage(
                "A Secondary Constraint must be specified with Secondary Table"
            )
        
        return

    def execute(self, params, messages):
        """The source code of the tool."""
        arcpy.AddMessage(f"Tool_Aggregator {version=}")
        
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
            tab_n = self.tabs[tab_lab]
            sdv_row = Aggregator.attributes[tab_n][att]
            custom_b = True
            att_col = sdv_row[0]
        else:
            att = params[5].values[-1]
            sdv_row = self.sdv_att[att]
            tab_lab = sdv_row['attributetablename']
            custom_b = False
        # arcpy.AddMessage(f"{sdv_row}")
        #Property Range lo/RV/hi
        if self.RV.get(att):
            if lorvhi := params[15].value:
                if lorvhi == 'Low':
                    suf1 = 'Low'
                    att_col += '_l'
                elif lorvhi == 'High':
                    suf1 = 'High'
                    att_col += '_h'
                else:
                    suf1 = 'RV'
                    att_col += '_r'

        if tab_n.startswith('ch'):
            depths = params[11].value
            abs_mm = params[22].value
        else:
            depths = None
            abs_mm = False
        if tab_n in Aggregator.above_comp:
            agg_meth = None
        else:
            agg_meth = params[7].value
        if params[12].value:
            months = params[12].value.getTrueRow(0)
        else:
            months = None
        if params[6].enabled:
            prim_con = params[6].valueAsText
        else:
            prim_con = ''

        # Is there Secondary constraint?
        if params[8].enabled and params[8].value:
            sec_tab_lab = params[8].valueAsText # 6: Secondary Table
            sec_att_lab = params[9].valueAsText # 7: Secondary Attribute
            sec_con = params[10].valueAsText # 8: Secondary Constratint

            sec_tab = self.tabs[sec_tab_lab]
            sec_sdv_row = Aggregator.attributes[sec_tab][sec_att_lab]
            sec_att = sec_sdv_row[0]
        else:
            sec_tab = None
            sec_att = None
            sec_con = None
        
        ag_tab = aggregator([
            params[0].valueAsText, # 0: SSURGO Feature to join to
            params[1].valueAsText, # 1: SSURGO database
            tab_n, # 2: SSURGO source table
            att_col, # 3: SSURGO attribute source column
            agg_meth, # 4: Aggregation method
            prim_con, # 5: Primary Constraint
            sec_tab, # 6: Secondary Table
            sec_att, # 7: Secondary Attribute
            sec_con, # 8: Secondary Constratint
            # Change when multiple depth ranges ready
            depths, # 9: depth ranges
            months, # 10: months
            params[13].value, # 11: Tiebreak for dominant condition
            None, # 12: Null = 0
            params[14].value, # 13: Component % cutoff
            params[16].value, # 14: fuzzy map
            None, # 15: Null rating
            sdv_row, # 16: SDV attribute row
            params[19].value, # 17: Consider only Majors?
            custom_b, # 18: custom or SDV 
            params[20].value, # 19: Primary NOT
            params[21].value, # 20: Secondary NOT
            abs_mm, # 21: Absolute horizon min/max
            os.path.dirname(inspect.getfile(aggregator)), # 22: module path
        ])
        # Add table to map
        try:
            arcpy.AddMessage(f"{ag_tab=}")
            if ag_tab:
                gdb_p = params[1].valueAsText
                output = f"{gdb_p}\\{ag_tab[0]}"
                arcpy.AddMessage(f"Summary table has been created: {output}")
                aprx = arcpy.mp.ArcGISProject("CURRENT")
                map = aprx.activeMap
                # arcpy.management.MakeTableView(output, ag_tab)
                tab_view = arcpy.mp.Table(output)
                map.addTable(tab_view)
                arcpy.AddMessage(f"Summary table has been added to map TOC")
                # Add Soil Map
                if(in_feat := params[0].valueAsText):
                    in_feat_n = in_feat[:in_feat.index('[') -1]
                    if '[map: ' in in_feat:
                        mi = in_feat[
                            in_feat.index('[map: ') + 6: in_feat.index(']')
                        ]
                        mi = int(mi)
                        lyr = map.listLayers(in_feat_n)[mi]
                        feat_p = lyr.dataSource
                    else:
                        feat_p = f"{gdb_p}/{in_feat_n}"
                    sym_fld = f"{ag_tab[0]}.{ag_tab[1]}"

                    # Create layer name
                    if depths:
                        dmin = depths[0][0]
                        dmax = depths[0][1]
                        d_cat = f"{dmin} to {dmax} cm"
                    else:
                        d_cat = ''
                    soil_map_n = f"{att} {d_cat} {Aggregator.agg_d[agg_meth]}"
                    dtype = arcpy.Describe(feat_p).datasetType
                    if dtype == 'FeatureClass':
                        soil_lyr = arcpy.management.MakeFeatureLayer(
                            feat_p, soil_map_n
                        )
                        soil_lyr_obj = soil_lyr.getOutput(0)
                        arcpy.AddMessage('\tCreating join')
                        join_out = arcpy.management.AddJoin(
                            in_layer_or_view=soil_lyr_obj,
                            in_field='MUKEY',
                            join_table=output,
                            join_field='MUKEY'
                        )
                        arcpy.AddMessage('\tRendering')
                        soil_lyr_obj2 =  join_out.getOutput(0)
                        add_out = map.addLayer(soil_lyr_obj2)
                        soil_lyr_obj3 = add_out[0]
                        soil_sym = soil_lyr_obj3.symbology
                        # if float or int and has uom
                        if sdv_row[1] == 'Float' or (sdv_row[1] == 'Integer' and sdv_row[2]):
                            soil_sym.updateRenderer("GraduatedColorsRenderer")
                            soil_sym.renderer.classificationField = sym_fld
                        else:
                            soil_sym.updateRenderer("UniqueValueRenderer")
                            soil_sym.renderer.fields = [sym_fld]
                        
                        soil_lyr_obj3.symbology = soil_sym
                    # Raster
                    else:
                        soil_lyr = arcpy.management.MakeRasterLayer(
                            feat_p, soil_map_n
                        )
                        soil_lyr_obj = soil_lyr.getOutput(0)

                        # Join
                        arcpy.AddMessage('\tCreating join')
                        join_out = arcpy.management.AddJoin(
                            in_layer_or_view=soil_lyr_obj,
                            in_field='MUKEY',
                            join_table=output,
                            join_field='MUKEY'
                        )

                        # Map it
                        arcpy.AddMessage('\tRendering')
                        soil_lyr_obj2 =  join_out.getOutput(0)
                        add_out = map.addLayer(soil_lyr_obj2)
                        # soil_lyr_obj3 = add_out[0]
                        # soil_sym = soil_lyr_obj3.symbology

                        # flds = [f.name for f in 
                        #         arcpy.Describe(soil_lyr_obj3).fields]
                        # # import time
                        # # arcpy.AddMessage("0")
                        # # time.sleep(1)
                        # # If Percent Present
                        # if agg_meth == 'Percent Present':
                        #     sym_fld = ag_tab[0] + '.COMPPCT_R'
                        #     # sym_fld = 'COMPPCT_R'
                        #     soil_sym.updateColorizer('RasterClassifyColorizer')
                        #     soil_sym.colorizer.classificationField = sym_fld
                        #     soil_sym.colorizer.breakCount = 6
                        #     soil_sym.colorizer.colorRamp = aprx.listColorRamps('Distance')[0]
                        #     brk_val = 0
                        #     lab = '0%'
                        #     for brk in soil_sym.colorizer.classBreaks:
                        #         brk.upperBound = brk_val
                        #         brk.label = lab
                        #         lab = f"{brk_val} - {brk_val + 20}%"
                        #         brk_val += 20

                        #     soil_lyr_obj3.symbology = soil_sym

                        # if float or has uom
                        # elif sdv_row[1] == 'Float' or sdv_row[2]:
                        #     soil_sym.updateColorizer('RasterClassifyColorizer')
                        #     soil_sym.colorizer.classificationField = sym_fld
                        # # has ordinal domain
                        # elif att_col in aggregator.ordinal:
                        #     # Create domain dictionary
                        #     db_p = f"{gdb_p}/mdstattabcols"
                        #     with (arcpy.da.SearchCursor(
                        #         db_p, 'domainname', 
                        #         where_clause=f"colphyname = '{att_col}'"
                        #     ) as sCur):
                        #         dom_n = next(sCur)[0]

                        #     db_p = f"{gdb_p}/mdstatdomdet"
                        #     with (arcpy.da.SearchCursor(
                        #         db_p, ['choice', 'choicesequence'], 
                        #         where_clause= f"domainname = '{dom_n}'"
                        #     ) as sCur):
                        #         domain_d = dict(sCur)

                        # else:
                        #     # If 
                        #     soil_sym.updateColorizer(
                        #         'RasterUniqueValueColorizer'
                        #     )
                        #     soil_sym.colorizer.field = sym_fld
                        #     for grp in soil_sym.colorizer.groups:
                        #         for itm in grp.items:
                        #             arcpy.AddMessage(f"{itm}")
                        
                        # soil_lyr_obj3.symbology = soil_sym
                        # Experiment 3 to symbolize on join, fail...
                        # arcpy.AddMessage("Apply Symbology")
                        # soil_lyr = map.listLayers('Drainage Class  PP')[0]
                        # lyr = r"D:\projects\SSURGO\Drainage Class  PP.lyrx"
                        # map.addDataFromPath(lyr)
                        # lyr2 = map.listLayers('Drainage Class  PP')[0]
                        # soil_lyr.symbology = lyr2.symbology
                        # arcpy.management.ApplySymbologyFromLayer(
                        #     soil_lyr,
                        #     r"D:\projects\SSURGO\Drainage Class  PP.lyrx",
                        #     [['Value_Field', 'ag_component_drainagecl_PP.COMPPCT_R', 'ag_component_drainagecl_PP.COMPPCT_R'],])
        except:
            arcpy.AddError(pyErr('Execute'))
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
