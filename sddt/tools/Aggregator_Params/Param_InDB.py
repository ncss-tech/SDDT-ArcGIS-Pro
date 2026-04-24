#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_Indb: gSSURGO Database 
Parameter for Summarize Soil Information tool

This parameter is workspace and can return "SSURGO" features that have an
mukey field.


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 04/24/2026
    @by: Alexnder Stum
@version 1.1


# --- Updated 4/24/2026, v 1.1
-Attributes that ended in 'r' we're getting truncated, replaced rstrip('_r')
with removesuffix('_r')
- Replaced a nested for-loop with list comprehension to create list of 
SSURGO features
"""
import arcpy
import re

from itertools import groupby

from ... import byKey
from ... import pyErr


class Param_InDB():
    def __init__(self):
        self.is_ssurgo = True
        self.atts = {} # unused, but will be used with SDV?
        # SDV cats
        self.cats = {}
        self.cross = {}
        self.path = ''
        self.error = None

        # Table Physical Name:
            # Column Label: 
                # [Column Physical Name, Logical data type, 
                # Unit of measure, field size, domain name]
        self.cols = dict()

         # Domain name: [sequence, choice]
        self.doms = dict()
        # Crop: [Crop Yield units]
        self.crp_units = dict()
        # Col Name: True/False if lo/RV/hi
        self.RV = dict()

        self.param = arcpy.Parameter(
            displayName="gSSURGO Database",
            name="db",
            direction="Input",
            parameterType="Required",
            datatype= "DEWorkspace",
            multiValue=False
        )
        self.param.filter.list = ["Local Database"]
        # List of potential ssurgo features in db
        self.ssurgo_feats = []


    def update(self, path):
        try:
            # reset if a new path has been provided
            if path:
                self.is_ssurgo = True
            else:
                return []
            if path != self.path:
                self.path = path

                # update cols if unpopulated
                if not self.cols:
                    self.updateDictionaries()

                # update interps
                self.updateInterps()

                # update list of features
                self.ssurgo_feats = self.get_features()
            return self.ssurgo_feats
        except:
            self.is_ssurgo = False
            return []
        
        
    def get_features(self) -> list[str]:
        """Produces a list of potential SSURGO features within the input
        database to populate a filter list for Param_Feat

        Returns
        -------
        list[str]
            Lit of potential SSURGO features found within the input database.
        """
        arcpy.env.workspace = self.path
        feats = arcpy.ListFeatureClasses()
        feats.extend(arcpy.ListRasters())
        ssurgo_feats = [lyr for lyr in feats 
                        for fld in arcpy.Describe(f'{self.path}/{lyr}').fields 
                        if fld.name.lower() == 'mukey']
        return ssurgo_feats
    
    
    def updateInterps(self):
        # Update Interps
        db_p = f"{self.path}/sainterp"
        nccpis = [
            'NCCPI - NCCPI Cotton Submodel (II)',
            'NCCPI - NCCPI Soybeans Submodel (I)',
            'NCCPI - NCCPI Small Grains Submodel (II)',
            'NCCPI - NCCPI Corn Submodel (I)',
            ('NCCPI - National Commodity Crop Productivity Index '
            '(Ver 3.0)')
        ]

        with arcpy.da.SearchCursor(db_p, ['interpname', 'interptype']) as sCur:
            tab_d = {
                name: [name, 'String', itype, 254, '', None]
                for name, itype in sCur
            }
        for nccpi in nccpis:
            tab_d[nccpi] = [nccpi, 'String', 'suitability', 254, '', None]
        # interp name: interp name, 'String', interp type, 254, '']
        self.cols['cointerp'] = tab_d

        # Constrain crop domain to those present in DB and add Units
        db_p = f"{self.path}/cocropyld"
        with (arcpy.da.SearchCursor(db_p, ['cropname', 'yldunits']) as sCur):
            for crop, unit in sCur:
                if crop in self.crp_units:
                    self.crp_units[crop].add(unit)
                else:
                    if crop:
                        self.crp_units[crop]= {unit,}
        crops = [[i, k] for i, k in enumerate(sorted(self.crp_units.keys()))]
        self.doms['crop_name'] = crops

    
    def updateDictionaries(self):
        try:
            # Update Domains
            db_p = f"{self.path}/mdstatdomdet"
            with (arcpy.da.SearchCursor(
                db_p, ['domainname', 'choicesequence', 'choice']) as sCur
            ):
                for dom_n, seq, choice in sCur:
                    if dom_n in self.doms:
                        self.doms[dom_n].append([seq, choice])
                    else:
                        self.doms[dom_n] = [[seq, choice],]

            # Update Attributes (columns)
            db_p = f"{self.path}/mdstattabcols"
            with (arcpy.da.SearchCursor(
                db_p, 
                ['tabphyname', 'collabel', 'colphyname', 'logicaldatatype', 
                 'uom', 'fieldsize', 'precision', 'domainname'],
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
                    col = list(col)
                    tab = col[0]
                    col_lab = col[1]
                    col_n = col[2]
                    # strings with leading '#' mess up filter lists. Add 'sieve'
                    col_lab = re.sub(r'#(\d+)', 'sieve #' + r'\1', col_lab)
                    
                    if col_n[-2:] == '_r':
                        col_lab = col_lab.replace(' - Representative Value', '')
                        self.RV[col_lab] = True
                        col[2] = col_n.removesuffix('_r')
                        tab_d[col_lab] = col[2:]
                    # if column name is not low or high
                    elif (col_n[-2:] != '_l') and (col_n[-2:] != '_h'):
                        tab_d[col_lab] = col[2:]

            # Get SDV Attributes
            db_p = f"{self.path}/sdvattribute"
            with (arcpy.da.SearchCursor(
                db_p, ["attributekey", "attributename"],
                # 'attributetablename', 'attributelogicaldatatype'],
                sql_clause=[None, "ORDER BY attributekey ASC"]) 
            as sCur):
                self.atts.update(dict(sCur))

            db_p = f"{self.path}/sdvfolder"
            with (arcpy.da.SearchCursor(
                db_p, ['foldername', 'folderkey'],
                sql_clause=[None, "ORDER BY foldersequence ASC"]) 
            as sCur):
                self.cats.update(dict(sCur))

            # Get key cross-walk
            db_p = f"{self.path}/sdvfolderattribute"
            with (arcpy.da.SearchCursor(
                db_p, ['folderkey', 'attributekey'],
                sql_clause=[None, "ORDER BY folderkey ASC"]
                )
            as sCur):
                # folder key: [(folder key, attribute key), ...]
                self.cross.update({
                    fk: list(zip(*ak))[1]
                    for fk, ak in groupby(sCur, byKey)
                })
            # self.d_pop = True
        except:
            self.error = pyErr('Param_InDB')
            return {}


