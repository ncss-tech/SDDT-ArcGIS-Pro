#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create gSSURGO File Geodatabase
Build gSSURGO File Geodatabase in ArcGIS Pro

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 10/04/2024
    @by: Alexnder Stum
@version: 0.2

# ---
Updated 10/04/2024
- Changed SDA query for developing list of soil surveys for a state to 
    look at legend overlap, not areasymbol.
- Corrected handling of signaling which FGDB were not successful.
- Several Pacific Islands were missing from states dictionary.
- Fixed file name references to xml files.
- Added condition to handle PR and VI as single state database request.
- Fixed error related to finding new rule classes
- Added interp key to gSSURGO 2.0
- Added CONUS build option
- Added 0.01s time outs to allow cursors to switch off
"""

# Import system modules

import concurrent.futures as cf
import csv
import datetime
import gc
import itertools as it
import json
import multiprocessing as mp
import os
import platform
import re
import sys
import time
import traceback
import xml.etree.cElementTree as ET
from importlib import reload
from urllib.request import urlopen
from typing import Any, Callable, Generator, Generic, Iterator, TypeVar, Set

import arcpy
import psutil
from arcpy import env

Tist = TypeVar("Tist", tuple, list)

states = {
    'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AS': 'American Samoa',
    'AZ': 'Arizona', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut',
    'DC': 'District of Columbia', 'DE': 'Delaware', 'FL': 'Florida',
    'FM': 'Federated States of Micronesia', 'GA': 'Georgia', 'GU': 'Guam',
    'HI': 'Hawaii', 'IA': 'Iowa', 'ID': 'Idaho', 'IL': 'Illinois',
    'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 
    'MA': 'Massachusetts', 'MD': 'Maryland', 'ME': 'Maine',
    'MH': 'Marshall Islands', 'MI': 'Michigan', 'MN': 'Minnesota',
    'MO': 'Missouri', 'MP': 'Northern Mariana Islands', 'MS': 'Mississippi',
    'MT': 'Montana', 'NC': 'North Carolina', 'ND': 'North Dakota',
    'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
    'PR': "Puerto Rico", 'PW': 'Palau', 'RI': 'Rhode Island', 
    'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
    'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia', 'VI': 'Virgin Islands',
    'VT': 'Vermont', 'WA': 'Washington', 'WI': 'Wisconsin',
    'WV': 'West Virginia', 'WY': 'Wyoming'
}

class xml:
    def __init__(self, aoi: str, path: str, gssurgo_v: str):
        self.path = path
        self.aoi = aoi
        self.version = gssurgo_v
        if self.version == '2.0':
            path_i = self.path + '/gSSURGO2_'
        else:
            path_i = self.path + '/gSSURGO1_'
        # Input XML workspace document used to create new gSSURGO schema in 
        # an empty geodatabase
        if aoi == "Lower 48 States":
            self.xml = path_i + "CONUS_AlbersNAD1983.xml"
        elif aoi == "Hawaii":
            self.xml = path_i + "Hawaii_AlbersWGS1984.xml"
        elif aoi == "Alaska":
            self.xml = path_i + "Alaska_AlbersNAD1983.xml"
        elif aoi == "Puerto Rico and U.S. Virgin Islands":
            self.xml = path_i + "PRUSVI_StateNAD1983.xml"
        else:
            self.xml = path_i + "Geographic_WGS1984.xml"
        self.exist = os.path.isfile(self.xml)


def pyErr(func: str) -> str:
    """When a python exception is raised, this funciton formats the traceback
    message.

    Parameters
    ----------
    func : str
        The function that raised the python error exception

    Returns
    -------
    str
        Formatted python error message
    """
    try:
        etype, exc, tb = sys.exc_info()
        
        tbinfo = traceback.format_tb(tb)[0]
        tbinfo = '\t\n'.join(tbinfo.split(','))
        msgs = (f"PYTHON ERRORS:\nIn function: {func}"
                f"\nTraceback info:\n{tbinfo}\nError Info:\n\t{exc}")
        return msgs
    except:
        return "Error in pyErr method"


def arcpyErr(func: str) -> str:
    """When an arcpy by exception is raised, this function formats the 
    message returned by arcpy.

    Parameters
    ----------
    func : str
        The function that raised the arcpy error exception

    Returns
    -------
    str
        Formatted arcpy error message
    """
    try:
        etype, exc, tb = sys.exc_info()
        line = tb.tb_lineno
        msgs = (f"ArcPy ERRORS:\nIn function: {func}\non line: {line}"
                f"\n\t{arcpy.GetMessages(2)}\n")
        return msgs
    except:
        return "Error in arcpyErr method"

def funYield(
        fn: Callable, iterSets: Tist, #[dict[str, Any]]
        constSets: dict[str, Any]
    ) : # -> Generator[list[int, str]]
    """Iterativley calls a function as a generator

    Parameters
    ----------
    fn : Callable
        The function to be called as a generator
    iterSets : Tist[dict[str, Any],]
        These dictionaries are a set of dynmaic variables for each iteration. 
        The keys must align with the ``fn`` parameters. The values the 
        arguments for the function call.
    constSets : dict[str, Any]
        This dictionary is composed of the static variables sent as 
        arguments to function call ``fn``. 
        The keys must align with the ``fn`` parameters.

    Yields
    ------
    Generator[int, str]
        If successful, yields the value 0 and an empty string, otherwise
        yields the value 2 with a string message. This generator can be
        modified to yield the returned items from the function ``fn``.
    """
    try:
        fn_inputs = iter(iterSets)
        # initialize first set of processes
        outputs = {
            fn(**params, **constSets): params
            for params in it.islice(fn_inputs, len(iterSets))
        }
        # output, params = outputs.popitem()
        yield [0, '']

    except:
        arcpy.AddWarning('Better luck next time')
        func = sys._getframe().f_code.co_name
        msgs = pyErr(func)
        yield [2, msgs]


def getSSAList(input_p: str) -> Set[str,]:
    """Reports the SSURGO datasets found in a directory.
    Checks if each potential dataset has a tabular and spatial directory. 
    Candidate datasets must have a two letter prefix followed by a three 
    digit number. A 'soil_' prefix is acceptable.

    Parameters
    ----------
    input_p : str
        A directory that will be searched for SSURGO datasets

    Returns
    -------
    Set[str,]
        The directory names of the SSURGO datasets found in ``input_p``.
    """
    present_ssa = {
        ssa.lower()
        for d in os.scandir(input_p)
        if (d.is_dir() and re.match(
            r"[a-zA-Z]{2}[0-9]{3}", (ssa := d.name.removeprefix('soil_'))
            )
            and os.path.exists(f"{d.path}/tabular")
            and os.path.exists(f"{d.path}/spatial")
    )}
    return present_ssa



def sda_ssa_list(state):
    try:
        url = r'https://SDMDataAccess.sc.egov.usda.gov/Tabular/post.rest'
        if state == 'PRVI':
            la_areaname = ("(la.areaname = 'Puerto Rico' "
                            "OR la.areaname = 'Virgin Islands')")
        else:
            la_areaname = f"la.areaname = '{states[state]}'"
        sQuery = (
            "SELECT l.areasymbol "
            "FROM legend l "
            "INNER JOIN  laoverlap la ON l.lkey = la.lkey "
            "AND la.areatypename = 'State or Territory' "
            f"AND {la_areaname}"
            "WHERE l.areatypename = 'Non-MLRA Soil Survey Area' "
            "ORDER BY l.areasymbol"
        )

        # Create request using JSON, return data as JSON
        dRequest = dict()
        dRequest["format"] = "JSON"
        dRequest["query"] = sQuery
        jData = json.dumps(dRequest)
        # Send request to SDA Tabular service using urllib2 library
        jData = jData.encode('ascii')
        response = urlopen(url, jData)
        jsonString = response.read()

        # Convert the returned JSON string into a Python dictionary.
        data = json.loads(jsonString)
        del jsonString, jData, response
        return data
    
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return None
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return None


def createGDB(gdb_p: str, inputXML: xml, label: str) -> str:
    """Creates the SSURGO file geodatabase using an xml workspace file to 
    create tables, features, and spatila relations.

    Parameters
    ----------
    gdb_p : str
        The path of the SSURGO file geodatabase to be created.
    imputXML: xml
        An xml class object that has information about the xml workspace to 
        template new file geodatabase.
    label: str
        A string to be appended to spatial feature alias name.


    Returns
    -------
    str
        An empty string if successful, an error message if unsuccessful.

    """
    try:
        outputFolder = os.path.dirname(gdb_p)
        gdb_n = os.path.basename(gdb_p)

        if arcpy.Exists(gdb_p):
            arcpy.AddMessage(f"\tDeleting existing file gdb {gdb_p}")
            arcpy.management.Delete(gdb_p)
        arcpy.AddMessage(f"\tCreating new geodatabase ({gdb_n}) in "
                         f"{outputFolder}")

        env.XYResolution = "0.001 Meters"
        env.XYTolerance = "0.01 Meters"

        arcpy.management.CreateFileGDB(outputFolder, gdb_n)
        if not arcpy.Exists(gdb_p):
            arcpy.AddError("Failed to create new geodatabase")
            return False
        # The following command will fail when the user only has a Basic license
        arcpy.management.ImportXMLWorkspaceDocument(
            gdb_p, inputXML.xml, "SCHEMA_ONLY"
        )

        env.workspace = gdb_p
        tblList = arcpy.ListTables()
        if len(tblList) < 50:
            arcpy.AddError(f"Output geodatabase has only {len(tblList)} tables")
            return False

        # Alter aliases for featureclasses
        if label:
            try:
                arcpy.AlterAliasName(
                    "MUPOLYGON", "Map Unit Polygons - " + label
                )
                arcpy.AlterAliasName(
                    "MUPOINT", "Map Unit Points - " + label
                )
                arcpy.AlterAliasName(
                    "MULINE", "Map Unit Lines - " + label
                )
                arcpy.AlterAliasName(
                    "FEATPOINT", "Special Feature Points - " + label
                )
                arcpy.AlterAliasName(
                    "FEATLINE", "Special Feature Lines - " + label
                )
                arcpy.AlterAliasName(
                    "SAPOLYGON", "Survey Boundaries - " + label
                )
            except:
                pass
        return True

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def importCoint(ssa_l: list[str], 
              input_p: str, 
              gdb_p: str, 
              table_d: dict[list[str, str, list[tuple[int, str]]]],
              light_b: bool
              ) -> str:
    """Runs through each SSURGO download folder and imports the rows into the 
    specified cointerp table . This table has unique information from each 
    survey area. This funciton is only called for gSSURGO 1.0 builds.

    Parameters
    ----------
    ssa_l : list[str]
        List of soil surveys
    input_p : str
        Path to the SSRUGO downloads
    gdb_p : str
        Path of the SSURGO geodatabase
    table_d : dict[list[str, str, list[tuple[int, str]]]]
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name.

    Returns
    -------
    str
        An empty string if successful, otherwise and error message.
    """
    try:
        time.sleep(0.01)
        arcpy.env.workspace = gdb_p
        csv.field_size_limit(2147483647)
        nccpi_sub = ['37149', '37150', '44492', '57994']
        table = 'cointerp'
        tab_p = f"{gdb_p}/{table}"
        cols = table_d[table][2]
        # get fields in sequence order
        cols.sort()
        fields = [f[1] for f in cols]
        iCur = arcpy.da.InsertCursor(tab_p, fields)
        for ssa in ssa_l:
            # Make file path for text file
            txt_p = f"{input_p}/{ssa.upper()}/tabular/cinterp.txt"
            if not os.path.exists(txt_p):
                return f"{txt_p} does not exist"
            csvReader = csv.reader(
                open(txt_p, 'r'), delimiter='|', quotechar='"'
            )
            if light_b:
                for row in csvReader:
                    if (row[1] == row[4] 
                        or (row[1] == "54955" and row[4] in nccpi_sub)):
                        # Slice out excluded elements
                        row = row[:7] + row[11:13] + row[15:]
                        # replace empty sets with None
                        iCur.insertRow(tuple(v or None for v in row))
            else:
                for row in csvReader:
                    # Slice out excluded elements
                    row = row[:7] + row[11:13] + row[15:]
                    # replace empty sets with None
                    iCur.insertRow(tuple(v or None for v in row))
        del csvReader, iCur
        arcpy.AddMessage(f"\tSuccessfully populated {table}")
        return 0 # None

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return 1 # arcpyErr(func)
    except:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return 1 # pyErr(func)


def importList(ssa_l: list[str], 
              input_p: str, 
              gdb_p: str, 
              table_d: dict[list[str, str, list[tuple[int, str]]]],
              table: str,
              sub_fld: str
              ) -> int:
    """Runs through each SSURGO download folder and imports the rows into the 
    specified ``table`` . These tables have unique information from each 
    survey area.

    Parameters
    ----------
    ssa_l : list[str]
        List of soil surveys
    input_p : str
        Path to the SSRUGO downloads
    gdb_p : str
        Path of the SSURGO geodatabase
    table_d : dict[list[str, str, list[tuple[int, str]]]]
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name.
    table : str
        Table that is being imported.
    sub_fld : str
        Either the 'spatial' or 'tabular' subdirectory of the SSURGO download.

    Returns
    -------
    int
        An empty string if successful, otherwise and error message.
    """
    try:
        time.sleep(0.01)
        arcpy.env.workspace = gdb_p
        csv.field_size_limit(2147483647)
        txt = table_d[table][0]
        cols = table_d[table][2]
        tab_p = f"{gdb_p}/{table}"
        # get fields in sequence order
        cols.sort()
        fields = [f[1] for f in cols]
        iCur = arcpy.da.InsertCursor(tab_p, fields)
        for ssa in ssa_l:
            # Make file path for text file
            txt_p = f"'{input_p}/{ssa.upper()}/{sub_fld}/{txt}.txt'"
            # in some instances // can create special charaters with eval
            txt_p = txt_p.replace('\\', '/')
            # convert latent f strings
            txt_p = eval("f" + txt_p)
            
            if not os.path.exists(txt_p):
                return f"{txt_p} does not exist"
            csvReader = csv.reader(
                open(txt_p, 'r'), delimiter='|', quotechar='"'
            )
            for row in csvReader:
                # replace empty sets with None
                iCur.insertRow(tuple(v or None for v in row))
        del csvReader, iCur
        arcpy.AddMessage(f"\tSuccessfully populated {table}")
        return 0 # None

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return 1 # arcpyErr(func)
    except:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return 1 # pyErr(func)


def importSet(ssa_l: list[str], 
              input_p: str, 
              gdb_p: str, 
              table_d: dict[str, list[str, str, list[tuple[int, str]]]]
    ) -> str:
    """Runs through each SSURGO download folder and compiles a set of unique 
    values to insert into respective tables. These tables are largely common 
    to all surveys but some states have rows unique to their surveys.

    Parameters
    ----------
    ssa_l : list[str]
        List of soil surveys
    input_p : str
        Path to the SSRUGO downloads
    gdb_p : str
        Path of the SSURGO geodatabase
    table_d : dict[list[str, str, list[tuple[int, str]]]]
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name.

    Returns
    -------
    str
        An empty string if successful, otherwise and error message.
    """
    try:
        time.sleep(0.01)
        csv.field_size_limit(2147483647)
        # 'distsubinterpmd'
        tabs_l = ['distinterpmd', 'sdvattribute', 'sdvfolderattribute']
        arcpy.env.workspace = gdb_p
        
        for table in tabs_l:
            txt = table_d[table][0]
            cols = table_d[table][2]
            tab_p = f"{gdb_p}/{table}"
            # get fields in sequence order
            cols.sort()
            fields = [f[1] for f in cols]
            iCur = arcpy.da.InsertCursor(tab_p, fields)
            row_s = set()
            for ssa in ssa_l:
                txt_p = f"{input_p}/{ssa.upper()}/tabular/{txt}.txt"
                if not os.path.exists(txt_p):
                    return f"{txt_p} does not exist"
                csvReader = csv.reader(
                    open(txt_p, 'r'), 
                    delimiter = '|', 
                    quotechar = '"'
                )
                for row in csvReader:
                    # replace empty sets with None
                    row_s.add(tuple(v or None for v in row))
            for row in row_s:
                iCur.insertRow(row)
        del iCur
        return ''

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        func = sys._getframe().f_code.co_name
        return arcpy.AddError(arcpyErr(func))
    except:
        try:
            arcpy.AddError(f"While working on {table} from {ssa}")
            arcpy.AddMessage(cols)
            arcpy.AddMessage(txt)
            for i, e in enumerate(row):
                if e:
                    size = len(e)
                else:
                    size = 0
                arcpy.AddMessage(f"{fields[i]}: {size}")
            del iCur
        except:
            pass
        func = sys._getframe().f_code.co_name
        return arcpy.AddError(pyErr(func))
        
        
        
def importSing(ssa: str, input_p: str, gdb_p: str) -> dict:
    """Import the tables that are common for each SSURGO download and therefore
    only need to be imported once. Also creates a table dictionary that with 
    the table information.

    Parameters
    ----------
    ssa : str
        Single soil survey area symbol in the list.
    input_p : str
        Path to the SSRUGO downloads
    gdb_p : str
        Path of the SSURGO geodatabase

    Returns
    -------
    dict
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name. If the function 
        returns in error the dictionary will return wiht the key 'Error' 
        and a message.
    """
    try:
        time.sleep(0.01)
        # First read in mdstattabs: mstab table into 
        # There should be 75 tables, 6 of which are spatial, so 69
        # Then read tables from gdb
        # Copy common tables and report unused
        # Then import the common tables
        tn = 69
        csv.field_size_limit(2147483647)
        tabs_common = [
            'mdstattabcols', 'mdstatrshipdet', 'mdstattabs', 'mdstatrshipmas',
            'mdstatdommas', 'mdstatidxmas', 'mdstatidxdet',  'mdstatdomdet',
            'sdvfolder', 'sdvalgorithm'
        ]

        arcpy.env.workspace = gdb_p
        txt_p = f"{input_p}/{ssa.upper()}/tabular/mstab.txt"
        if not os.path.exists(txt_p):
            table_d = {'Error': (f"{txt_p} does not exist", '', [])}
            return table_d
        csvReader = csv.reader(open(txt_p, 'r'), delimiter='|', quotechar='"')
        
        # dict{Table Physical Name: 
        # [text file, Table Label, [(seq, column names)]]}
        table_d = {t[0]: [t[4], t[2], []] for t in csvReader}
        # Retrieve column names
        txt_p = f"{input_p}/{ssa.upper()}/tabular/mstabcol.txt"
        if not os.path.exists(txt_p):
            table_d = {'Error': f"{txt_p} does not exist"}
            return table_d
        csvReader = csv.reader(open(txt_p, 'r'), delimiter='|', quotechar='"')
        for row in csvReader:
            table = row[0]
            if table in table_d:
                # add tuple with sequence (as int to sort) and column name
                table_d[table][2].append((int(row[1]), row[2]))
        
        # Populate static tables
        for table in tabs_common:
            txt = table_d[table][0]
            cols = table_d[table][2]
            tab_p = f"{gdb_p}/{table}"
            # get fields in sequence order
            cols.sort()
            fields = [f[1] for f in cols]

            iCur = arcpy.da.InsertCursor(tab_p, fields)
            txt_p = f"{input_p}/{ssa.upper()}/tabular/{txt}.txt"
            if not os.path.exists(txt_p):
                table_d = {'Error': f"{txt_p} does not exist"}
                return table_d
            csvReader = csv.reader(
                open(txt_p, 'r'), 
                delimiter = '|', 
                quotechar='"'
            )
            for row in csvReader:
                # replace empty sets with None
                iCur.insertRow(tuple(v or None for v in row))
            del iCur
            # Populate the month table
            months = [
                (1, 'January'), (2, 'February'), (3, 'March'), (4, 'April'),
                (5, 'May'), (6, 'June'), (7, 'July'), (8, 'August'),
                (9, 'September'), (10, 'October'), (11, 'November'),
                (12, 'December')
            ]
            month_p = f"{gdb_p}/month"
            iCur = arcpy.da.InsertCursor(month_p, ['monthseq', 'monthname'])
            for month in months:
                iCur.insertRow(month)
            del iCur

        return table_d

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        table_d['Error'] = (arcpy.AddError(arcpyErr(func)), '', [])
        return table_d
    except:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
            arcpy.AddError(f"{row= }")
        except:
            pass
        func = sys._getframe().f_code.co_name
        table_d['Error'] = (arcpy.AddError(arcpyErr(func)), '', [])
        return table_d


def appendFeatures(
        gdb_p: str, feat: list[str], input_f: str, ssa_l: list[str], 
        light_b: bool
    )-> list[str]:
    """Appends spatial features to File Geodatabase
    
    Appends spatial features from each SSURGO download to the respective 
    SSURGO feature. Note that SAPOLYGON should be appeneded first to aid with 
    spatial indexing by setting append order from NW extent of survey set.

    Parameters
    ----------
    gdb : str
        The path of the new SSURGO geodatabase.
    feat : tuple(str)
        Contains two strings. The first string is the SSURGO feature name, the 
        second string is the shapefile name.
    input_f str
        Folder with the unzipped SSURGO donwloads.
    ssa_l : list[str]
        List of soil survey areas to be appended.

    Returns
    -------
    list[str]
        Returns the list of soil survey areas. When the input feature is 
        SAPOLYGON it returns the list spatially sorted. If unsuccessful, it 
        returns an empty list.

    """
    try:
        arcpy.env.workspace = gdb_p
        gdb_n = os.path.basename(gdb_p)
        arcpy.env.geographicTransformations = 'WGS_1984_(ITRF00)_To_NAD_1983'
        feat_gdb = feat[0]
        feat_shp = feat[1]
        # if SAPOLYGON, set up temp file to append a spatially indexed version
        if (feat_gdb == 'SAPOLYGON'):
            feat_p = f"memory/SAPOLYGON_{gdb_n[:-4]}"
        elif light_b and (feat_gdb == 'MUPOLYGON'):
            feat_p = f"memory/MUPOLYGON_{gdb_n[:-4]}"
            mudis_p = f"memory/mudis_{gdb_n[:-4]}"
        else:
            feat_p = f"{gdb_p}/{feat_gdb}"

        # total count of features
        count = 0
        feat_l = []
        # Create list feature paths to append if not empty
        for ssa in ssa_l:
            shp = f"{input_f}/{ssa.upper()}/spatial/{feat_shp}_{ssa}.shp"
            if os.path.isfile(shp):
                cnt = int(arcpy.GetCount_management(shp).getOutput(0))
                count += cnt
            else: 
                # Statsgo
                if feat_gdb == 'MUPOLYGON':
                    shp = f"{input_f}/spatial/gsmsoilmu_a_{ssa}.shp"
                    if os.path.isfile(shp):
                        cnt = int(arcpy.GetCount_management(shp).getOutput(0))
                        count += cnt
                    else:
                        cnt = -1
                else:
                    cnt = -1
            if cnt > 0:
                feat_l.append(shp)

        # if there are features
        if feat_l:
            arcpy.SetProgressorLabel(f"\tAppending features to {feat_gdb}")
            if light_b and (feat_gdb == 'MUPOLYGON'):
                arcpy.CopyFeatures_management(f"{gdb_p}/{feat_gdb}", feat_p)
                arcpy.Append_management(feat_l, feat_p, "NO_TEST")
            elif feat_gdb != 'SAPOLYGON':
                arcpy.Append_management(feat_l, feat_p, "NO_TEST")
            # if only a single SSA, no need to sort
            elif len(ssa_l) == 1:
                feat_p = f"{gdb_p}/{feat_gdb}"
                arcpy.Append_management(feat_l, feat_p, "NO_TEST")
                return ssa_l
            # Make virtual copy of template SAPOLYOGN to preserve metadata
            else:
                arcpy.CopyFeatures_management(f"{gdb_p}/{feat_gdb}", feat_p)
                arcpy.Append_management(feat_l, feat_p, "NO_TEST")
                feat_temp = feat_p
                feat_p = f"{gdb_p}/{feat_gdb}"
                feat_desc = arcpy.Describe(feat_p)
                shp_fld = feat_desc.shapeFieldName
                # Spatially sort fron NW extent
                arcpy.management.Sort(feat_temp, feat_p, shp_fld, "UR")
                # Get SSA sort list
                sCur = arcpy.da.SearchCursor(feat_p, "areasymbol")
                sort_d = {ssa: None for ssa, in sCur}
                del sCur
                arcpy.Delete_management("memory")
                arcpy.management.AddSpatialIndex(feat_p)
                return tuple(sort_d.keys())

            cnt = int(arcpy.GetCount_management(feat_p).getOutput(0))
            if cnt == count:
                arcpy.AddMessage(
                    f"\t{cnt} features were appended to {feat_gdb}"
                )
                # Dissolve MUPOLYGON to a multipart
                if light_b and (feat_gdb == 'MUPOLYGON'):
                    arcpy.SetProgressorLabel('Dissolving MUPOLYGON feature')
                    # Repair Geometry, set parrallel
                    with arcpy.EnvManager(parallelProcessingFactor="100"):
                        output = arcpy.management.RepairGeometry(
                            in_features=feat_p,
                            delete_null="DELETE_NULL",
                            validation_method="OGC"
                        )
                        # Get Repair report
                        # arcpy.AddMessage(output.getMessages())
                        # Dissolve
                        fields = "SPATIALVER FIRST;AREASYMBOL FIRST;MUSYM FIRST"
                        arcpy.analysis.PairwiseDissolve(
                            in_features=feat_p,
                            out_feature_class=mudis_p,
                            dissolve_field="MUKEY",
                            statistics_fields=fields,
                            multi_part="MULTI_PART",
                            concatenation_separator=""
                        )
                    feat_p = f"{gdb_p}/{feat_gdb}"
                    # Append, table schema
                    schema = (
                        'AREASYMBOL "Area Symbol" true true false 10 Text 0 0,'
                        f'First,#,{mudis_p},FIRST_AREASYMBOL,0,20;'
                        'SPATIALVER "Spatial Version" true true false 2 Short '
                        f'0 0,First,#,{mudis_p},FIRST_SPATIALVER,-1,-1;'
                        'MUSYM "Map Unit Symbol" true true false 6 Text 0 0,'
                        f'First,#,{mudis_p},FIRST_MUSYM,0,6;'
                        'MUKEY "Map Unit Key" true true false 4 Long 0 0,'
                        f'First,#,{mudis_p},MUKEY,0,30'
                    )
                    arcpy.management.Append(
                        inputs=mudis_p,
                        target=feat_p,
                        schema_type="NO_TEST",
                        field_mapping=schema
                    )
                    arcpy.Delete_management("memory")
                arcpy.management.AddSpatialIndex(feat_p)
                return []
            else:
                arcpy.AddError(
                    f"\tOnly {cnt} of {count} features were "
                    f"appended to {feat_gdb}"
                )
                arcpy.Delete_management("memory")
                return ['incomplete']
        # There must be at least a single SAPOLYGON and MUPOLYGON feature
        elif (feat_gdb == 'SAPOLYGON') or (feat_gdb == 'MUPOLYGON'):
            arcpy.AddMessage(f"\tThere were no features appended to {feat_gdb}")
            arcpy.Delete_management("memory")
            return ['empty error']
        # No MUPOINT, MULINE, or special features
        else:
            arcpy.AddMessage(f"\tThere were no {feat_gdb} features")
            return ['empty']

    except arcpy.ExecuteError:
        arcpy.Delete_management("memory")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return ['error']
    except:
        arcpy.Delete_management("memory")
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return ['error']


def updateMetadata(gdb_p: str,
                   target: str,
                   survey_i: str,
                   description: str,
                   state_l: list[str]
    ) -> list[str]:
    """ Used for featureclass and geodatabase metadata. Does not do individual 
    tables. Reads and edits the original metadata object and then exports the 
    edited version back to the featureclass or geodatabase.

    Parameters
    ----------
    gdb_p : str
        Path of the SSURGO geodatabase.
    target : str
        feature or geodatabse the metadata is being updated for.
    survey_i : str
        Summary string of the Survey Area Version date by soil survey.
    description : str
        _description_

    Returns
    -------
    list[str]
        Collection of messages, no messages means function was completely 
        successful.
    """
    try:
        msg = []
        gdb_n = os.path.basename(gdb_p)
        msgAppend = msg.append
        place_str = f"{description}, {state_l}"
        # initial metadata exported from current target featureclass
        meta_export = env.scratchFolder + f"/xxExport_{gdb_n}.xml"
        # the metadata xml that will provide the updated info
        meta_import = env.scratchFolder + f"/xxImport_{gdb_n}.xml"
        # Cleanup XML files from previous runs
        if os.path.isfile(meta_import):
            os.remove(meta_import)
        if os.path.isfile(meta_export):
            os.remove(meta_export)
        meta_src = arcpy.metadata.Metadata(target)
        meta_src.exportMetadata(meta_export, 'FGDC_CSDGM')

        # Set date strings for metadata, based upon today's date
        d = datetime.date.today()
        fy = d.strftime('%Y%m')
        # ---- call getLastDate
        tbl = gdb_p + "/SACATALOG"
        sqlClause = [None, "ORDER BY SAVEREST DESC"]

        sCur = arcpy.da.SearchCursor(
            tbl, ['SAVEREST'], sql_clause = sqlClause
        )
        row = next(sCur)[0]
        lastDate = row.strftime('%Y%m%d')
        del sCur
        # Parse exported XML metadata file
        # Convert XML to tree format
        tree = ET.parse(meta_export)
        root = tree.getroot()

        # new citeInfo has title.text, edition.text, serinfo/issue.text
        for child in root.findall('idinfo/citation/citeinfo/'):
            if child.tag == "title":
                if child.text.find('xxSTATExx') >= 0:
                    child.text = child.text.replace('xxSTATExx', description)
                # elif place_str != "":
                #     child.text = child.text + " - " + description
            elif child.tag == "edition":
                if child.text == 'xxFYxx':
                    child.text = fy
            elif child.tag == "serinfo":
                for subchild in child.iter('issue'):
                    if subchild.text == "xxFYxx":
                        subchild.text = fy

        # Update place keywords
        ePlace = root.find('idinfo/keywords/place')
        for child in ePlace.iter('placekey'):
            if child.text == "xxSTATExx":
                child.text = place_str
            elif child.text == "xxSURVEYSxx":
                child.text = survey_i

        # Update credits
        eIdInfo = root.find('idinfo')

        for child in eIdInfo.iter('datacred'):
            sCreds = child.text
            if sCreds.find("xxSTATExx") >= 0:
                child.text = child.text.replace("xxSTATExx", description)
            if sCreds.find("xxFYxx") >= 0:
                child.text = child.text.replace("xxFYxx", fy)
            if sCreds.find("xxTODAYxx") >= 0:
                child.text = child.text.replace("xxTODAYxx", lastDate)

        idPurpose = root.find('idinfo/descript/purpose')
        if not idPurpose is None:
            ip = idPurpose.text
            if ip.find("xxFYxx") >= 0:
                idPurpose.text = ip.replace("xxFYxx", fy)

        procDates = root.find('dataqual/lineage')
        if not procDates is None:
            for child in procDates.iter('procdate'):
                sDate = child.text
                if sDate.find('xxTODAYxx'):
                    child.text = lastDate
        else:
            msgAppend("Process date not found")

        #  create new xml file which will be imported, 
        # thereby updating the table's metadata
        tree.write(
            meta_import, 
            encoding = "utf-8", 
            xml_declaration = None, 
            default_namespace = None, 
            method = "xml"
        )

        # import updated metadata to the geodatabase feature
        meta_src.importMetadata(meta_import, "FGDC_CSDGM")
        meta_src.deleteContent('GPHISTORY')
        meta_src.save()

        # delete the temporary xml metadata files
        if os.path.isfile(meta_import):
            os.remove(meta_import)
        if os.path.isfile(meta_export):
            os.remove(meta_export)
        del meta_src

        return msg
    except arcpy.ExecuteError:
        try:
            tree.write(
                meta_import, 
                encoding = "utf-8", 
                xml_declaration = None, 
                default_namespace = None, 
                method = "xml"
            )
            meta_src.save()
            del meta_src
        except:
            pass
        func = sys._getframe().f_code.co_name
        msgAppend(arcpy.AddError(arcpyErr(func)))
        return msg
    except:
        try:
            tree.write(
                meta_import, 
                encoding = "utf-8", 
                xml_declaration = None, 
                default_namespace = None, 
                method = "xml"
            )
            meta_src.save()
            del meta_src
        except:
            pass
        func = sys._getframe().f_code.co_name
        msgAppend(arcpy.AddError(pyErr(func)))
        return msg


def gSSURGO(input_p: str,
            survey_l: list[str],
            gdb_p: str,
            aoi: str,
            label: str,
            light_b: bool,
            module_p: str,
            gssurgo_v: str,
            v: str
    ) -> str:
    """This function is the backbone of the fgdb.py module. 
    It calls these functions to create and populate a SSURGO geodatabase: 
    1) ``CreateGDB`` to create a geodatabase using an xml template
    2) ``appendFeatures`` to append to the spatial features from each 
    SSURGO folder.
    3) ``importSing`` imports tabels that are idential in each SSURGO folder.
    4) ``importSet`` imports tabels that are largely indentical, with some
    novelty.
    5) ``importList`` imports tabels with unique information to each SSURGO
    dataset.
    6) ``createTableRelationships`` Establishes relationships between tables
    to other tables or spatial features.
    7) ``updateMetadata`` Update the geodatabase and spatial features 
    metadata.

    Parameters
    ----------
    input_p : str
        Directory locatoin of the SSURGO downloads.
    survey_l : list[str]
        List of soil surveys to be imported into the SSURGO geodatabase.
    gdb_p : str
        The path of the SSURGO file geodatabase to be created.
    aoi : str
        Defining the region of the SSURGO dataset for defining xml with the
        appropriate projection.
    label : str
        _description_

    Returns
    -------
    str
        Returns an empty string if a SSURGO geogdatabase is successfully 
        created, otherwise returns an error message.
    """
    try:
        env.overwriteOutput= True
        gdb_n = os.path.basename(gdb_p)
        gdb_n = gdb_n.replace("-", "_")
        surveyCount = len(survey_l)
        date_format = "(%Y-%m-%d)"
        # Get the XML Workspace Document appropriate for the specified aoi
        # %% check 1
        # ---- make xml
        inputXML = xml(aoi, module_p, gssurgo_v)
        if not inputXML.exist:
            arcpy.AddError(" \nMissing xml file: " + inputXML.xml)
            return False
        # %% check 1
        # ---- call createGDB
        gdb_b = createGDB(gdb_p, inputXML, label)
        if not gdb_b:
            arcpy.AddMessage(f"Didn't successfully create {gdb_n}\n")
            return False
        # %% check 1
        # ---- call appendFeatures
        # gdb feature name paired with shapefile root name.
        features = [
            ('SAPOLYGON', 'soilsa_a'),
            ('MUPOLYGON', 'soilmu_a'),
            ('MULINE', 'soilmu_l'),
            ('MUPOINT', 'soilmu_p'),
            ('FEATLINE', 'soilsf_l'),
            ('FEATPOINT', 'soilsf_p')
        ]
        # SAPOLYGON must be run first to sort `survey_l`
        # if len(survey_l) > 1:
        outcome = appendFeatures(gdb_p, features[0], input_p, survey_l, light_b)
        if outcome:
            survey_l = outcome
        else:
            return
        for feat in features[1:]:
            output = appendFeatures(gdb_p, feat, input_p, survey_l, light_b)
            if not output:
                arcpy.AddMessage(
                    f"\tSuccessfully appended {feat[0]}"
                )
            elif output[0] != 'empty':
                arcpy.AddError(f"Failed to append {feat[0]}")
                arcpy.AddError(output)
                return
        gc.collect()
        arcpy.SetProgressorPosition()
        arcpy.ResetProgressor()

        # ---- call importSing
        arcpy.SetProgressorLabel("Importing constant tables")
        table_d = importSing(survey_l[0], input_p, gdb_p)
        if 'Error' in table_d:
            arcpy.AddError(table_d['Error'])
            return
        arcpy.SetProgressorLabel("Importing table sets")
        msg = importSet(survey_l, input_p, gdb_p, table_d)
        if msg:
            arcpy.AddError(msg)
            return
        # Tables which are unique to each SSURGO soil survey area
        arcpy.SetProgressorLabel("Importing unique tables")
        tabs_uniq = [
            'component', 'cosurfmorphhpp', 'legend', 'chunified','cocropyld',
            'chtexturegrp', 'cosurfmorphss', 'coforprod', 'sacatalog',
            'cosurfmorphgc', 'cotaxmoistcl', 'chtext', 'chconsistence',
            'chtexture', 'copmgrp', 'cosoilmoist', 'mucropyld', 'chtexturemod',
            'cotext', 'coecoclass', 'cosurfmorphmr', 'cosurffrags',
            'cotreestomng', 'cosoiltemp', 'sainterp', 'chstructgrp',
            'distlegendmd', 'copwindbreak', 'chdesgnsuffix', 'corestrictions',
            'cotaxfmmin', 'chstruct', 'chfrags', 'coforprodo', 'distmd',
            'mutext', 'legendtext', 'muaggatt', 'chorizon', 'cohydriccriteria',
            'chpores', 'chaashto', 'coerosionacc', 'copm', 'comonth',
            'muaoverlap', 'cotxfmother', 'mapunit', 'coeplants', 'laoverlap',
            'cogeomordesc', 'codiagfeatures', 'cocanopycover'
        ]
        # Exclude these cointerp columns
        # interpll, interpllc, interplr, interplrc, interphh, interphhc
        exclude_i = {8, 9, 10, 11, 14, 15}
        table_d['cointerp'][2] = [
            cols for cols in table_d['cointerp'][2] if cols[0] not in exclude_i
        ]
        if gssurgo_v != '1.0':
            tabs_uniq.remove('sainterp')
        # If light, exclude interp rules, except NCCPI
        else:
            co_out = importCoint(survey_l, input_p, gdb_p, table_d, light_b)
            if co_out:
                arcpy.AddError(co_out)
                return False

        # Create parameter dictionary with gdb table name and text file folder
        paramSet = [
            {'table': tab, 'sub_fld': 'tabular'} for tab in tabs_uniq
        ]
        txt = 'soilsf_t_{ssa}'
        table_d['featdesc'][0] = txt
        paramSet.append({'table': 'featdesc', 'sub_fld': 'spatial'})
        constSet = {
            'ssa_l': survey_l, 
            'input_p': input_p, 
            'gdb_p': gdb_p, 
            'table_d': table_d
        }
        # threadCount = 1 #psutil.cpu_count() // psutil.cpu_count(logical=False)
        # arcpy.AddMessage(f"{threadCount= }")
        import_all = True
        ti = time.time()
        import_jobs = funYield(importList, paramSet, constSet)
        for paramBack, output in import_jobs:
        # for paramBack in paramSet:
            # output = importList(**paramBack, **constSet)
            try:
                # if not output:
                    # arcpy.AddMessage(
                        # f"\tSuccessfully populated {paramBack['table']}"
                    # )
                # else:
                if output:
                    # arcpy.AddError(f"Failed to populate {paramBack['table']}")
                    arcpy.AddError(output)
                    import_all = False
            except GeneratorExit:
                arcpy.AddWarning("passed")
                arcpy.AddWarning(f"{paramBack}")
                arcpy.AddWarning(f"{output}")
                pass
        import_jobs.close()
        del import_jobs
        gc.collect()
        if not import_all:
            return False
        # arcpy.AddMessage(f"time: {time.time() - ti}")

        if not versionTab(input_p, gdb_p, gssurgo_v, light_b, v, survey_l[0]):
            arcpy.AddWarning('Version table failed to populate successfully.')

        if gssurgo_v != '1.0':
            table_d['mdruleclass'] = ['NA', 'Rule Class Text Metadata', ()]
            table_d['mdrule'] = ['NA', 'Interpretation Rules Metadata', ()]
            table_d['mdinterp'] = ['NA', 'Interpretations Metadata', ()]
            msg = schemaChange(
                gdb_p, input_p, module_p, table_d, survey_l, light_b)
            # if msg:
            #     arcpy.AddWarning(msg)

        # Create Indices
        if not createIndices(gdb_p, module_p, gssurgo_v):
            arcpy.AddWarning(
                "Failed to create indices which may imparct efficient use of "
                "database."
            )

        # Create table relationships and indexes
        # ---- call createTableRelationships
        rel_b = createTableRelationships(gdb_p, gssurgo_v, module_p)
        if not rel_b:
            return False
        
        # Query the output SACATALOG table to get list of surveys that were 
        # exported to the gSSURGO
        arcpy.AddMessage("\tUpdating metadata...")
        tab_sac = f"{gdb_p}/sacatalog"

        # Areasymbol and Survey Area Version Established
        sCur = arcpy.da.SearchCursor(tab_sac, ["AREASYMBOL", "SAVEREST"])
        export_query = [
            (f"{ssa} {date_obj.strftime(date_format)}", f"'{ssa}'")
            for ssa, date_obj in sCur
        ]
        del sCur
        # survey_i format: NM007 (2022-09-08)
        # query_i format: 'NM007'
        survey_i, query_i = map(','.join, zip(*export_query))

        q = "areatypename = 'State or Territory'"
        sCur = arcpy.da.SearchCursor(f"{gdb_p}/laoverlap", 'areasymbol', q)
        state_overlaps = {states[st] for st, in sCur}
        del sCur
        state_overlaps = ', '.join(state_overlaps)
        # Update metadata for the geodatabase and all featureclasses
        arcpy.SetProgressorLabel("Updating metadata...")
        md_l = [
            gdb_p, 
            f"{gdb_p}/FEATLINE",
            f"{gdb_p}/FEATPOINT",
            f"{gdb_p}/MUPOINT",
            f"{gdb_p}/MULINE",
            f"{gdb_p}/MUPOLYGON",
            f"{gdb_p}/SAPOLYGON"
        ]

        for target in md_l:
            # ---- call updateMetadata
            description = ''
            msgs = updateMetadata(
                gdb_p, target, survey_i, label, state_overlaps
            )
            if msgs:
                for msg in msgs:
                    arcpy.AddError(msg)

        arcpy.SetProgressorLabel("\tCompacting new database...")
        arcpy.Compact_management(gdb_p)

        env.workspace = os.path.dirname(env.scratchFolder)

        arcpy.AddMessage(
            f"Successfully created {gdb_p} "
            f"\nWhich includes the following surveys:"
        )
        for line in re.findall('.{1,80}\W', query_i):
            arcpy.AddMessage(line.replace("'", " "))
        return True

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def createIndices(gdb_p: str, module_p: str, gssurgo_v: str) -> bool:
    """Creates attribute indices for the specified table attribute fields.
    As any field involved with a Relationship Class is already indexed,
    therefore the  mdstatidxdet and mdstatidxmas tables are not referenced. 
    Instead, a consolidated csv file, relative to the gSSURGO version, 
    is referenced.

    Parameters
    ----------
    gdb_p : str
        The geodatabase path.
    module_p : str
        The path to the sddt module.
    gssurgo_v : str
        The gSSURGO version

    Returns
    -------
    bool
        Returns True if all indices were successfully created, otherwis 
        False.
    """
    try:
        arcpy.AddMessage('\n\tAdding attribute indices...')
        # Any field involved with a Relationship Class is already indexed
        if (gssurgo_v.split('.')[0]) == '2':
            csv_p = module_p + "/md_index_insert2.csv"
        else:
            csv_p = module_p + "/md_index_insert1.csv"
        with open(csv_p, newline='') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            # Sequence, Unique, ascending are irrelavent in FGDB's
            arcpy.SetProgressorLabel("Creating indexes")
            for tab_n, idx_n, seq, col_n, uk in csv_r:
                if uk == 'Yes':
                    un_b = "UNIQUE"
                else:
                    un_b = "NON_UNIQUE"
                tab_p = f"{gdb_p}/{tab_n}"
                arcpy.management.AddIndex(tab_p, col_n, idx_n, un_b)
        return True
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(f"{tab_p= } {col_n= } {un_b= }")
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def createTableRelationships(gdb_p: str, gssurgo_v: str, module_p: str) -> str:
    """Creates the tabular relationships between the SSRUGO tables using arcpy
    CreateRelationshipClass function. These relationship classes are defined in 
    the mdstatrshipdet and mdstatrshipmas metadata tables. Note that the 
    seven spatial relationships classes were inherited from the xml workspace.

    Parameters
    ----------
    gdb_p : str
        The path of the new geodatabase with the recently imported SSURGO 
        tables.

    Returns
    -------
    str
        An empty string if successful, an error message if unsuccessful.

    """
    try:
        arcpy.AddMessage(
            "\n\tCreating table relationships on key fields..."
        )
        env.workspace = gdb_p

        if (arcpy.Exists(f"{gdb_p}/mdstatrshipdet")
            and arcpy.Exists(f"{gdb_p}/mdstatrshipmas")):
            tbl1 = f"{gdb_p}/mdstatrshipmas"
            tbl2 = f"{gdb_p}/mdstatrshipdet"
            flds1 = ['ltabphyname', 'rtabphyname']
            flds2 = [
                'ltabphyname', 'rtabphyname', 'ltabcolphyname', 'rtabcolphyname'
            ]
            # Create a set of all table to table relations in mdstatrshipmas
            sCur = arcpy.da.SearchCursor(tbl1, flds1)
            relSet = {(ltab, rtab) for ltab, rtab in sCur}
            del sCur
            # if table to table relationship defined in mdstatrshipmas, then 
            # create relationship with column names from mdstatrshipdet
            sCur = arcpy.da.SearchCursor(tbl2, flds2)
            for ltab, rtab, lcol, rcol in sCur:
                if (ltab, rtab) in relSet:
                    # left table: Destination table
                    # left column: Destination Foreign Key
                    # right table: Origin Table
                    # right column: Origin Primary Key
                    rel_n = f"z_{ltab.lower()}_{rtab.lower()}"
                    # create Forward Label i.e. "> Horizon AASHTO Table"
                    fwdLabel = f"on {lcol}"
                    # create Backward Label i.e. "< Horizon Table"
                    backLabel = f"on {rcol}"
                    arcpy.SetProgressorLabel(
                        "Creating table relationship "
                        f"between {ltab} and {rtab}"
                    )
                    arcpy.management.CreateRelationshipClass(
                        f"{gdb_p}/{ltab}", f"{gdb_p}/{rtab}", rel_n, "SIMPLE",
                        fwdLabel, backLabel, "NONE", "ONE_TO_MANY", "NONE",
                        lcol, rcol
                    )
            del sCur
            # Create relationships for spatial features in version 1.0
            if gssurgo_v == '1.0':
                csv_p = module_p + "/md_relationships_insert1.csv"
                with open(csv_p, newline='') as csv_f:
                    csv_r = csv.reader(csv_f, delimiter=',')
                    hdr = next(csv_r)
                    row_det_l = []
                    for ltab, rtab, _1, _2, _3, lcol, rcol in csv_r:
                        rel_n = f"z_{ltab.lower()}_{rtab.lower()}"
                        # create Forward Label i.e. "> Horizon AASHTO Table"
                        fwdLabel = f"on {lcol}"
                        # create Backward Label i.e. "< Horizon Table"
                        backLabel = f"on {rcol}"
                        arcpy.SetProgressorLabel(
                            "Creating table relationship "
                            f"between {ltab} and {rtab}"
                        )
                        arcpy.management.CreateRelationshipClass(
                            f"{gdb_p}/{ltab}", f"{gdb_p}/{rtab}", rel_n,
                            "SIMPLE", fwdLabel, backLabel, "NONE",
                            "ONE_TO_MANY", "NONE", lcol, rcol
                        )
            return True
        else:
            return("Missing mdstatrshipmas and/or mdstatrshipdet tables,"
                   "relationship classes not created")
    except arcpy.ExecuteError:
        try:
            del sCur
        except:
            pass
        arcpy.AddMessage(
            f"{gdb_p}/{rtab}, {gdb_p}/{ltab}, {rel_n}, SIMPLE, "
            f"{fwdLabel}, {backLabel}, NONE, ONE_TO_MANY, NONE, {rcol}, {lcol}"
        )
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        try:
            del sCur
        except:
            pass
        arcpy.AddMessage(
            f"{gdb_p}/{rtab}, {gdb_p}/{ltab}, {rel_n}, SIMPLE, "
            f"{fwdLabel}, {backLabel}, NONE, ONE_TO_MANY, NONE, {rcol}, {lcol}"
        )
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def schemaChange(
        gdb_p: str, input_p: str, module_p: str, 
        table_d: dict[str, list[str, str, list[tuple[int, str]]]], 
        ssa_l: list[str], light: bool
    ) -> bool:
    """This function reconciles differences in importing and schemas between 
    gSSURGO versions.
    One of the most signifcant differences in schema between version 1.0 and 
    2.0 is the structure of the cointerp and sainterp tables. Also, three 
    additional tables related to this restructuring have been added, the 
    mdrule, mdinterp and mdruleclass tables.
    Various csv files are read from sddt/construct module folder to assist 
    in this. Each csv file as a version number <v>.
    md_column_update<v>.csv: List of changes/deletion of columns
    md_column_insert<v>.csv: List of new columns
    md_index_insert<v>.csv: List of new indices to added
    md_index_delete<v>.csv: List of indices to be removed
    md_relationships_insert<v>.csv: List of relationships to add
    md_rule_classes.csv: List of unique interpretation value classes
    md_tables_insert<v>.csv: List of new tables

    Parameters
    ----------
    gdb_p : str
        Path of the SSURGO file geodatabase.
    input_p : str
        Directory with the SSURGO datasets.
    module_p : str
        path to the sddt module
    table_d : dict[str, list[str, str, list[tuple[int, str]]]]
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name.
    ssa_l : list[str]
        List of SSURGO datasets to be imported.
    light : bool
        Indicates if a concise dataset is being selected. This is pertinent
        to the populating of the cointerp table.

    Returns
    -------
    bool
        Returns True if successful, otherwise False.
    """

    try:
        arcpy.env.workspace = gdb_p
        # Update mdstattabs table
        # Add mdinterp, mdrule, mdruleclass tables
        mdtab_p = gdb_p + "/mdstattabs"
        mdtab_cols = table_d['mdstattabs'][2]
        mdtab_cols.sort()
        mdtab_cols = [col[1] for col in mdtab_cols]
        iCur = arcpy.da.InsertCursor(mdtab_p, [mdtab_cols])
        csv_p = module_p + "/md_tables_insert2.csv"
        with open(csv_p, newline='') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            for row in csv_r:
                iCur.insertRow(row)
        del iCur

        # update mdstattabcols
        # update field lengths and/or datatype, i.e. make keys numeric
        mdcols_p = gdb_p + "/mdstattabcols"
        mdcols_cols = table_d['mdstattabcols'][2]
        mdcols_cols.sort()
        mdcols_cols = [col[1] for col in mdcols_cols]
        csv_p = module_p + "/md_column_update2.csv"
        # collect list of column updates
        with open(csv_p, newline='') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            # Table: Column: [type, length, sequence]
            col_updates = {}
            for row in csv_r:
                if (table := row[0]) in col_updates:
                    col_updates[table].update({row[1]: row[4:]})
                else:
                    col_updates[table] = {row[1]: row[4:]}
        # Update mdstattabcols table
        uCur = arcpy.da.UpdateCursor(mdcols_p, mdcols_cols)
        d = 0
        u = 0
        for col_row in uCur:
            if (table := col_row[0]) in col_updates:
                tab_updates = col_updates[table]
                if (col := col_row[2]) in tab_updates:
                    d_type, col_l, seq = tab_updates[col]
                    if d_type.lower() != 'delete':
                        # update sequence if updated
                        col_row[1] = seq or col_row[1]
                        # update data type
                        col_row[5] = d_type
                        # update length
                        col_row[7] = col_l or None
                        uCur.updateRow(col_row)
                        tab_updates.pop(col)
                        u += 1
                    else:
                        uCur.deleteRow()
                        tab_updates.pop(col)
                        d += 1
        # arcpy.AddWarning(col_updates)
        del uCur

        # Add new columns
        csv_p = module_p + "/md_column_insert2.csv"
        with open(csv_p, newline='') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            iCur = arcpy.da.InsertCursor(mdcols_p, mdcols_cols)
            for row in csv_r:
                iCur.insertRow(tuple(v or None for v in row))
        del iCur

        # Add new Relationships to mdstatrshipmas and mdstatrshipdet tables
        mdrel_stat_p = gdb_p + '/mdstatrshipmas'
        mdrel_stat_cols = table_d['mdstatrshipmas'][2]
        mdrel_stat_cols .sort()
        mdrel_stat_cols  = [col[1] for col in mdrel_stat_cols]
        mdrel_det_p = gdb_p + '/mdstatrshipdet'
        mdrel_det_cols = table_d['mdstatrshipdet'][2]
        mdrel_det_cols.sort()
        mdrel_det_cols  = [col[1] for col in mdrel_det_cols]
        iCur = arcpy.da.InsertCursor(mdrel_stat_p, mdrel_stat_cols)
        csv_p = module_p + "/md_relationships_insert2.csv"
        with open(csv_p, newline='') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            row_det_l = []
            for row in csv_r:
                row_stat = row[0:5]
                row_det = row[0:3] + row[-2:]
                iCur.insertRow(row_stat)
                row_det_l.append(row_det)
        del iCur
        iCur = arcpy.da.InsertCursor(mdrel_det_p, mdrel_det_cols)
        for row in row_det_l:
            iCur.insertRow(row)
        del iCur

        # Update mdstatidxmas and mdstatidxdet tables
        mdid_stat_p = gdb_p + '/mdstatidxmas'
        mdid_stat_cols = table_d['mdstatidxmas'][2]
        mdid_stat_cols.sort()
        mdid_stat_cols  = [col[1] for col in mdid_stat_cols]
        mdid_det_p = gdb_p + '/mdstatidxdet'
        mdid_det_cols = table_d['mdstatidxdet'][2]
        mdid_det_cols.sort()
        mdid_det_cols  = [col[1] for col in mdid_det_cols]
        # delete obsolete indices
        csv_p = module_p + "/md_index_delete2.csv"
        with open(csv_p, newline='') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            idx_delete = {row[0]: row[1] for row in csv_r}
        uCur = arcpy.da.UpdateCursor(mdid_stat_p, mdid_stat_cols[:2])
        for table, col in uCur:
            if table in idx_delete:
                if idx_delete[table] == col:
                    uCur.delteRow()
        del uCur
        uCur = arcpy.da.UpdateCursor(mdid_det_p, mdid_det_cols[:2])
        for table, col in uCur:
            if table in idx_delete:
                if idx_delete[table] == col:
                    uCur.delteRow()
        del uCur

        # Insert new indices
        iCur = arcpy.da.InsertCursor(mdid_stat_p, mdid_stat_cols)
        csv_p = module_p + "/md_index_insert2.csv"
        with open(csv_p, newline='') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            idx_det_l = []
            for row in csv_r:
                iCur.insertRow(row[:2] + [row[-1]])
                idx_det_l.append(row[0:4])
        del iCur
        iCur = arcpy.da.InsertCursor(mdid_det_p, mdid_det_cols)
        for row in idx_det_l:
            iCur.insertRow(row)
        del iCur

        # Populate mdruleclass table
        # leave iCur open in case new interp classes found
        csv_p = module_p + "/md_rule_classes2.csv"
        crt_p = gdb_p + "/mdruleclass"
        iCur = arcpy.da.InsertCursor(crt_p, ['classtxt', 'classkey'])
        # rule class text: class key
        class_d = {}
        with open(csv_p, newline='') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            for class_txt, class_i in csv_r:
                class_i = int(class_i)
                iCur.insertRow([class_txt, class_i])
                class_d[class_txt] = class_i
        class_sz = len(class_d)
        del iCur
        arcpy.AddMessage("\tSuccessfully populated mdruleclass")

        # Read cinterp.txt
            # exclude non-main rule cotinterps if light
            # except for NCCPI rules (main rule 54955)
        nccpi_sub = ['37149', '37150', '44492', '57994']
        arcpy.SetProgressorLabel("importing cointerp")
        co_tbl = 'cointerp'
        q = "tabphyname = 'cointerp'"
        sCur = arcpy.da.SearchCursor(mdcols_p, ['colsequence', 'colphyname'], q)
        coi_cols = [row for row in sCur]
        del sCur
        # get fields in sequence order
        coi_cols.sort()
        fields = [f[1] for f in coi_cols]
        txt = table_d[co_tbl][0]
        co_p = f"{gdb_p}/{co_tbl}"
        iCur = arcpy.da.InsertCursor(co_p, fields)
        # collate interpration names with key as sainterp.txt lacks key
        # interpname: interpkey
        interp_d = {}
        # don't simultaneously populate mdrule as there is are many to one
        # (interpkey, rulekey): [rulename, ruledepth, seq]
        rule_d = {}
        for ssa in ssa_l:
            # Make file path for text file
            txt_p = f"{input_p}/{ssa.upper()}/tabular/{txt}.txt"
            if not os.path.exists(txt_p):
                return f"{txt_p} does not exist"
            csvReader = csv.reader(
                open(txt_p, 'r'), delimiter='|', quotechar='"'
            )
            for row in csvReader:
                # replace empty sets with None
                row = [v or None for v in row]
                interp_k = row[1]
                rule_k = row[4]
                # Add to new (rules, interps) to dict to populate mdrule table
                if (interp_k, rule_k) not in rule_d:
                    rule_d[(interp_k, rule_k)] = [
                        *row[5:7], row[3]
                    ]

                # If its a rule not an interp, 
                # the rule and interp keys (mrulekey) are not equal
                if interp_k != rule_k:
                    # An NCCPI rule (some SDV Attributes based on them)
                    # OR not light (all rules included in cointerp)
                    if rule_k in nccpi_sub or not light:
                        class_txt = row[12]
                        class_k = class_d.get(class_txt)
                        # Possible that new classes come with new interps
                        if not class_k:
                            # increment class key
                            class_i += 1
                            class_k = class_i
                            class_d[class_txt] = class_k
                            arcpy.AddMessage(
                                f"New rule class found: {class_txt}"
                            )
                        nulls = [v.strip() for v in row[15:18]]
                        iCur.insertRow([
                            row[11], class_k, *nulls, rule_k, interp_k, row[0],
                            row[18]
                        ])
                # an interp
                else:
                    # Collect interps 
                    if (rule_n := row[2]) not in interp_d:
                        # Collect interp keys (main rule keys) by name to add to
                        # sainterp and mdinterp tables
                        interp_d[rule_n] = interp_k
                    class_txt = row[12]
                    class_k = class_d.get(class_txt)
                    # Possible that new classes come with new interps
                    if not class_k:
                        # increment class key
                        class_i += 1
                        class_k = class_i
                        class_d[class_txt] = class_k
                        arcpy.AddMessage(f"New rule class found: {class_txt}")
                    # some zeros have a space after them
                    nulls = [v.strip() for v in row[15:18]]
                    iCur.insertRow([
                        row[11], class_k, *nulls, rule_k, interp_k, row[0], 
                        row[18]
                    ])
        arcpy.AddMessage("\tSuccessfully populated cointerp")
        del iCur
        # insert any new found interp classes
        if len(class_d) != class_sz:
            arcpy.AddMessage(f"Adding {len(class_d)} new interp classes")
            iCur = arcpy.da.InsertCursor(crt_p, ['classtxt', 'classkey'])
            for class_txt, class_k in class_d.items():
                iCur.insertRow([class_txt, class_k])
            del iCur
        # Delete rulekey if light

        # Populate mdrule table
        mdr_p = gdb_p + "/mdrule"
        fields = [
            'rulename', 'ruledepth', 'seqnum', 'interpkey', 'rulekey'
        ]
        iCur = arcpy.da.InsertCursor(mdr_p, fields)
        for k, v in rule_d.items():
            # [rulename, ruledepth, seq], [interpkey, rulekey]
            iCur.insertRow([*v, *k])
        del iCur
        arcpy.AddMessage("\tSuccessfully populated mdrule")

        # Sainterp table
        sa_tbl = 'sainterp'
        q = "tabphyname = 'sainterp'"
        sCur = arcpy.da.SearchCursor(mdcols_p, ['colsequence', 'colphyname'], q)
        sa_cols = [row for row in sCur]
        del sCur
        # get fields in sequence order
        sa_cols.sort()
        fields = [f[1] for f in sa_cols]
        txt = table_d[sa_tbl][0]
        sa_p = f"{gdb_p}/{sa_tbl}"
        iCur = arcpy.da.InsertCursor(sa_p, fields)
        # interp key: first 7 elements from sintperp
        mdinterp_d = {}
        for ssa in ssa_l:
            # Make file path for text file
            txt_p = f"{input_p}/{ssa.upper()}/tabular/{txt}.txt"
            if not os.path.exists(txt_p):
                return f"{txt_p} does not exist"
            csvReader = csv.reader(
                open(txt_p, 'r'), delimiter='|', quotechar='"'
            )
            for row in csvReader:
                # replace empty sets with None
                row = tuple(v or None for v in row)
                interp_n = row[1]
                interp_k = interp_d.get(interp_n)
                if interp_k not in mdinterp_d:
                    # Get interp info to populate mdinterp
                    mdinterp_d[interp_k] = row[1:7]
                iCur.insertRow([interp_k, *row[-2:]])
        del iCur
        arcpy.AddMessage("\tSuccessfully populated sainterp")

        # populate mdinterp table
        mdi_p = gdb_p + "/mdinterp"
        fields = [
            'interpname', 'interptype', 'interpdesc', 'interpdesigndate',
            'interpgendate', 'interpmaxreasons', 'interpkey'
        ]
        iCur = arcpy.da.InsertCursor(mdi_p, fields)
        for k, vals in mdinterp_d.items():
            iCur.insertRow([*vals, k])
        del iCur
        arcpy.AddMessage("\tSuccessfully populated mdinterp")
        return True

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        try:
            del uCur
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        try:
            del iCur
        except:
            pass
        try:
            del uCur
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def versionTab(input_p, gdb_p, gssurgo_v, light, script_v, ssa):
    try:
            # populate version table
        txt_p = f"{input_p}/{ssa.upper()}/tabular/version.txt"
        if not os.path.exists(txt_p):
            ssurgo_v = 'NA'
        else:
            csvReader = csv.reader(
                open(txt_p, 'r'), delimiter='|', quotechar='"'
            )
            ssurgo_v = next(csvReader)[0]
            del csvReader
        esri_i = arcpy.GetInstallInfo()
        # File Geodatabase version
        # https://pro.arcgis.com/en/pro-app/latest/arcpy/functions/
        # workspace-properties.htm
        gdb_v = arcpy.Describe(gdb_p).release
        if gdb_v == '3,0,0':
            gdb_v = '10.0'
        version_d = {
            'ssurgo': ('Data Source', 'SSURGO', ssurgo_v),
            'gSSURGO': ('Data Model', 'gSSURGO', gssurgo_v),
            'OS': (
                'Operating System', "Microsoft " + platform.system(),
                platform.version()
            ),
            'ESRI': (
                'GIS application', 'ESRI: ' + esri_i['ProductName'],
                esri_i['Version']
            ),
            'Python': (
                'Prgramming language', 'Python', platform.python_version()
            ),
            'FGDB': ('Database', 'File Geodatabase', gdb_v),
            'script': (
                'Script', 'SDDT: Create SSURGO File Geodatabase', script_v
            )
        }
        if light:
            version_d['abbrev1'] = ('Abbreviation Level', 'cointerp', '0.5')
            version_d['abbrev2'] = ('Abbreviation Level', 'MUPOLYGON', '1.0')
        else:
            version_d['abbrev1'] = ('Abbreviation Level', 'cointerp', '0.0')
            version_d['abbrev2'] = ('Abbreviation Level', 'MUPOLYGON', '0.0')
        version_p = f"{gdb_p}/version"
        iCur = arcpy.da.InsertCursor(version_p, ['type', 'name', 'version'])
        for vals in version_d.values():
            iCur.insertRow([*vals])
        del iCur
        arcpy.AddMessage("\tSuccessfully populated version")
        return True
    
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def main(args):
    # %% m
    try:
        v = '0.2'
        arcpy.AddMessage("Create SSURGO File GDB, version: " + v)
        # location of SSURGO datasets containing SSURGO downloads
        input_p = args[0]
        option = args[1]
        survey_str = args[2]
        state_str = args[3]
        ssa_lyr = args[4]
        geog_lyr = args[5]
        geog_fld = args[6]
        geog_str = args[7]
        label = args[8]
        clip_b = args[9]
        output_p = args[10]
        aoi = args[11]
        light_b = args[12]
        gssurgo_v = args[13]
        module_p = args[14]

        if gssurgo_v == 'gSSURGO 2.0':
            gssurgo_v = '2.0'
        else:
            gssurgo_v = '1.0'
        
        # This is the SSURGO version supported by this script and the 
        # gSSURGO schema (XML Workspace document)
        dbVersion = 2 
        # arcpy.AddMessage(f"SDDT version {v} for SSURGO version {dbVersion}")
        licenseLevel = arcpy.ProductInfo().upper()
        if licenseLevel == "BASIC":
            arcpy.AddError(
                "ArcGIS License level must be Standard or Advanced "
                "to run this tool"
            )
            return False
        
        # Create list of successful completions
        gdb_l = []
        fail_l = []
        # By State
        if option == 1:
            state_l = state_str.split(';')
            present_ssa = getSSAList(input_p)
            for state in state_l:
                data = sda_ssa_list(state)
                # Find data section (key='Table')
                if "Table" in data:
                    # Data as a list of lists. All values come back as string.
                    ssa_s = {ssa.lower() for ssa, in data["Table"]}
                    if (missing := ssa_s - present_ssa):
                        arcpy.AddWarning(
                            f"Incomplete set of SSURGO datasets for {state}."
                            f"\n{missing= }")
                        survey_l = list(ssa_s - missing)
                        return False
                    else:
                        survey_l = list(ssa_s)
                    arcpy.AddMessage(f'\nProcessing {len(survey_l)} surveys')
                    gdb_p = f"{output_p}/gSSURGO_{state}.gdb"
                    gdb_b = gSSURGO(
                        input_p, survey_l, gdb_p, aoi, state, light_b, module_p,
                        gssurgo_v, v
                    )
                    if gdb_b:
                        gdb_l.append(gdb_p)
                    else:
                        fail_l.append(gdb_p)
                else:
                    arcpy.AddWarning(
                        f'Unable to determine which surveys are within {state}.'
                        ' Geodatabase will not be created.'
                    )

        # By Soil Survey Layer
        elif option == 2:
            # Verify ssa layer has AREASYMBOL field
            ssa_des = arcpy.Describe(ssa_lyr)
            field_areasym = [
                fld_name for fld in ssa_des.fields 
                if (fld_name := fld.name).lower() == 'areasymbol'
            ]
            if not field_areasym:
                arcpy.AddError(
                    f"{ssa_des.name} does not have an areasymbol field"
                )
                return False
            elif len(field_areasym) > 1:
                arcpy.AddWarning(
                    "More than one possible areasymbol field found in "
                    f"{ssa_des.name}. Using {field_areasym[0]}")
            # Get list of requested soil survey areas
            with arcpy.da.SearchCursor(ssa_lyr, field_areasym[0]) as sCur:
                ssa_s = {ssa.lower() for ssa, in sCur}
            # Verify that all requested surveys are present
            present_ssa = getSSAList(input_p)
            if (missing := (ssa_s - present_ssa)):
                arcpy.AddWarning(
                    "Incomplete set of SSURGO datasets for those selected in "
                    f"{ssa_des.name}.\n{missing= }")
                survey_l = list(ssa_s - missing)
            else:
                survey_l = list(ssa_s)
            # Create requested gSSURGO database
            arcpy.AddMessage(f'\nProcessing {len(survey_l)} surveys')
            gdb_p = output_p
            gdb_b = gSSURGO(
                input_p, survey_l, gdb_p, aoi, '', light_b, module_p,
                gssurgo_v, v
            )
            if gdb_b:
                gdb_l.append(gdb_p)
            else:
                fail_l.append(gdb_p)

        # By Geography
        elif option == 3:
            present_ssa = getSSAList(input_p)
            ssa_lyr_d = arcpy.Describe(ssa_lyr)
            ssa_lyr_p = ssa_lyr_d.CatalogPath
            # arcpy.MakeFeatureLayer_management(ssa_lyr_p, ssa_lyr_s)
            # Find AREASYMBOL field
            field_areasym = [
                fld_name for fld in ssa_lyr_d.fields 
                if (fld_name := fld.name).lower() == 'areasymbol'
            ]
            if not field_areasym:
                arcpy.AddError(
                    f"The reference soil survey layer {ssa_lyr_d.name} "
                    "does not have an areasymbol field"
                )
                return False
            elif len(field_areasym) > 1:
                arcpy.AddWarning(
                    "More than one possible areasymbol field found in "
                    f"{ssa_lyr_d.name}. Using {field_areasym[0]}")

            # By each selected geographical unit
            geog_l = geog_str.split(';')
            for geog in geog_l:
                ssa_lyr_s = 'ssa_lyr_select'
                geog_lyr_s = f'geog_lyr_select_{geog}'
                q = f"{geog_fld} = '{geog}'"
                q = q.replace("''", "'")
                arcpy.AddMessage(q)
                arcpy.MakeFeatureLayer_management(geog_lyr, geog_lyr_s, q)
                arcpy.MakeFeatureLayer_management(ssa_lyr_p, ssa_lyr_s)
                arcpy.SelectLayerByLocation_management(
                    ssa_lyr_s, 'INTERSECT', geog_lyr_s  #
                )
                cnt = int(arcpy.GetCount_management(ssa_lyr_s).getOutput(0))

                if not cnt:
                    arcpy.AddError(
                        f"The {ssa_lyr.name} feature and {geog_lyr.name} do "
                        "not intersect"
                    )
                    return False

                # Create list SSURGO datasets
                with arcpy.da.SearchCursor(ssa_lyr_s, field_areasym[0]) \
                    as sCur:
                    ssa_s = {ssa.lower() for ssa, in sCur}
                if (missing := ssa_s - present_ssa):
                    arcpy.AddWarning(
                        f"Incomplete set of SSURGO datasets for those selected "
                        "in {geog}.\n{missing= }")
                    survey_l = list(ssa_s - missing)
                    return False
                else:
                    survey_l = list(ssa_s)
                arcpy.AddMessage(f'\nProcessing {len(survey_l)} surveys')
                gdb_p = f"{output_p}/gSSURGO_{label}_{geog}.gdb"
                gdb_b = gSSURGO(
                    input_p, survey_l, gdb_p, aoi, label, light_b, module_p,
                    gssurgo_v, v
                )
                if gdb_b:
                    gdb_l.append(gdb_p)
                else:
                    fail_l.append(gdb_p)

        # By selected surveys
        elif option == 0:
            gdb_p = output_p
            if len(survey_l := survey_str.split(';')):
                arcpy.AddMessage(f'\nProcessing {len(survey_l)} surveys')
                gdb_b = gSSURGO(
                    input_p, survey_l, gdb_p, aoi, '', light_b, module_p, 
                    gssurgo_v, v
                )
                if gdb_b:
                        gdb_l.append(gdb_p)
                else:
                    fail_l.append(gdb_p)
            else:
                arcpy.AddError("No surveys available to create specified gdb.")
                return False
        
        # By CONUS
        else:
            gdb_p = output_p
            survey_l = []
            # CONUS states
            state_l = [
                'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
                'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
                'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ',
                'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
                'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY'
            ]
            present_ssa = getSSAList(input_p)
            for state in state_l:
                data = sda_ssa_list(state)
                # Find data section (key='Table')
                if "Table" in data:
                    # Data as a list of lists. All values come back as string.
                    ssa_s = {ssa.lower() for ssa, in data["Table"]}
                    if (missing := ssa_s - present_ssa):
                        arcpy.AddWarning(
                            f"Incomplete set of SSURGO datasets for {state}."
                            f"\n{missing= }")
                        return False
                    else:
                        survey_l.extend(ssa_s)
                else:
                    arcpy.AddWarning(
                        f'Unable to determine which surveys are within {state}.'
                        ' Geodatabase will not be created.'
                    )
            
            arcpy.AddMessage(f'\nProcessing {len(survey_l)} surveys')
            gdb_b = gSSURGO(
                input_p, survey_l, gdb_p, aoi, state, light_b, module_p,
                gssurgo_v, v
            )
            if gdb_b:
                gdb_l.append(gdb_p)
            else:
                fail_l.append(gdb_p)
                

        if gdb_l:
            arcpy.AddMessage(
                "\nSuccessfully created the following gSSURGO geodatabases:"
            )
            for gdb_p in gdb_l:
                gdb_n = os.path.basename(gdb_p)
                arcpy.AddMessage(f"\t{gdb_n}")
        if fail_l:
            arcpy.AddWarning(
                "\nThese gSSURGO geodatabases were not successfully created:"
            )
            for gdb_p in fail_l:
                    gdb_n = os.path.basename(gdb_p)
                    arcpy.AddMessage(f"\t{gdb_n}")
        return gdb_l

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return []
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return []

if __name__ == '__main__':
    main(sys.argv[1:])
# %%
