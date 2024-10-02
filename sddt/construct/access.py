# -*- coding: utf-8 -*-
"""
Created on Thu May  4 20:33:31 2023

@author: Alexander.Stum

Need to check sdv tables

"""

import sys
import os
import re
import traceback
import shutil
import pyodbc
import arcpy
import csv
from datetime import datetime
import multiprocessing as mp
import concurrent.futures as cf
import itertools as it


        
def pyErr(func):
    try:
        etype, exc, tb = sys.exc_info()
        
        tbinfo = traceback.format_tb(tb)[0]
        msgs = f"PYTHON ERRORS:\nTraceback info:\nIn function: {func}\n{tbinfo}\nError Info:\n{exc}"
        return msgs
    except:
        return "Error in pyErr method"
    
def concurrently(fn, max_concurrency, iterSets, constSets ):
    """
    Adapted from https://github.com/alexwlchan/concurrently/blob/main/concurrently.py
    

    Generates (input, output) tuples as the calls to ``fn`` complete.

    See https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/ for an explanation
    of how this function works.
    
    Parameters
    ----------
    fn : function
        The function that it to be run in parallel.
        
    max_concurrency : int
        Maximum number of processes, parameter for itertools islice function.
        
    iterSets : list
        List of dictionaries that will be iterated through. The keys of the dictionary
        must be the same for each dictionary and align with keywords of the function.
        
    constSets : dict
        Dictionary of parameters that constant for each iteration of the function
        ``fn``. Dictionary keys must align with function keywords.
        

    """
    try:
        # Make sure we get a consistent iterator throughout, rather than
        # getting the first element repeatedly.
        # count = len(iterSets)
        fn_inputs = iter(iterSets)
    
        with cf.ThreadPoolExecutor() as executor:
            # initialize first set of processes
            futures = {
                executor.submit(fn, **params, **constSets): params
                for params in it.islice(fn_inputs, max_concurrency)
            }
            # Wait for a future to complete, returns sets of complete and incomplete futures
            while futures:
                done, _ = cf.wait(
                    futures, return_when = cf.FIRST_COMPLETED
                )
    
                for fut in done:
                    # once process is done clear it out, yield results and params
                    original_input = futures.pop(fut)
                    yield original_input, fut.result()
                
                # Sends another set of processes equivalent in size to those just completed
                # to executor to keep it at max_concurrency in the pool at a time,
                # to keep memory consumption down.
                futures.update({executor.submit(fn, **params, **constSets): params
                for params in it.islice(fn_inputs, len(done))
                })
    except GeneratorExit:
        raise
        # arcpy.AddError("Need to do some clean up.")
        # yield 2, 'yup'
    except:
        func = sys._getframe(  ).f_code.co_name
        msgs = pyErr(func)
        yield [2, msgs]   
        

def dateFormat(date):
        wssDT = "%m/%d/%Y %H:%M:%S"
        mdbDT = "%Y-%m-%d %H:%M:%S"
        wsst = datetime.strptime(date, wssDT)
        return f'#{wsst.strftime(mdbDT)}#'
    
## ===============================================================================================================
def readAccess(ssaDir, importDB):

    try:
        msgs = ['']
        msgApp = msgs.append
        msgExt = msgs.extend
        base_f = os.path.basename(ssaDir)
        areaSym = base_f.removeprefix('soil_').upper()
        versionTxt = f"{ssaDir}/tabular/version.txt"
        scaTxt = f"{ssaDir}/tabular/sacatlog.txt"

        #---- Cursor
        cDriver = f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={importDB}'
        conn = pyodbc.connect(cDriver)
        cursor = conn.cursor()
        
        #---- Date
        # saverest = Survey Area Version Established
        tab1 = 'sacatalog'
        q1 = f"SELECT saverest FROM {tab1} WHERE areasymbol = '{areaSym}'"
        cursor.execute(q1)
        try:
            dateObj = next(cursor)[0]
            intDate = "%Y%m%d"                       # YYYYMMDD
            dbDate = int(dateObj.strftime(intDate))
            
            fh = open(scaTxt, "r")
            rec = fh.readline()
            fh.close()
            # Example date (which is index 3 in pipe-delimited file):  9/23/2014 6:49:27
            vals = rec.split("|")
            recDate = vals[3]
            wssDate = "%m/%d/%Y %H:%M:%S"  # string date format used for SAVEREST in text file
            intDate = "%Y%m%d"             # YYYYMMDD format for comparison
            dateObj = datetime.strptime(recDate, wssDate)
            txtDate = int(dateObj.strftime(intDate))
            
            if txtDate <= dbDate:
                # download_b = False
                msgApp((f"Local dataset for {areaSym} already exists "
                       "and is current"))
                return 2, msgs
            else:
                msgApp(f"Existing dataset out of date: {dbDate}; "
                       "WSS date: {txtDate};")
                # ---- Delete existing
                qd = f"DELETE CASCADE FROM legend WHERE areasymbol = '{areaSym}'"
                cursor.execute(qd)
                conn.commit()
                
                qdf = f"DELETE FROM featdesc WHERE areasymbol = '{areaSym}'"
                cursor.execute(qdf)
                conn.commit()
                
                qmd = f"SELECT distmdkey FROM distlegendmd WHERE areasymbol = '{areaSym}'"
                cursor.execute(qmd)
                for distKey, in cursor:
                    qmdd = f"DELETE CASCADE FROM distmd WHERE distmdkey = '{distKey}'"
                    cursor.execute(qmdd)
                conn.commit()
        except:
            # survey not yet imported
            return_ = 1
        

        #---- tables
        # Retrieve physical and alias names from MDSTATTABS table
        # mdstattabs table contains information for other SSURGO tables
        # physical names (key) and aliases (value) i.e. {chasshto:'Horizon AASHTO,chaashto'}
        tab2 = 'mdstattabs'
        cNames = 'iefilename, tabphyname, tablogname'
        q2 = f"SELECT {cNames} FROM {tab2}"
        cursor.execute(q2)
        tblInfo = {row[0]: row[1:] for row in cursor}

        #---- version
        # Ideally we want to compare with the value in version.txt with the version in
        # the "SYSTEM - Template Database Information" table. If they are not the same
        # the tabular import should be aborted. There are some more specifics about the
        # SSURGO version.txt valu in one of the Import macros of the Template database.
        # Need to follow up and research this more.
        # At this time we are only checking the first 'digit' of the string value.

        # Valid SSURGO version for data model. Ensures
        # compatibility between template database and SSURGO download.
        tab3 = '[SYSTEM - Template Database Information]'
        q3 = f"SELECT [Item Value] FROM {tab3} WHERE [Item Name] = 'SSURGO Version'"
        cursor.execute(q3)
        dbVersion = next(cursor)[0].split(".")[0]
        
        
        if os.path.isfile(versionTxt):
            # read just the first line of the version.txt file
            fh = open(versionTxt, "r")
            txtVersion = fh.readline().split(".")[0] # only need primary version
            fh.close()
        else:
            # Unable to compare vesions. Warn user but continue
            msgApp("\tUnable to find file: version.txt")
        
        if txtVersion != dbVersion:
            # SSURGO Versions do not match. Warn user but continue
            msgApp("\tDiscrepancy in SSURGO Version number for Template database"
                   " and SSURGO download")
        
        #---- call Import Tabular
        status, msg = ImportTabular(areaSym, ssaDir, cursor, tblInfo)
        msgExt(msg)
        if status:
            # ---- call SortMapunits
            # Need to change this if inserting into a central template to only run once
            status, msg = SortMapunits(cursor)
        else:
            return 0, msgs
        return 1, msgs

    except:
        func = sys._getframe(  ).f_code.co_name
        msgApp(pyErr(func))
        return 0, msgs
    finally:
        cursor.close()
        conn.close()
        del conn, cursor
    


## ===================================================================================
def SortMapunits(cursor):
    # Populate table 'SYSTEM - Mapunit Sort Specifications'. Required for Soil Data Viewer
    # Looks like an alpha sort on AREASYMBOL, then MUSYM will work to set
    # lseq and museq values within the "SYSTEM - Mapunit Sort Specifications" table
    #
    # Problem, this sort does not handle a mix of alpha and numeric musym values properly
    #
    # Populate table "SYSTEM - INTERP DEPTH SEQUENCE" from COINTERP using cointerpkey and seqnum
    #
    
    # Check if SYSTEM - Mapunit Sort Specifications is even populated
    # Read existing legend and mapunit tables
    # Read existing SYSTEM - Mapunit Sort Specifications
    # delete SYSTEM - Mapunit Sort Specifications
    # sort new legend mapunit
    # Write values back to SYSTEM - Mapunit Sort Specifications and insert new legend items
    msgs = ['']
    msgApp = msgs.append
    try:
        qd = "DELETE FROM [SYSTEM - Mapunit Sort Specifications]"
        cursor.execute(qd)
        cursor.commit()
    except:
        msgApp('Error deleting SYSTEM - Mapunit Sort Specifications')
        func = sys._getframe(  ).f_code.co_name
        msgApp(pyErr(func))
        return 0, msgs
    try:
        qf = ("SELECT legend.areaname, mapunit.musym, legend.lkey, mapunit.mukey FROM "
              "legend INNER JOIN mapunit ON legend.lkey = mapunit.lkey ORDER BY "
              "legend.areaname, mapunit.musym;")
        cursor.execute(qf)
    except:
        msgApp('Error running sort query')
        func = sys._getframe(  ).f_code.co_name
        msgApp(pyErr(func))
        return 0, msgs
    try:
        vals = [[row[2], row[3]] for row in cursor]
    
        qi = '''INSERT INTO [SYSTEM - Mapunit Sort Specifications](lkey, mukey) VALUES(?,?) '''
        cursor.executemany(qi, vals)
    
        li = 1
        for mi, (v1, v2) in enumerate(vals):
            mi += 1
            qi = ('''INSERT INTO [SYSTEM - Mapunit Sort Specifications](lseq, museq,lkey, mukey) '''
                  f'''VALUES({li}, {mi}, '{v1}','{v2}') ''')
            cursor.execute(qi)
    except:
        msgApp('Error inserting values into SYSTEM - Mapunit Sort Specifications')
        func = sys._getframe(  ).f_code.co_name
        msgApp(pyErr(func))
        return 0, msgs



## ===================================================================================
def ImportTabular(areaSym, ssaDir, cursor, tblInfo):
    """Thsi function imports text files in a new Access template database if the
    user provided a template.
    

    Parameters
    ----------
    areaSym : TYPE
        DESCRIPTION.
    newFolder : str
        The soil_areasym folder with the user specified output folder.
    importDB : str
        Access Tempalate database.
    newDB : str
        If a template Access database is provided, this is the new Access database
        to be put in the newFolder/tabular. It is named soil_d_areasym.mdb.
    bRemoveTXT : TYPE
        DESCRIPTION.

    Returns
    -------
    bool
        DESCRIPTION.

    """
    try:
        msgs = ['']
        msgApp = msgs.append

        datematch = r'\d{2}(/)\d{2}(/)\d{4}(/)? \d{2}(:\d{2}):\d{2}'

        # Create a list of textfiles to be imported. The import process MUST follow the
        # order in this list in order to maintain referential integrity. This list
        # will need to be updated if the SSURGO data model is changed in the future.
        #
        txtFiles = ["distmd","legend","distimd","distlmd","lareao","ltext","mapunit", \
        "comp","muaggatt","muareao","mucrpyd","mutext","chorizon","ccancov","ccrpyd", \
        "cdfeat","cecoclas","ceplants","cerosnac","cfprod","cgeomord","chydcrit", \
        "cinterp","cmonth", "cpmatgrp", "cpwndbrk","crstrcts","csfrags","ctxfmmin", \
        "ctxmoicl","ctext","ctreestm","ctxfmoth","chaashto","chconsis","chdsuffx", \
        "chfrags","chpores","chstrgrp","chtext","chtexgrp","chunifie","cfprodo",\
        "cpmat","csmoist", "cstemp","csmorgc","csmorhpp","csmormr","csmorss","chstr",\
        "chtextur", "chtexmod","sacatlog","sainterp","sdvalgorithm","sdvattribute",\
        "sdvfolder","sdvfolderattribute"]
            
        txtPaths = list(f"{ssaDir}/tabular/{txt}.txt" for txt in txtFiles)
        txtFiles.append('featdesc')
        txtPaths.append(f"{ssaDir}/spatial/soilsf_t_{areaSym}.txt")
        
        # Need to import text files in a specific order or the MS Access database will
        # return an error due to table relationships and key violations

        # Problem with length of some memo fields, need to allocate more memory
        #csv.field_size_limit(sys.maxsize)
        csv.field_size_limit(512000)
        
        # qi1 = "INSERT INTO distmd(distgendate, diststatus, interpmaxreasons, distmdkey) VALUES (#2022-09-12 13:00:37#, 'Successful', 5, '81014')"

        for txtFile, txtPath in zip(txtFiles, txtPaths):
            # Get table name and alias from dictionary
            if txtFile in tblInfo:
                tbl, aliasName = tblInfo[txtFile]
            else:
                msgApp(f"Textfile reference '{txtFile}' not found in 'mdstattabs table'")
                return False, msgs
            # column names
            cursor.execute(f"SELECT * FROM {tbl}")
            cols = [column[0] if column[0] != 'text' else '[text]' for column in cursor.description]
            columns = str(tuple(cols)).replace("'","")
            # columns = columns.replace("text", "[text]")
            
            if not os.path.isfile(txtPath):
                msgApp(f"Textfile {txtFile} not found in {areaSym} tabular folder")
                return False, msgs
            csvReader = csv.reader(open(txtPath, 'r'), delimiter='|', quotechar='"')
            # csvReader = csv.reader(open(txtPath, 'r'), delimiter='|', quotechar="'")
            iRows = 1
            for row in csvReader:
                try:
                    # Replace empty sets with NULL
                    vals1 = ('NULL' if (v == '') or (v == '""') else v for v in row)
                    # ---- call dateFormat
                    # Look for date formats and reformat for sql
                    vals2 = map(lambda s: s.replace("'", "''"), vals1)
                    vals3 = (f"""'{v.replace('"', '`')}'""" if not re.search(datematch, v) 
                             else dateFormat(v) for v in vals2)
                    
                    val_str = ", ".join(vals3)
                    # v2 = values.replace('"', '`')
                    # v3 = v2.replace("'", "''")
                    # v2 = values.replace("'", "''") # replace apostrophes with SQL frienly ''
                    # v3 = v2.replace('"', "'")
                    val_str = val_str.replace("'NULL'", "NULL")
                    qi1 = f"INSERT INTO {tbl}{columns} VALUES ({val_str})"
                    cursor.execute(qi1)
                    iRows += 1
                except:
                    msgApp(f"Error importing line {iRows} of {txtFile}")
                    func = sys._getframe(  ).f_code.co_name
                    pyErr(func)
                    return False, msgs
            arcpy.SetProgressorPosition()
        cursor.commit()
        del csvReader
        return True, msgs

    except:
        func = sys._getframe(  ).f_code.co_name
        msgApp(pyErr(func))
        return 0, msgs
    
def ProcessSurvey(ssaDir, baseFolder, tempOpt, template):
    """Manages the download process for each soil survey
    

    Parameters
    ----------
    outputFolder : str
        Folder location for the downloaded SSURGO datasets.

    areaSym : str
        The area symbol of current soil survey area being processed.
    surveyInfo : list
        The date the soil survey area was updated on WSS and survey name


    Returns
    -------
    str
        keywords: 'Successful', 'Skipped',or 'Failed'.

    """
    # Download and import the specified SSURGO dataset

    try:
        # get date string
        msgs = ['']
        msgApp = msgs.append
        
        msgApp(f"Importing {ssaDir}")
        # get survey name
        # set standard final path and name for template database
        curdir = f"{baseFolder}/{ssaDir}"
        
        if not os.path.isdir(curdir):
            msgApp(f"\tCan't find folder: {curdir}")
            return 0, msgs
        
        if tempOpt == 0:
            template = f"{curdir}/{template}"
            
        elif tempOpt == 2:
            template2 = f"{curdir}/{template}"
            shutil.copy2(template, template2)
            template = template2
            # copy file and set template     
            
        if not os.path.isfile(template):
            msgApp("\tCan't find template database")
            return 0, msgs
        # ---- Call readAccess
        readAccess(curdir, template)

        msgApp(f'Successfully imported {ssaDir}')
        return [1, msgs]

    
    except:
        func = sys._getframe(  ).f_code.co_name
        msgApp(pyErr(func))
        return [0, msgs]
    

def main(args):
    try:
        
        # ---- Parameters
        baseFolder = args[0]
        ssa_dirs = args[1]
        optionT = args[2]
        template = args[3]
        
        # ---- Setup
        msgs = ['']
        msgApp = msgs.append
        
        v = 0.1
        msgApp(f"version {v}")
        
        if optionT == 'Import into individual Default templates':
            option = 0
            template = 'soildb_US_2003.mdb'
        elif optionT == 'Import into specified central template':
            option == 1
            if not os.path.isfile(template):
                msgApp(f"Cant't find template file: {template}")
                return 0, msgs
        elif optionT == 'Copy and import specified template in each':
            option == 2
            if not os.path.isfile(template):
                msgApp(f"Cant't find template file: {template}")
                return 0, msgs
        else:
            msgApp(f"'{optionT}' is not a relevant Option parameter")
            return 0, msgs

        
        # %%% By Area Symbol
        paramSet = [{'ssaDir': ssa} for ssa in ssa_dirs]
        surveyCount = len(ssa_dirs)
        threadCount = min(mp.cpu_count(), surveyCount)
        msgApp(f"Running on {threadCount} threads.")
        successCount = 0
        failList = []
        # Run import process
        # ---- Call ProcessSurvey
        constSet = {'baseFolder': baseFolder, 
                    'tempOpt': option, 
                    'template': template}
        for paramBack, output in concurrently(ProcessSurvey,
                                             threadCount,
                                             paramSet,
                                             constSet):
            try:
                outcome, msgs = output
                if outcome == 0:
                    arcpy.AddMessage(f"{paramBack['areaSym']}:")
                    for msg in msgs:
                        arcpy.AddMessage(f"\t{msg}")
                    successCount += 1
                elif outcome == 1:
                    arcpy.AddWarning(f"{paramBack['areaSym']}:")
                    for msg in msgs:
                        arcpy.AddWarning(f"\t{msg}")
                    failList.append(paramBack['areaSym'])
                else:
                    arcpy.AddError(f"{paramBack['areaSym']}:")
                    for msg in msgs:
                        arcpy.AddError(f"\t{msg}")
                    failList.append(paramBack['areaSym'])
            except GeneratorExit:
                pass
    
    except:
        func = sys._getframe(  ).f_code.co_name
        msgApp(pyErr(func))
        return 0, msgs

# %% Main
if __name__ == '__main__':
    main(sys.argv[1:])