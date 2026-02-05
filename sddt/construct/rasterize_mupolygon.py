#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rasterize_mupolygon
A tool for the Soil Data Development Toolbox for ArcGISPro arctoolbox
Rasterize SSURGO soil polgon feature to a gSSURGO raster dataset.

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 02/05/2026
    @by: Alexnder Stum
@version: 0.5

# --- Updated 11/19/2025, v 0.5
- Removed arcpyErr and pyErr functions, calling from sddt
# --- Updated 11/19/2025, v 0.4
- Allow user to specify cell assignment logic: "CELL_CENTER", "MAXIMUM_AREA", 
"MAXIMUM_COMBINED_AREA". Default is now CELL_CENTER to align with gdal used
by SSURGO Portal.
- Set arcpy.SetLogHistory to True to record geoprocessing step
- Adds entries to Version table
# --- Updated 10/16/2024
- Converted workspace variable from geoprocessing object to string path
# --- Updated 10/02/2024
- Changed cell assignment paramter in Polygon to Raster function from 
    Cell Center to Maximum combined area to better generalize patterned
    areas
# --- Updated 10/08/2024
- Updated Metadata elements
"""
v = 0.5

import sys
import platform
import os
import time
import datetime
import xml.etree.cElementTree as ET
import arcpy
from arcpy import env

from .. import pyErr
from .. import arcpyErr
    

def versionTab(gdb_p: str, v: str, raster_n: str, cell_assig: str):
    """This function populates the version table within the FGDB.

    Parameters
    ----------
    gdb_p : str
        The path of the newly created SSURGO file geodatabase.
    raster_n : str
        Name of the newly created raster
    cell_assig : str
        The method to determine how the cell will be assigned a value 
        when more than one feature falls within a cell. Used by the arcpy
        PolygonToRaster function.
    v : str
        Version of the rasterize_mupolygon.py script.

    Returns
    -------

    """
    try:
        cell_assig = cell_assig.replace('_', ' ')
        version_p = f"{gdb_p}/version"
        iCur = arcpy.da.InsertCursor(version_p, ['type', 'name', 'version'])
        iCur.insertRow(['Raster cell assignment', raster_n, cell_assig])
        iCur.insertRow(['Script', f'SDDT: Create gSSURGO Raster', v])
        del iCur
        arcpy.Delete_management("memory")

        return
    
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddWarning(arcpyErr(func))
        return
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddWarning(pyErr(func))
        return


def extCoord(coord: float, cell_r: float, offset=0) -> float:
    """Calculates coordinate component to snap extent
Number of cells from snap point to corner coordinate times
resolution equals new extent coordinate component.

    Parameters
    ----------
    coord : float
        Either the X or Y coordinate 
    cell_r : float
        Raster cell size
    offset : float
        Offset factor
    Returns
    -------
    float
        Coordinate componet for new raster extent. 
        Returns the string 'Error' if an exception is raised.
    """

    try:
        coord = coord + offset
        coord_n = (
            coord // cell_r + round((coord % cell_r) / cell_r)
        ) * cell_r
        return coord_n - offset

    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return 'Error'


def updateMetadata(
        wksp: str, target: str, survey_i: str, resolution: str, 
        script_p: str
        ) ->str:
    """Creates metadata for the newly created gSSURGO raster from 
    FGDC_CSDGM xml template

    Replaces xx<keywords>xx in template
    xxSTATExx : State or states from legend overlap table (laoverlap)
    xxSURVEYSxx : String listing all the soil survey areas
    xxTODAYxx : Todays date
    xxFYxx : mmyyyy format, to signify vintage of SSURGO data
    xxENVxx : Windows, ArcGIS Pro, and Python version information
    xxNAMExx : Name of the gSSURGO raster dataset
    xxDBxx : Database the SSURGO data was sourced from
    xxVERxx : Version of that database

    Parameters
    ----------
    wksp : str
        Source path of the SSURGO database.
    target : str
        Name of the created gSSURGO raster.
    survey_i : str
        List of the soil survey areas.
    resolution : str
        sSSURGO cell size formatted with units.
    script_p : str
        Path to the SDDT/construct submodule where xml metadata 
        template is found.

    Returns
    -------
    bool
        Returns empty string if successful, otherwise returns a message.
    """

    try:
        arcpy.SetProgressor("default", "Updating raster metadata")
        gdb_n = os.path.basename(wksp)
        # Define input and output XML files
        # the metadata xml that will provide the updated info
        meta_import = env.scratchFolder + f"/xxImport_{gdb_n}.xml"
        # original template metadata in script directory
        meta_export = f"{script_p}/gSSURGO_MapunitRaster.xml"
        # Cleanup output XML files from previous runs
        if os.path.isfile(meta_import):
            os.remove(meta_import)

        # Get replacement value for the search words
        # State overlaps
        states = {
            'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas',
            'AS': 'American Samoa', 'AZ': 'Arizona', 'CA': 'California',
            'CO': 'Colorado', 'CT': 'Connecticut', 
            'DC': 'District of Columbia', 'DE': 'Delaware', 'FL': 'Florida',
            'FM': 'Federated States of Micronesia', 'GA': 'Georgia',
            'GU': 'Guam', 'HI': 'Hawaii', 'IA': 'Iowa', 'ID': 'Idaho',
            'IL': 'Illinois', 'IN': 'Indiana', 'KS': 'Kansas',
            'KY': 'Kentucky', 'LA': 'Louisiana', 'MA': 'Massachusetts',
            'MD': 'Maryland', 'ME': 'Maine', 
            'MH': 'Republic of the Marshall Islands', 'MI': 'Michigan',
            'MN': 'Minnesota', 'MO': 'Missouri',
            'MP': 'Commonwealth of the Northern Mariana Islands',
            'MS': 'Mississippi', 'MT': 'Montana', 'NC': 'North Carolina',
            'ND': 'North Dakota', 'NE': 'Nebraska', 'NH': 'New Hampshire',
            'NJ': 'New Jersey', 'NM': 'New Mexico', 'NV': 'Nevada',
            'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania', 'PR': 'Puerto Rico',
            'PW': 'Republic of Palau', 'RI': 'Rhode Island',
            'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
            'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia', 
            'VI': 'U.S. Virgin Islands', 'VT': 'Vermont',
            'WA': 'Washington', 'WI': 'Wisconsin', 'WV': 'West Virginia',
            'WY': 'Wyoming'
            }
        q = "areatypename = 'State or Territory'"
        sCur = arcpy.da.SearchCursor(f"{wksp}/laoverlap", 'areasymbol', q)
        state_overlaps = {states[st] for st, in sCur}
        if len(state_overlaps) == 1:
            state = list(state_overlaps)[0]
        else:
            state = ''
            state_overlaps = list(state_overlaps)
            state_overlaps.sort()
        state_overlaps = ', '.join(state_overlaps)

        # System Environment
        esri_i = arcpy.GetInstallInfo()
        sys_env = (
            f"Microsoft {platform.system()} {platform.release()} "
            f"Version {platform.version()}; "
            f"ESRI {esri_i['ProductName']} {esri_i['Version']}; "
            f"Python {platform.python_version()}"
            )

        # Database
        wksp_d = arcpy.Describe(wksp)
        wksp_ext = wksp_d.extension
        if wksp_ext == 'gdb':
            db = 'ESRI File Geodatabase'
            tool = 'Soil Data Development Toolbox in ArcGIS Pro'
            ver = str(int(wksp_d.release.split(',')[0]) + 7)
        elif wksp_ext == 'gpkg':
            db = 'Geopackage database'
            tool = 'SSURGO Portal'
            ver = wksp_d.release.split(',')[0]
        elif wksp_ext == 'sqlite':
            db = 'SpatiaLite database'
            tool = 'SSURGO Portal'
            ver = wksp_d.release.split(',')[0]
        else:
            db = 'database'
            tool = ''
            ver = ''

        # Set date based upon today's date
        d = datetime.date.today()
        today = str(d.isoformat().replace("-",""))
        # As of July 2020, switch gSSURGO version format to YYYYMM
        fy = d.strftime('%Y%m')

        # Process gSSURGO_MapunitRaster.xml from script directory
        tree = ET.parse(meta_export)
        root = tree.getroot()

        # new citeInfo has title.text, edition.text, serinfo/issue.text
        citeinfo = root.findall('idinfo/citation/citeinfo/')
        if citeinfo is not None:
            # Process citation elements
            # title, edition, issue
            for child in citeinfo:
                if child.tag == "title":
                    newTitle = f"Map Unit Raster {resolution} {state}"
                    child.text = newTitle
                elif child.tag == "edition":
                    if child.text == 'xxFYxx':
                        child.text = fy
                elif child.tag == "serinfo":
                    for subchild in child.iter('issue'):
                        if subchild.text == "xxFYxx":
                            subchild.text = fy

        # Update place keywords
        place = root.find('idinfo/keywords/place')
        if place is not None:
            for child in place.iter('placekey'):
                if child.text == "xxSTATExx":
                    child.text = state_overlaps
                elif child.text == "xxSURVEYSxx":
                    child.text = survey_i

        # Update credits
        idinfo = root.find('idinfo')
        if idinfo is not None:
            for child in idinfo.iter('datacred'):
                text = child.text
                if text.find("xxSTATExx") >= 0:
                    text = text.replace("xxSTATExx", state_overlaps)
                if text.find("xxFYxx") >= 0:
                    text = text.replace("xxFYxx", fy)
                if text.find("xxTODAYxx") >= 0:
                    text = text.replace("xxTODAYxx", today)
                child.text = text

        purpose = root.find('idinfo/descript/purpose')
        if purpose is not None:
            text = purpose.text
            if text.find("xxFYxx") >= 0:
                purpose.text = text.replace("xxFYxx", fy)

        # Update process steps
        procstep = root.findall('dataqual/lineage/procstep')
        if procstep:
            for child in procstep:
                for subchild in child.iter('procdesc'):
                    text = subchild.text
                    if text.find('xxTODAYxx') >= 0:
                        text = text.replace("xxTODAYxx", d.strftime('%Y-%m-%d'))
                    if text.find("xxSTATExx") >= 0:
                        text = text.replace("xxSTATExx", state_overlaps)
                    if text.find("xxFYxx") >= 0:
                        text = text.replace("xxFYxx", fy)
                    if text.find("xxRESxx") >= 0:
                        text = text.replace('xxRESxx', resolution)
                    if text.find("xxDBxx") >= 0:
                        text = text.replace('xxDBxx', db)
                    if text.find("xxTOOLxx") >= 0:
                        text = text.replace('xxTOOLxx', tool)
                    subchild.text = text

        # Update VAT name
        enttypl = root.find('eainfo/detailed/enttyp/enttypl')
        if enttypl is not None:
            text = enttypl.text
            if text.find("xxNAMExx") >= 0:
                enttypl.text = text.replace(
                    "xxNAMExx", os.path.basename(target))

        # Update OS, ESRI, Python system information
        native = root.find('idinfo/native')
        if native is not None:
            text = native.text
            if text == "xxENVxx":
                native.text = sys_env
        envirDesc = root.find('dataIdInfo/envirDesc')
        if envirDesc is not None:
            text = envirDesc.text
            if text == "xxENVxx":
                envirDesc.text = sys_env

        # update raster resoluton
        stepDesc = root.find('dqInfo/dataLineage/prcStep/stepDesc')
        if stepDesc is not None:
            text = stepDesc.text
            if text.find('xxRESxx') >= 0:
                text = text.replace('xxRESxx', resolution)
            if text.find("xxDBxx") >= 0:
                text = text.replace('xxDBxx', db)
            if text.find("xxTOOLxx") >= 0:
                text = text.replace('xxTOOLxx', tool)
            stepDesc.text = text

        # Update database information
        formname = root.find('distinfo/stdorder/digform/digtinfo/formname')
        if formname is not None:
            if formname.text == "xxDBxx":
                formname.text = db
        formvern = root.find('distinfo/stdorder/digform/digtinfo/formvern')
        if formvern is not None:
            if formvern.text == "xxVERxx":
                formvern.text = ver

        formatName = root.find('distInfo/distributor/distorFormat/formatName')
        if formatName is not None:
            if formatName.text == "xxDBxx":
                formatName.text = db
        formatVer = root.find('distInfo/distributor/distorFormat/formatVer')
        if formatVer is not None:
            if formatVer.text == "xxVERxx":
                formatVer.text = ver

        #  create new xml file which will be imported, 
        # thereby updating the table's metadata
        tree.write(
            meta_import, 
            encoding="utf-8", 
            xml_declaration=None, 
            default_namespace=None, 
            method="xml")

        # Save changes
        meta_src = arcpy.metadata.Metadata(target)
        meta_src.importMetadata(meta_import, "FGDC_CSDGM")
        meta_src.deleteContent('GPHISTORY')
        meta_src.save()

        # delete the temporary xml metadata file
        if os.path.isfile(meta_import):
            os.remove(meta_import)
        return ''

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        return arcpy.AddError(arcpyErr(func))
    except:
        func = sys._getframe().f_code.co_name
        return arcpy.AddError(pyErr(func))


def rasterize(
        wksp: str, mu_n: str, resolution: int, external: bool, cell_assig: str,
        script_p: str, v: str
        ) -> bool:
    """Primary function that creates the new gSSURGO raster

    Parameters
    ----------
    wksp : str
        Source path of the SSURGO database.
    mu_n : str
        Name of the soil polygon layer, i.e. MUPOLYGON.
    resolution : int
        Output cell resolution in meters, if output coordinate system 
        is not projected, then it will be converted to an approximate 
        arcsecond units.
    external : bool
        For file geodatabases, the gSSRUGO raster can be saved as a 
        tif outside of the geodatabase.
    cell_assig : str
        The method to determine how the cell will be assigned a value 
        when more than one feature falls within a cell. Used by the arcpy
        PolygonToRaster function.
        CELL_CENTER—The polygon that overlaps the center of the cell yields 
          the attribute to assign to the cell.
        MAXIMUM_AREA—The single feature with the largest area within 
          the cell yields the attribute to assign to the cell.
        MAXIMUM_COMBINED_AREA—If there is more than one feature in a cell with 
          the same value, the areas of these features will be combined. 
          The combined feature with the largest area within the cell 
          will determine the value to assign to the cell.
    script_p : str
        Path of the construct submodules where .xml templates are saved.
    v : str
        Version of the rasterize_mupolygon.py script.

    Returns
    -------
    bool
        If gSSURGO was successfully created and metadata imported, 
        returns True, otherwise False.
    """

    try:
        # Set geoprocessing environment
        env.overwriteOutput = True
        arcpy.env.compression = "LZ77"
        env.tileSize = "128 128"
        # env.rasterStatistics = "NONE"
        env.pyramid = "PYRAMIDS 0"
        wksp_d = arcpy.Describe(wksp)
        poly_w = arcpy.da.Walk(
                    wksp, datatype='FeatureClass', type='Polygon')
        mu_p = [f"{dirpath}/{filename}"
                    for dirpath, dirnames, filenames in poly_w
                    for filename in filenames if filename == mu_n
                ][0]
        arcpy.AddMessage(f"\nDatabase: {wksp}")

        # Check input layer's coordinate system linear units are meters
        mu_d = arcpy.Describe(mu_p)
        sr = mu_d.spatialReference
        if sr.type.upper() == "PROJECTED":
            unit = sr.linearUnitName.upper()
            if unit == "METER":
                cell_str = f"{resolution} {unit.capitalize()}"
                rast_suffix = f"{resolution}m"
                cell_r = resolution
                if resolution == 5:
                    off_f = 0
                elif resolution == 10:
                    off_f = 5
                else:
                    off_f = 15
            else:
                arcpy.AddError(
                    "\nSoil polygon feature spatial reference linear unit is "
                    f"not in meters: \n{mu_p}")
                return False
        else:
            # if decimal degrees use a consistent resolution worldwide
            # Mirroring 3dep
            off_f = 0
            dd1_3 = 0.00009259259269220167
            if resolution == 5:
                cell_r = dd1_3 / 2
                cell_str = "1/6 Arc Second"
                rast_suffix = "1_6as"
            elif resolution == 10:
                cell_r = dd1_3
                cell_str = "1/3 Arc Second"
                rast_suffix = "1_3as"
            elif resolution == 30:
                cell_r = dd1_3 * 3
                cell_str = "1 Arc Second"
                rast_suffix = "1as"
            else:
                cell_r = dd1_3 * 9
                cell_str = "3 Arc Second"
                rast_suffix = "3as"
        # Set environment to coordinate system of input polygon feature
        env.outputCoordinateSystem = sr
        # Get extent of input polygon feature
        mu_ext = mu_d.extent
        mu_lr = mu_ext.lowerRight
        mu_ul = mu_ext.upperLeft
        # Calculate new extent that will snap to NLCD for 30m & 90m
        # (factor of 30 offset by 15) 
        # or Prime Meridian and Equator
        rast_lrx = extCoord(mu_lr.X, cell_r, off_f)
        rast_lry = extCoord(mu_lr.Y, cell_r, off_f)
        rast_ulx = extCoord(mu_ul.X, cell_r, off_f)
        rast_uly = extCoord(mu_ul.Y, cell_r, off_f)
        rast_ext = arcpy.Extent(rast_ulx, rast_lry, rast_lrx, rast_uly)
        # Set environment to new extent.
        env.extent = rast_ext
        arcpy.AddMessage(
            f"\tRaster will be projected in {sr.name},\n"
            f"\t{cell_str} cell size\n"
            "\tExtent:\n\t\t"
            f"Xmin: {rast_ulx}, Ymin: {rast_lry},\n\t\t"
            f"Xmax: {rast_lrx}, Ymax: {rast_uly}")

        # Path for new raster
        # Create a tif file in same directory as wksp
        rast_n = "MURASTER_" + rast_suffix
        arcpy.AddMessage(
            f"\tConverting featureclass {mu_n} to raster {rast_n}"
        )
        if wksp_d.extension != 'gdb' or external:
            rast_p = f"{wksp_d.path}/{rast_n}.tif"
        else:
            rast_p = f"{wksp}/{rast_n}"

        if arcpy.Exists(rast_p):
            arcpy.management.Delete(rast_p)
            if arcpy.Exists(rast_p):
                arcpy.AddError(f"{rast_p} already exists and won't delete.")

        ti = time.time()
        # The Lookup table contains both MUKEY and its integer counterpart 
        # (CELLVALUE).
        # Using the joined lookup table creates a raster with 
        # CellValues that are the same as MUKEY (but integer).
        arcpy.SetProgressorLabel("Creating Lookup table...")
        arcpy.management.Delete("memory")
        lu = "memory/Lookup"
        if arcpy.Exists(lu):
            arcpy.management.Delete(lu)
        arcpy.management.CreateTable("memory", "Lookup")
        arcpy.management.AddField(lu, "CELLVALUE", "LONG")
        arcpy.management.AddField(lu, "MUKEY", "TEXT", "#", "#", "30")

        # Create a list of map unit keys present in the 
        # MUPOLYGON featureclass
        arcpy.SetProgressorLabel("Populating Lookup table...")
        mu_lyr = "SoilPolygons"
        with arcpy.da.SearchCursor(mu_p, ["MUKEY"]) as sCur:
            # Create set MUKEY values in the MUPOLYGON featureclass
            mukey_s = {int(key) for key, in sCur}
        # mukey_s.sort()
        if not mukey_s:
            arcpy.AddError("Failed to get MUKEY values from " + mu_p)
            return False
        # Load MUKEY values into Lookup table
        with arcpy.da.InsertCursor(lu, ("CELLVALUE", "MUKEY") ) as iCur:
            for mukey in mukey_s:
                iCur.insertRow([mukey, str(mukey)])
        # Add MUKEY attribute index to Lookup table
        mu_lyr = "poly_tmp"
        arcpy.management.MakeFeatureLayer(mu_p, mu_lyr)
        # get MUKEY field data type
        mu_type = [f.type for f in mu_d.fields if f.name == 'MUKEY'][0]
        if mu_type == 'String':
            arcpy.management.AddJoin(mu_lyr, "MUKEY", lu, "MUKEY", "KEEP_ALL")
        else:
            arcpy.management.AddJoin(
                mu_lyr, "MUKEY", lu, "CELLVALUE", "KEEP_ALL"
            )
        arcpy.SetProgressor("default", "Running PolygonToRaster conversion...")
        arcpy.conversion.PolygonToRaster(
            mu_lyr, "Lookup.CELLVALUE", rast_p,
            cell_assig, "#", cell_r
        ) #"MAXIMUM_COMBINED_AREA"
        
        arcpy.management.Delete(mu_lyr)
        arcpy.management.Delete("memory")
        arcpy.AddMessage("\tRaster completed")

        # Add MUKEY field to raster
        arcpy.management.AddField(rast_p, "MUKEY", "TEXT", "#", "#", "30")
        with arcpy.da.UpdateCursor(rast_p, ["VALUE", "MUKEY"]) as uCur:
            for rec in uCur:
                rec[1] = rec[0]
                uCur.updateRow(rec)
        
        # Update version table
        try:
            versionTab(wksp, v, rast_n, cell_assig)
        except:
            arcpy.AddWarning("Failed to update version table")

        # Build pyramids and statistics
        if arcpy.Exists(rast_p):
            arcpy.SetProgressor(
                "default", "Calculating raster statistics..."
            )
            with arcpy.EnvManager(parallelProcessingFactor="1"):
                arcpy.management.CalculateStatistics(
                    rast_p, 1, 1, "", "OVERWRITE")

            arcpy.SetProgressor("default", "Building pyramids...")
            env.pyramid = "PYRAMIDS -1 NEAREST"
            arcpy.management.BuildPyramids(
                rast_p, "-1", "NONE", "NEAREST", "DEFAULT", "",
                "SKIP_EXISTING"
            )

            # Add attribute index (MUKEY) for raster
            arcpy.management.AddIndex(rast_p, ["mukey"], "Indx_RasterMukey")

        else:
            arcpy.AddError(f"Creation of {rast_p} Failed")
            return False

        # Compare list of original mukeys with the list of raster mukeys
        # Discrepancies are usually thin polygons along survey boundaries,
        # added to facilitate a line-join.
        arcpy.SetProgressor("default", "Looking for missing map units...")
        rast_cnt = int(
            arcpy.management.GetRasterProperties(
                rast_p, "UNIQUEVALUECOUNT"
                ).getOutput(0)
                )
        mu_cnt = len(mukey_s)
        if rast_cnt != mu_cnt:
            # Create list of raster mukeys...
            with arcpy.da.SearchCursor(rast_p, ("MUKEY",)) as sCur:
                rmukey_s = {int(mukey) for mukey, in sCur}
            mukey_diff = set(mukey_s) - rmukey_s
            if mukey_diff:
                #mu_q1 = "', '".join(mukey_diff)
                mu_q = f"MUKEY IN ('{tuple(map(str, mukey_diff))}')"
                arcpy.AddWarning(
                    "Discrepancy in mapunit count for new raster.\n"
                    "The following MUKEY values were present in the "
                    "original MUPOLYGON featureclass, but not in the "
                    f"raster:\n{mu_q}\n"
                    "Such discrepancies are a natural consequence of "
                    "generalization that occurs in vector to raster conversion"
                    " due to polygons too small to be preserved.")

        if wksp_d.extension == "gdb" or not external:
            # Update metadata file for the geodatabase
            # Query the output SACATALOG table to get list of surveys
            arcpy.SetProgressorLabel("Updating metadata...")
            sacat_p = f"{wksp}/sacatalog"
            with arcpy.da.SearchCursor(
                sacat_p, ("AREASYMBOL") #, "SAVEREST")
                ) as sCur:
                # f"{rec[0]} ({str(rec[1]).split()[0]})"
                exp_l = [rec[0] for rec in sCur]
            survey_str = ", ".join(sorted(exp_l))
            meta_msg = updateMetadata(
                wksp, rast_p, survey_str, cell_str, script_p)
            arcpy.SetProgressorLabel("Compacting database...")
            arcpy.management.Compact(wksp)

        t_delta = time.time() - ti
        if t_delta > 3600:
            arcpy.AddMessage(
                f"\n\tProcessing time: {t_delta// 3600:.0f} hours "
                f"{t_delta % 3600 // 60:.0f} minutes "
                f"{t_delta % 60:.1f} seconds")
        elif t_delta > 60:
            arcpy.AddMessage(
                f"\n\tProcessing time: {t_delta % 3600 // 60:.0f} minutes "
                f"{t_delta % 60:.1f} seconds")
        else:
            arcpy.AddMessage(
                f"\n\tProcessing time: {t_delta % 60:.1f} seconds")
        if meta_msg:
            arcpy.AddError(f'Failed to update metadata: \n{meta_msg}')
            return False
        return True

    except MemoryError:
        arcpy.AddError("Not enough memory to process.")
        return False
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def main(
        wksp_l: list[str], 
        mu_n: str, 
        resolution: int, 
        external: bool, 
        cell_assig: str,
        script_p: str
        ) ->bool:
    """Generate gSSURGO raster
    This is the main funciton that calls the ``rasterize`` for 
    every database.

    Parameters
    ----------
    wskp_l : list[str]
        List of SSURGO databases for which a gSSURGO layer will be 
        created from the specified soil polygon layer.
    mu_n : str
        Name of the soil polygon layer, i.e. MUPOLYGON
    resolution : int
        Output cell resolution in meters, if output coordinate system 
        is not projected, then it will be converted to an approximate 
        arcsecond units.
    external : bool
        For file geodatabases, the gSSRUGO raster can be saved as a 
        tif outside of the geodatabase
    cell_assig : str
        The method to determine how the cell will be assigned a value 
        when more than one feature falls within a cell. 
    script_p : str
        Path of the construct submodules where .xml templates are saved.

    Returns
    -------
    bool
        True if sucessful, otherwise False.
    """

    try:
        arcpy.AddMessage(f"Create SSURGO raster, {v = !s}")
        env.overwriteOutput= True
        arcpy.SetLogHistory(True)

        cell_ops = ["CELL_CENTER", "MAXIMUM_AREA", "MAXIMUM_COMBINED_AREA"]
        if cell_assig not in cell_ops:
            cell_assig = "CELL_CENTER"
        arcpy.AddMessage(f"Cell assigment method: {cell_assig}")

        bad_apples = []
        for wksp_p in wksp_l:
            wksp_p = f"{wksp_p}"
            conversion_b = rasterize(
                wksp_p, mu_n, resolution, external, cell_assig, script_p, v
            )
            if not conversion_b:
                bad_apples.append(wksp_p)
                arcpy.AddWarning(
                    f"\tIt does not appear that the {mu_n} in {wksp_p} was "
                    "converted to gSSURGO successfully."
                )
        if bad_apples:
            try:
                apples = '\n\t'.join(bad_apples)
            except:
                apples = '\n\t'.join([str(a.value) for a in bad_apples])
            arcpy.AddError(
                "The following datasets didn't successfully process: "
                f"{apples} \nSee specific error messages above"
            )
        arcpy.SetLogHistory(False)

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return


if __name__ == '__main__':
    main(*sys.argv[1:])