#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module reclasses a raster from a table using gdal and other. The arcpy
module and ESRI licensing is not needed to use this module. If it is called
from an ESRI toolbox script it does output messages using arcpy.

It was developed with SSURGO MURASTERs in mind where soil properties/interps 
aggregated by map unit could hardened instead of just joined to a RAT. 
This is particularly useful for use and visualization in QGIS.

This script is similar to ESRI Reclass by Table geoprocessing tool, but allows 
the Value field to be float or integer and the lookup value to be either 
string or numeric. When it is String, 
it conserves this nominal class in the outputs RAT. Also, in the case of using 
summary talbes from SDDT, you won't have to first make an integer field to hold 
the  mukey.
This script is similar to ESRI Lookup, but Lookup behaves very poorly with 
joined  fields, often failing to complete when the raster is modest in size. 
The work around here is to add fields to the raster, perform a join, 
calculate a field, remove the join, and then perform Lookup. 
This tool avoids all that. Also, the ESRI Lookup and Reclassify tools do not 
create pyramids.

This tool is equivalent in processing time to the ESRI tools but without the
hassles. 

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 02/27/2026
    @by: Alexnder Stum
@Version: 0.1

"""
v = 0.1

import csv
import os
import sys
import timeit

import numpy as np

from osgeo import gdal
from osgeo import ogr
from osgeo import gdalconst
from osgeo.gdal import Dataset, Band, GDT_Unknown
from osgeo.ogr import Layer

import xml.etree.ElementTree as ET
from typing import Any, Callable, Generic, TypeVar, Union, Optional

from .. import pyErr

Numeric = Union[int, float]
Shape = TypeVar("Shape", tuple, list)
DType = TypeVar("DType")

 
class Array(np.ndarray, Generic[Shape, DType]):
    """  
    Use this to type-annotate numpy arrays, e.g. 
        image: Array['H,W,3', np.uint8]
        xy_points: Array['N,2', float]
        nd_mask: Array['...', bool]
        (https://stackoverflow.com/questions/35673895/
        type-hinting-annotation-pep-484-for-numpy-ndarray)
    """
    pass


Nx = TypeVar("N", bound=Array[tuple[int], np.int64])
NxN_int = TypeVar("NxN_int", bound=Array[tuple[int, int], np.int32])
NxN_num = TypeVar("NxN_num", bound=Array[tuple[int, int], np.number])


def set_dtype(index_size: int) -> tuple[int, np.dtype, int]:
    """Establishes the appropriate data types for the output raster based
    off the maximum range of values.

    Parameters
    ----------
    index_size : int
        While an integer, it is a specific integer from gdal GDT variables
        which represent a specific data data type later used by the gdal 
        geotiff driver to create the output raster.

    Returns
    -------
    tuple[int, dtype, int]
        dt [int]: While an integer, it is a specific integer from gdal GDT 
            variables which represent a specific data data type later used by 
            the gdal geotiff driver to create the output raster.
        dtype [np.dtype]: The data type used to specify the numpy array `rcl_a`
            in the lookup functions
        max_id [int]: The maximum value that can be stored by the specified
            dataype
    """
    try:
        if index_size <= 255:
            dt = gdal.GDT_Byte
            dtype = np.uint8()
            max_id = 255
        elif index_size <= 65535:
            dt = gdal.GDT_UInt16
            dtype = np.int16()
            max_id = 65535
        else:
            dt = gdal.GDT_UInt32
            dtype = np.int32()
            max_id = 4294967295

        return dt, dtype, max_id
    except:
        func = sys._getframe().f_code.co_name
        return (pyErr(func))
    

def build_RAT(band: Band, sort_d: dict[int, Any], freq: Nx, col_n: str) -> str:
    """Build a Raster Attribute Table (RAT) within the .aux.xml file with
     a GDAL RasterAttributeTable object.

    Parameters
    ----------
    band : Band
        GDAL raster Band object of the output raster
    sort_d : dict[int, Any]
        This dictionary is a crosswalk between the raster value and the 
        nominal class.
        Key: integer representing raster value
        Value: Any string or numeric type representing a nominal class
    freq : Nx
        Numpy array holding the pixel count for each Key in the sort_d.
        The fist position [0] is usually just a place holder. It must be 
        an array with a size equal to length of sort_d + 1.
    col_n : str
        Name of the column to hold the nominal class

    Returns
    -------
    str
       An empty string if successful, otherwise an error message.
    """
    try:
        rat = gdal.RasterAttributeTable()
        # Add columns
        rat.CreateColumn("Value", gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
        rat.CreateColumn("Count", gdalconst.GFT_Real, gdalconst.GFU_PixelCount)
        rat.CreateColumn(col_n, gdalconst.GFT_String, gdalconst.GFU_Name)
    
        # Populate rows
        sort_d = dict(sorted(sort_d.items()))
        for i, (val, prop) in enumerate(sort_d.items()):
            if val is not None:
                rat.SetValueAsInt(i, 0, val)
                rat.SetValueAsInt(i, 1, int(freq[val]))
                rat.SetValueAsString(i, 2, prop)
        band.SetDefaultRAT(rat)
        band.FlushCache()
        return ''
    except:
        func = sys._getframe().f_code.co_name
        return (pyErr(func))


def calculateStatistics(
        rast_p: str, src_ds: Dataset, out_band: Band, n_cls: int = 256
    ) -> str:
    """Calculate raster statistics to populate the .aux.xml file

    Parameters
    ----------
    rast_p : str
        Full path of the raster for which statistis will be calculated
    src_ds : Dataset
        GDAL dataset object of the raster for which statistics 
        will be calculated
    out_band : Band
        GDAL Band of the src_ds for which statistics will be calculated. 
        Assumed to be only one band.
    n_cls : int, optional
        Number of histogram bins, by default 256

    Returns
    -------
    str
        An empty string if successful, otherwise an error message.
    """
    try:
        #is approximate calculation okay (BOOL: default=False), 
        #force recalculation if stats already exist (BOOL: default=False)
        #Rtns list: Min, Max, Mean, StdDev
        stats = out_band.GetStatistics(False,True)

        #GetStatistics wasn't writing the .aux.xml stats file immediately. 
        # so I added this.
        #Not too far down, there is code to modify the stats  
        src_ds.FlushCache()

        #gdal.org/doxygen/classGDALRasterBand.html#aa21dcb3609bff012e8f217ebb7c81953
        # buckets is number of bins, default is 256. For nominal want 
        # number of unique values
        if stats[0] != stats[1]:
            histogram = out_band.GetHistogram(
                min=stats[0], max=stats[1], approx_ok=False, buckets=n_cls
            )
        else:
            histogram = out_band.GetHistogram(
                min=stats[0], max=stats[0] + .5, approx_ok=False, buckets=n_cls
            )

        #open the statistics file that GetStatistics creates automatically 
        xmlpath = rast_p + '.aux.xml'
        meta_tree = ET.parse(xmlpath)
        xml_root = meta_tree.getroot()

        # Create the <Histograms> block
        histograms = ET.Element("Histograms")
        hist_item = ET.SubElement(histograms, "HistItem")
        ET.SubElement(hist_item, 'HistMin').text = str(int(stats[0]))
        ET.SubElement(hist_item, 'HistMax').text = str(int(stats[1]))
        ET.SubElement(hist_item, 'BucketCount').text = '256'
        ET.SubElement(hist_item, 'IncludeOutOfRange').text = '1'
        ET.SubElement(hist_item, 'Approximate').text = '0'
        ET.SubElement(
            hist_item, 'HistCounts').text = ' | '.join(map(str, histogram)
        )

        pam_band = xml_root.find("PAMRasterBand")
        pam_band.insert(0, histograms)

        meta_tree.write(xmlpath)
        del xml_root
        del meta_tree

        return ''
    except:
        func = sys._getframe().f_code.co_name
        return pyErr(func)
    

def buildPyramids(src_ds: Dataset, method: str) -> str:
    """Build pyramids for the raster dataset. Calculates for all zoom levels
     with >= 512 rows or columns plus two more levels. Pyramid file is
     written out as an .ovr file.

    Parameters
    ----------
    src_ds : Dataset
        GDAL dataset object of the raster for which pyramids 
        will be built
    method : str
        Resampling method to be used, typical methods are NEAREST or BILINEAR,
        NEAREST is default
        https://gdal.org/en/stable/programs/gdal_raster_overview_add.html

    Returns
    -------
    str
        An empty string if successful, otherwise an error message.
    """
    try:
        # DEFLATE ~ LZ77
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
        max_dim = max([src_ds.RasterXSize, src_ds.RasterYSize])
        if max_dim >= 1024:
            n2 = np.array([2])**np.arange(1, 30)
            r2 = np.array([max_dim]) // n2
            pyr_idx = np.where(r2 <= 512)[0][0]
            levels = n2[:pyr_idx + 2]
            src_ds.BuildOverviews(method, levels.tolist())
        return ''

    except:
        func = sys._getframe().f_code.co_name
        return pyErr(func)

    
def add_pyramid_meta(rast_o: str, pyr_method: str) -> str:
    """Adds pyramid resampling method to the .aux.xml metadata file.
     Should be run after raster statistics have been calcualted and
     pyramids have been written or this element gets wiped.

    Parameters
    ----------
    rast_o : str
        File path of the raster dataset.
    pyr_method : str
        Pyramid resampling method.

    Returns
    -------
    str
        An empty string if successful, otherwise an error message.
    """
    try:
        # Add the metadata domain="Esri"
        # This hast to be run after the pyramids have been produced 
        # or it gets wiped
        xmlpath = rast_o + '.aux.xml'
        meta_tree = ET.parse(xmlpath)
        xml_root = meta_tree.getroot()
        
        ESRI_metadata = ET.Element("Metadata", domain="Esri")
        ET.SubElement(
            ESRI_metadata, "MDI", key="PyramidResamplingType"
        ).text = pyr_method
        xml_root.insert(0, ESRI_metadata)

        meta_tree.write(xmlpath)
        del meta_tree

        return ''

    except:
        func = sys._getframe().f_code.co_name
        return pyErr(func)

    
def lookup_nom(
        src_a: NxN_int, rcl_d: dict[int, int], freq_a: Nx, 
        dtype: np.dtype, prnt_e: Callable, nodata: int
    ) -> NxN_num:
    """This is the crux of the Lookup module. This function is intended for 
     cases where the input raster is integer and the output is a nominal class.
     The function looks up pixel values from the `src_a` and retrieve 
     their new value from the `rcl_d` thereby returing a 
     reclassified array `rcl_a`

    Parameters
    ----------
    src_a : NxN_int
        A Numpy array with the values of chunk of the source raster dataset 
        with values to be reclassified.
        Recommend using 256 x 256 chunks. 
    rcl_d : dict
        This dictionary is a crosswalk between the input raster value and the 
        new raster value.
        Key: integer representing input raster value
        Value: integer representing a nominal class value or an index to a
        nominal class
    freq_a : Nx
        Numpy array holding the pixel count for each Key in the sort_d.
        The fist position [0] is usually just a place holder.
    dtype : np.dtype
        Data type of the returned array `rcl_a`. Generally should be np.int32
    prnt_e : Callable
        The print function to be called in the event an exception is raised
    nodata : int
        Nodata value used when value in input raster not in lookup table, 
        fill in with nodata

    Returns
    -------
    NxN_num
        The reclassified array
    """
    try:
        rcl_get = rcl_d.get
        mukeys, counts = np.unique(src_a, return_counts=True)
        rcl_a = np.empty(src_a.shape, dtype=dtype)
        for i, mk in enumerate(mukeys):
            try:
                val = rcl_get(mk)
                # Twice as fast as uisng where
                rcl_a[src_a == mk] = val
                freq_a[val] += counts[i]
            except TypeError:
                # missing from lookup -> nodata
                rcl_a[src_a == mk] = nodata
                # not be included in RAT as nodata
                continue
            except:
                func = sys._getframe().f_code.co_name
                prnt_e(pyErr(func + ': in_loop'))
                exit()

        return rcl_a
    except:
        # prnt_e(f"{i}: {mk}: {val=}")
        func = sys._getframe().f_code.co_name
        prnt_e(pyErr(func))
        exit()


def lookup_cont(
        src_a: NxN_int, rcl_d: dict, dtype: np.dtype, 
        prnt_e: Callable, nodata: int
    ) -> NxN_num:
    """This is the crux of the Lookup module. This function is intended for 
     cases where the input raster is integer and the output is a continuous.
     The function looks up pixel values from the `src_a` and retrieves 
     the new value from the `rcl_d` thereby returing a 
     reclassified array `rcl_a`

    Parameters
    ----------
    src_a : NxN_int
        A Numpy array with the values of chunk of the source raster dataset 
        with values to be reclassified.
        Recommend using 256 x 256 chunks. 
    rcl_d : dict
        This dictionary is a crosswalk between the input raster value and the 
        new raster value.
        Key: integer representing input raster value
        Value: integer representing a nominal class value or an index to a
        nominal class
    dtype : np.dtype
        Data type of the returned array `rcl_a`. Generally should be np.int32
    prnt_e : Callable
        The print function to be called in the event an exception is raised
    nodata : int
        Nodata value used when value in input raster not in lookup table, 
        fill in with nodata

    Returns
    -------
    NxN_num
        The reclassified array
    """
    try:
        rcl_get = rcl_d.get
        mukeys = np.unique(src_a)
        rcl_a = np.empty(src_a.shape, dtype=dtype)
        for mk in mukeys:
            try:
                val = rcl_get(mk)
                # Twice as fast as uisng where
                rcl_a[src_a == mk] = val
            except TypeError:
                rcl_a[src_a == mk] = nodata
            except:
                func = sys._getframe().f_code.co_name
                prnt_e(pyErr(func + ': in_loop'))
                exit()
        return rcl_a 
    
    except:
        func = sys._getframe().f_code.co_name
        prnt_e(pyErr(func))
        exit()


def read_csv(
        tbl_p: str, mk_f: str, prop_f: str, prnt_e: Callable, sort_f: str = ''
    ) -> tuple[dict[int, Numeric], dict[int, Any], bool]:
    """Reads the lookup table, where the table is a csv with column headers. 
     The `mk_f` and `prop_f` fields are required. The `sort_f` can be 
     passed as an empty string. 

    Parameters
    ----------
    tbl_p : str
        Path for the input table
    mk_f : str
        The key field or the field that corresponds to pixel values within
        the source raster that is to be reclassified. This field should be
        integer or be able to be type cast to integer.
    prop_f : str
        The field with the new raster values or nominal values that will 
        saved in an anciallary column of the RAT. This field can be either
        numeric or string
    prnt_e : Callable
        The print function to be called in the event an exception is raised
    sort_f : str
        If the nominal classes are ordinal or have a specific key value by
        which they should be represented in the output raster. This field should be
        integer or be able to be type cast to integer.
        This parameter is optional.
    
    Returns
    -------
    tuple[dict[int, Numeric], dict[int, Any], bool]
        reclass_d:
        This dictionary is a crosswalk between the current raster value and the 
        new rater value.
            Key: integer representing the current raster value
            Value: Any number representing the new raster value
        sort_d: This dictionary is a crosswalk between the new raster value 
        and the nominal class.
            Key: integer representing the new raster value
            Value: Any string or numeric type representing a nominal class
        nominal_b:
        A boolean indicating whether the output is nominal
    """
    try:
        csv_f = open(tbl_p, 'r', encoding='utf-8-sig')
        csv_r = csv.reader(csv_f)
        hdr = next(csv_r)
        prop_fi = hdr.index(prop_f)
        mk_fi = hdr.index(mk_f)
        sort_d = {}

        if sort_f:
            nominal_b = True
            sort_fi = hdr.index(sort_f)
            reclass_d = {}
            for row in csv_r:
                sort_id = row[sort_fi]
                prop_c = row[prop_fi]
                if prop_c and sort_id:
                    mk_i = int(row[mk_fi])
                    reclass_d[mk_i] = int(sort_id)
                    sort_d[sort_id] = prop_c
        else:
            reclass_d = {}
            prop_fi = hdr.index(prop_f)
            mk_fi = hdr.index(mk_f)
            try:
                reclass_d = {
                    int(row[mk_fi]): float(row[prop_fi]) 
                    for row in csv_r if row[prop_fi]
                }
            except:
                reclass_d = {
                    int(row[mk_fi]): row[prop_fi] 
                    for row in csv_r if row[prop_fi]
                }
                nominal_b = True
        del csv_r
        del csv_f

        return reclass_d, sort_d, nominal_b
    
    except:
        func = sys._getframe().f_code.co_name
        prnt_e(pyErr(func))
        raise


def read_tab(
        lyr: Layer, mk_f: str, prop_f: str, prnt_e: Callable, sort_f: str = ''
    ) -> tuple[dict, dict, bool]:
    """Reads the lookup table, where the table is a .dbf or table within
    a File Geodatabase. 
     The `mk_f` and `prop_f` fields are required. The `sort_f` can be 
     passed as an empty string. 

    Parameters
    ----------
    tbl_p : Layer
        OGR Layer object
    mk_f : str
        The key field or the field that corresponds to pixel values within
        the source raster that is to be reclassified. This field should be
        integer or be able to be type cast to integer.
    prop_f : str
        The field with the new raster values or nominal values that will 
        saved in an anciallary column of the RAT. This field can be either
        numeric or string
    prnt_e : Callable
        The print function to be called in the event an exception is raised
    sort_f : str
        If the nominal classes are ordinal or have a specific key value by
        which they should be represented in the output raster. This field should be
        integer or be able to be type cast to integer. 
        This parameter is optional.

    Returns
    -------
    tuple[dict[int, Numeric], dict[int, Any], bool]
        reclass_d:
        This dictionary is a crosswalk between the current raster value and the 
        new rater value.
            Key: integer representing the current raster value
            Value: Any number representing the new raster value
        sort_d: This dictionary is a crosswalk between the new raster value 
        and the nominal class.
            Key: integer representing the new raster value
            Value: Any string or numeric type representing a nominal class
        nominal_b:
        A boolean indicating whether the output is nominal
    """
    try:
        sort_d = {}
        if sort_f:
            reclass_d = {}
            for ft in lyr:
                reclass_d[int(ft.GetField(mk_f))] = ft.GetField(sort_f)
                sort_d[ft.GetField(sort_f)] = ft.GetField(prop_f)
        else:
            reclass_d = {int(ft.GetField(mk_f)): ft.GetField(prop_f) for ft in lyr}
        # Get property data type
        lyr_def = lyr.GetLayerDefn()
        fld_def = lyr_def.GetFieldDefn(prop_f)
        fld_typ = fld_def.GetType()
        if fld_def.GetFieldTypeName(fld_typ) == 'String' or sort_f:
            nominal_b = True
        else:
            nominal_b = False
        return reclass_d, sort_d, nominal_b
    except:
        func = sys._getframe().f_code.co_name
        prnt_e(pyErr(func))
        raise


def read_gpkg(db_p: str, prop_f: str, prnt_e: Callable, sort_f: str = ''):
    """Reads the lookup table, where the table is within
    an SQLite geopackage. 
     The `mk_f` and `prop_f` fields are required. The `sort_f` can be 
     passed as an empty string. 

    Parameters
    ----------
    tbl_p : Layer
        OGR Layer object
    mk_f : str
        The key field or the field that corresponds to pixel values within
        the source raster that is to be reclassified. This field should be
        integer or be able to be type cast to integer.
    prop_f : str
        The field with the new raster values or nominal values that will 
        saved in an anciallary column of the RAT. This field can be either
        numeric or string
    prnt_e : Callable
        The print function to be called in the event an exception is raised
    sort_f : str
        If the nominal classes are ordinal or have a specific key value by
        which they should be represented in the output raster. This field should be
        integer or be able to be type cast to integer. 
        This parameter is optional.

    Returns
    -------
    tuple[dict[int, Numeric], dict[int, Any], bool]
        reclass_d:
        This dictionary is a crosswalk between the current raster value and the 
        new rater value.
            Key: integer representing the current raster value
            Value: Any number representing the new raster value
        sort_d: This dictionary is a crosswalk between the new raster value 
        and the nominal class.
            Key: integer representing the new raster value
            Value: Any string or numeric type representing a nominal class
        nominal_b:
        A boolean indicating whether the output is nominal
    """
    try:
        db = ogr.Open(db_p)
        tab_n = tab_n.lstrip('.main')
        lyr = db.GetLayerByName(tab_n)
        sort_d = {}
        if sort_f:
            nominal_b = True
            reclass_d = {}
            for ft in lyr:
                reclass_d[int(ft.GetFID())] = ft.GetField(sort_f)
                sort_d[ft.GetField(sort_f)] = ft.GetField(prop_f)
        else:
            reclass_d = {int(ft.GetFID()): ft.GetField(prop_f) for ft in lyr}
        # Get property data type
        lyr_def = lyr.GetLayerDefn()
        fld_def = lyr_def.GetFieldDefn(prop_f)
        fld_typ = fld_def.GetType()

        if fld_def.GetFieldTypeName(fld_typ) == 'String' or sort_f:
            nominal_b = True
        else:
            nominal_b = False
        
        return reclass_d, sort_d, nominal_b
    except:
        func = sys._getframe().f_code.co_name
        prnt_e(pyErr(func))
        raise


def main(
        rast_p: str, rast_o: str, tbl_p: str, mk_f: str, prop_f: str, 
        sort_f: Optional[str] = '', null_id_opt: Optional[str] = "NoData", 
        null_id: Optional[int] = None, nd_user: Optional[Numeric] = None,
        esri: Optional[int] = 0
    ) -> str:
    """This tool was designed with gSSURGO MURASTER in mind,
        where soil properties/interps aggregated by map unit could 
        hardened instead of just joined to a RAT. 
        This is particularly useful for use and visualization in QGIS 
        or is optimal for certain ESRI Geoprocessing tools that don't 
        work with joined fields in rasters or perform suboptimally.
        This script is similar to ESRI Reclass by Table geoprocessing 
        tool, but allows the Value field to be float or integer and the 
        lookup value to be either string or numeric. When it is String, 
        it conserves this nominal class in the output's RAT. 
        Also, in the case of using 
        summary talbes from SDDT, you won't have to first make an integer 
        field to hold the  mukey.
        This script is aslo similar to ESRI Lookup, but Lookup behaves 
        very poorly with joined  fields, often failing to complete when 
        the raster is modest in size.

    Parameters
    ----------
    rast_p : str
        Full path of the raster to be reclassified.
    rast_o : str
        Full path of the reclassified raster as a geotiff.
    tbl_p : str
        Full path of the lookup table.
    mk_f : str
        Column name from the lookup table with the current input raster values.
    prop_f : str
        Column name from the lookup table with the new value or class field
        of the output raster.
    sort_f : Optional[str], optional
        Column name from the lookup table that holds key or sequence index. 
        This is useful of the nominal class has a specific order they should
        be written in the RAT which will enhance symbolization. If not 
        specified and input is determined to be nominal (integer or string)
        and index value will be autogenerated, by default ''
    null_id_opt : Optional[str], optional
        What to do if an input value in the lookup table has a null property
        value. 
        If "NoData" specified (default) then these pixels will be reclassified
        with the output nodata value. If "None" is specified these pixels will
        receive the `null_id` if specified otherwise an arbitrary index value, 
        each will have the with the class text of 'None'. 
        Note: That any input raster values not found in the lookup table will always
        be reclassified with the output nodata value. by default "NoData"
        Note: This option is only applied if the `prop_f` column is integer 
        or string
    null_id : Optional[int], optional
        The key or sequence value applied to values with a null class value.
        Note: this option is only applicable if `null_id_otp` is "None" and 
        the `prop_f` column is integer or string, by default None
    nd_user : Optional[Numeric], optional
        The user can specify the output no data value. Otherwise the default 
        value is 0 if output is integer or 3.40282306e+38 if float, 
        by default None
    esri : Optional[int], optional
        Expects 0 (non-ESRI) or 1 (ESRI).
        This flags whether the module is being called from ArcGIS Pro toolbox, 
        in which case the module imports the arcpy module for message outputs,
        otherwise (default) messages are output using the `print` funtion, 
        by default 0

    Returns
    -------
    str
       Path of the reclassified raster, empty string if unsuccessful. 

    Raises
    ------
    RuntimeError
        _description_
    RuntimeError
        _description_
    RuntimeError
        _description_
    """
    try:
        if esri:
            import arcpy
            prnt = arcpy.AddMessage
            prnt_w = arcpy.AddWarning
            prnt_e = arcpy.AddError
        else:
            prnt = print
            prnt_w = print
            prnt_e = print
        prnt(f"Raster Lookup by Table {v=}\n")

        if null_id_opt == 'NoData':
            null_id = None

        gdal.UseExceptions()
        # MB of GDAL internal cache
        gdal.SetConfigOption("GDAL_CACHEMAX", "2048") 
        # used in compression/overviews
        gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS") 

        ti = timeit.time.time()
        # Read Table
        tab_n = os.path.basename(tbl_p)
        tab_ext = os.path.splitext(tab_n)[1]
        db_p = os.path.dirname(tbl_p)
        db_ext = os.path.splitext(db_p)[1]

        nominal_b = False
        
        # Read lookup table
        if tab_ext == '.csv':
            prnt('reading CSV table')
            reclass_d, sort_d, nominal_b = read_csv(
                tbl_p, mk_f, prop_f, prnt_e, sort_f
            )
        elif tab_ext == '.dbf':
            prnt('Reading dbf table')
            driver = ogr.GetDriverByName("ESRI Shapefile")
            dataSource = driver.Open(tbl_p, 0)
            lyr = dataSource.GetLayer()
            reclass_d, sort_d, nominal_b = read_tab(
                lyr, mk_f, prop_f, prnt_e, sort_f
            )
        elif db_ext == '.gdb':
            prnt('Reading FGDB table')
            db = ogr.Open(db_p)
            lyr = db.GetLayerByName(tab_n)
            reclass_d, sort_d, nominal_b = read_tab(
                lyr, mk_f, prop_f, prnt_e, sort_f
            )
        elif db_ext in ['.gpkg', '.sqlite']:
            prnt('Reading SQLite table')
            reclass_d, sort_d, nominal_b = read_gpkg(
                lyr, mk_f, prop_f, prnt_e, sort_f
            )
        else:
            # prnt(f"{db_ext=}, {tab_ext=}, {tbl_p=}, {rast_o}")
            prnt('Compatible lookup table not provided')
            raise

        if nd_user is not None:
            out_nodata = int(nd_user)
        else:
            out_nodata = 0

        # If nominal class
        # Build sort_d for RAT index
        # None and NoData values need appropriate index value
        # Determine data type to accomodate integer range
        if nominal_b:
            pyr_method = "NEAREST"
            pred_v = 2

            # Populate sort_d dictionary
            if not sort_f:
                # {Property: id}
                prop_d = {}
                i = 1
                # set the values for reclass_d
                for mki, prop_i in reclass_d.items():
                    if mki is None and prop_i is None:
                        reclass_d[mki] = out_nodata
                        continue

                    # is property null?
                    if prop_i is None:
                        if null_id_opt == 'NoData':
                            reclass_d[mki] = out_nodata
                            continue
                        elif null_id is not None:
                            if null_id not in sort_d:
                                sort_d[null_id] = None
                            reclass_d[mki] = null_id
                            continue
                        # else keep going to get next available integer id
                        # or get id already established for None
                            
                    if prop_i not in prop_d:
                        sort_d[i] = prop_i
                        prop_d[prop_i] = i
                        reclass_d[mki] = i
                        i += 1
                        # as index is arbitrary, skip values
                        # already established for null_id and nd_user
                        if i == null_id or i == out_nodata:
                            i += 1
                            if i == null_id or i == out_nodata:
                                i += 1
                    else:
                        reclass_d[mki] = prop_d[prop_i]
                index_max = max(out_nodata, i)
                dt, dtype, max_id = set_dtype(index_max)
            else:
                if null_id is not None and null_id in sort_d:
                    prnt_w(
                        f"You specified {null_id} as the "
                        "Sequence or Key Index For Null Nominal Class,\n"
                        f"Classes with this id will be classified as 'None'"
                    )
                    sort_d[null_id] = 'None'
                if nd_user is not None  and nd_user in sort_d:
                    prnt_w(
                        f"You specified {nd_user} as the NoData Value\n"
                        f"Classes with this id will be classified as NoData"
                    )
                    # Remove from RAT
                    sort_d.pop(nd_user)

                # Determine max_index and data type
                keys = list(sort_d.keys())
                keys.remove(None)
                if null_id_opt == 'None':
                    index_max = max([*keys, out_nodata]) + 1
                else: 
                    index_max = max([*keys, out_nodata])
                dt, dtype, max_id = set_dtype(index_max)

                # need to specify another nodata value if default no data value
                # 0 is used as in index
                if nd_user is None and 0 in sort_d:
                    # Need to type cast up to accomodate nodata value
                    if max_id in sort_d:
                        dt, dtype, max_id = set_dtype(max_id + 1)
                    out_nodata = max_id
                    if null_id is None:
                        null_id = max_id - 1
                elif null_id is None:
                    null_id = max_id

                # Null assumes outnodata value and not included in RAT
                if None in reclass_d and reclass_d[None] is None:
                    reclass_d[None] = out_nodata
    
                if null_id_opt == 'NoData':
                    if None in sort_d:
                        sort_d.pop(None)
                    reclass_d = {k: (v if v is not None else out_nodata) 
                        for k, v in reclass_d.items()}
                elif None in sort_d:
                    sort_d.pop(None)
                    sort_d[null_id] = 'None'
                    if null_id is not None:
                        reclass_d = {k: (v if v is not None else null_id) 
                            for k, v in reclass_d.items()}
                    else:
                        reclass_d = {k: (v if v is not None else max_id) 
                            for k, v in reclass_d.items()}

            # Frequency array for RAT Count
            freq = np.zeros(
                [max([*sort_d.keys(), out_nodata]) + 1], dtype=np.int64
            )
        # Otherwise, simply continuous reclass
        else:
            dt = gdal.GDT_Float32
            if nd_user is not None:
                out_nodata = nd_user
            else:
                out_nodata = 3.40282306e+38
            pyr_method = "AVERAGE"
            dtype = np.float32()
            pred_v = 3

        # Source Raster file
        rast_n = os.path.basename(rast_p)
        rast_dir = os.path.dirname(rast_p)
        dir_ext = os.path.splitext(rast_dir)[1]
        if dir_ext == '.gdb':
            prnt('reading raster from FGDB')
            rast_in = f'OpenFileGDB:"{rast_dir}":{rast_n}'
        else:
            rast_in = rast_p        

        # -- Read in source raster
        src_ds = gdal.Open(rast_in, gdal.GA_ReadOnly)
        if src_ds is None:
            raise RuntimeError(f"Could not open source raster: {rast_p}")

        src_band = src_ds.GetRasterBand(1)
        x_size = src_ds.RasterXSize
        y_size = src_ds.RasterYSize
        src_gt  = src_ds.GetGeoTransform()
        src_prj = src_ds.GetProjection()
        src_nodata = src_band.GetNoDataValue()

        prnt(f"\nSource raster size: {x_size} x {y_size}")
        prnt(f"Output NoData value: {out_nodata}")

        # add nodata to reclass_d if not present
        if src_nodata not in reclass_d:
            reclass_d[src_nodata] = out_nodata
            # exclude from RAT
            if out_nodata in sort_d:
                sort_d.pop(out_nodata)
        # This takes care of situations where source raster value is not 
        # found in the lookup table
        if None not in reclass_d:
            reclass_d[None] = out_nodata
        if 0 not in reclass_d:
            reclass_d[0] = out_nodata

        # -- Create Output Raster
        driver = gdal.GetDriverByName("GTiff")
        out_options = [
            "COMPRESS=LZW",
            "BIGTIFF=YES",
            "TILED=YES",
            "BLOCKXSIZE=256",
            "BLOCKYSIZE=256",
            f"PREDICTOR={pred_v}" 
        ]

        out_ds = driver.Create(
            rast_o, x_size, y_size, 1, dt, options=out_options
        )
        if out_ds is None:
            raise RuntimeError(f"Could not create output raster: {rast_o}")

        out_ds.SetGeoTransform(src_gt)
        out_ds.SetProjection(src_prj)
        out_band = out_ds.GetRasterBand(1)
        out_band.SetNoDataValue(out_nodata)

        # -- Chunked Processing
        # tile_w = x_size // (x_size // 24000 + 1) + 1
        # if tile_w < x_size:
        #     tile_h = 1
        # else:
        #     tile_h = 24000 // x_size or 1
        # Chunks are much faster thain lines when using 
        # unique do to + spatial autocorrelation
        tile_w = 256
        tile_h = 256

        rows_done = 0

        for j, yoff in enumerate(range(0, y_size, tile_h)):
            yblock = min(tile_h, y_size - yoff)
            for xoff in range(0, x_size, tile_w):
                xblock = min(tile_w, x_size - xoff)

                # Read window from source
                src_chunk = src_band.ReadAsArray(xoff, yoff, xblock, yblock)
                
                if src_chunk is None:
                    raise RuntimeError(
                        "ReadAsArray returned None at window "
                        f"{xoff}:{xoff+xblock}, {yoff}:{yoff+yblock}"
                    )

                if nominal_b:
                    # accumulate frequency for RAT
                    recl_chunk = lookup_nom(
                        src_chunk, reclass_d, freq, dtype, prnt_e, out_nodata
                    )
                else:
                    recl_chunk = lookup_cont(
                        src_chunk, reclass_d, dtype, prnt_e, out_nodata
                    )

                del src_chunk
                
                # Write reclassed chunk to output
                out_band.WriteArray(recl_chunk, xoff, yoff)

            rows_done += yblock
            try:
                if not j % (y_size // (tile_h * 10)):
                    prnt(
                        f"Progress: {rows_done}/{y_size} "
                        f"rows ({rows_done / y_size:.1%})"
                    )
            except:
                pass
  
        prnt(f"Reclassified raster in {timeit.time.time() - ti:.1} seconds")

        # Flush and close datasets
        out_band.FlushCache()
        out_ds = None
        src_ds = None

        out_ds = gdal.Open(rast_o, gdal.GA_ReadOnly)
        out_band = out_ds.GetRasterBand(1)

        # Post processing, the following is not critical but useful.
        # Particularly the RAT. Failures may suggest an issue upstream
        prnt("\nCalculating Statistics..")
        msg = calculateStatistics(rast_o, out_ds, out_band)
        if msg:
            msg = "Unable to Calculate Statistics: \n" + msg
            prnt_w(msg)
        prnt("Building Pyramids...")
        msg = buildPyramids(out_ds, pyr_method)
        if msg:
            msg = "Failed to Build Pyramids: \n" + msg
            prnt_w(msg)
        if nominal_b:
            prnt("Building RAT...")
            msg = build_RAT(out_band, sort_d, freq, prop_f)
            if msg:
                msg = "Failed to create RAT:\n" + msg
                prnt_w(msg)

        # Flush and close datasets
        out_band.FlushCache()
        out_band.FlushCache()
        out_ds = None
        src_ds = None

        msg = add_pyramid_meta(rast_o, pyr_method)
        if msg:
            msg = ("Failed to include pyramid resampling method "
                   "in .aux.xml file: \n" + msg)
            prnt_w(msg)

        prnt(f"\n Successfully created reclassified raster: {rast_o}")
        return rast_o
    except:
        func = sys._getframe().f_code.co_name
        prnt_e(pyErr(func))
        return ''


if __name__ == '__main__':
    main(*sys.argv[1:])



