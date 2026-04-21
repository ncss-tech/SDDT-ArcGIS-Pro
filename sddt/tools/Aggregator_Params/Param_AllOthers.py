#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
All other arcpy Parameters for Summarize Soil Information tool
that don't require any other additional methods or characteristics


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 03/27/2026
    @by: Alexnder Stum
@version 1.1

# --- Update,
- import calendar and use calendar.month_name
# --- Update, v 1.1 3/27/2026
- added param13 for the months parameter

"""
import calendar
import arcpy


def param11():
    param = arcpy.Parameter(
        displayName="Secondary Constraint",
        name="secondary",
        direction="Input",
        parameterType="Optional",
        datatype="GPString",
        enabled=False,
        multiValue=True,
        category="Secondary"
    )
    return param


def param12():
    param = arcpy.Parameter(
        displayName="Depth Ranges (cm)",
        name="depths",
        direction="Input",
        parameterType="Optional",
        datatype="GPValueTable",
        # multiValue=True, # Make future version multivalue
        enabled=False
    )
    param.columns = [["GPLong", "Top"], ["GPLong", "Bottom"]]
    param.filters[0].type = "Range"
    param.filters[1].type = "Range"
    param.filters[0].list = [0, 499]
    param.filters[1].list = [1, 500]

    return param


def param13():
    months = list(calendar.month_name)[1:]

    param = arcpy.Parameter(
        displayName="Select Month(s): select all or none for Annual",
        name="month",
        direction="Input",
        parameterType="Optional",
        datatype="GPString",
        multiValue=True,
        enabled=False
    )
    param.filter.list = months
    # param.value = "Annual"

    return param
    

def param14():
    param = arcpy.Parameter(
        displayName="Tie Break Rule",
        name="tie",
        direction="Input",
        parameterType="Optional",
        datatype="GPString",
        enabled=False,
        category='Optional'
    )
    param.filter.list = ['Higher', 'Lower']
    param.value = 'Higher'

    return param


def param15():
    param = arcpy.Parameter(
        displayName="Component Percent Cutoff",
        name="comp_pct",
        direction="Input",
        parameterType="Required",
        datatype="GPLong",
        enabled=False,
        category='Optional'
    )
    param.value = 0

    return param


def param16():
    param = arcpy.Parameter(
        displayName="Property Range Value",
        name="range_value",
        direction="Input",
        parameterType="Optional",
        datatype="GPString",
        enabled=False,
        category='Optional'
    )
    param.filter.list = ['Representative', 'Low', 'High']
    param.value = 'Representative'

    return param


def param17():
    param = arcpy.Parameter(
        displayName="Map Interp Fuzzy Values",
        name="fuzzy",
        direction="Input",
        parameterType="Optional",
        datatype="GPBoolean",
        enabled=False,
        category='Optional'
    )
    param.value = False

    return param


def param18():
    param = arcpy.Parameter(
        displayName="Include Null Rating Values",
        name="rating_null",
        direction="Input",
        parameterType="Optional",
        datatype="GPBoolean",
        enabled=False
    )
    param.value = False

    return param


def param19():
    param = arcpy.Parameter(
        displayName="Treat Null entries as Zero",
        name="null",
        direction="Input",
        parameterType="Optional",
        datatype="GPBoolean",
        enabled=False
    )
    param.value = False

    return param


def param20():
    param = arcpy.Parameter(
        displayName="Invert Primary Constraint to NOT equal",
        name="primaryNOT",
        direction="Input",
        parameterType="Optional",
        datatype="GPBoolean",
        enabled=False,
        category='Optional'
    )
    param.value = False

    return param


def param21():
    param = arcpy.Parameter(
        displayName="Invert Secondary Constraint to NOT equal",
        name="secondaryNOT",
        direction="Input",
        parameterType="Optional",
        datatype="GPBoolean",
        enabled=False,
        category='Optional'
    )
    param.value = False

    return param


def param22():
    param = arcpy.Parameter(
        displayName="Find absoluste Horizon Min or Max",
        name="abs_min_max",
        direction="Input",
        parameterType="Optional",
        datatype="GPBoolean",
        enabled=False,
        category='Optional'
    )
    param.value = False

    return param