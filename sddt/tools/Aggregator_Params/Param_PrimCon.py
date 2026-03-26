#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input paramter Param_PrimCon: Primary Soil Attribute
Parameter for Summarize Soil Information tool

Identify the primary attribute that will be summarized


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


class Param_PrimCon():
    def __init__(self):
        self.primcon = None
        self.error = None

        self.param = arcpy.Parameter(
            displayName="Primary Constraint",
            name="primcon",
            direction="Input",
            parameterType="Optional",
            datatype="GPString",
            enabled=False,
            multiValue=True
        )


    def update(self, unit_l):
        try:
            unit_l = list(unit_l)
            if len(unit_l) == 1:
                unit = unit_l[0]
            else:
                unit = None
            param_d = {11: [True, unit, '*', unit_l]}

            self.error = None
            return param_d
        except:
            self.error = pyErr('Param_PrimCon')
            return {}

