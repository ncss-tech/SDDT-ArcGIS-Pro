# -*- coding: utf-8 -*-
"""
Created on Thu May 30 13:21:07 2019

Automated polygon generalization tool, smooth, generalize and remove 
acute angles


@author: Alexander.Stum


@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov

@modified 03/26/2024
    @by: Alexnder Stum
@version: 2.4
# ---
Updated 03/26/2024 - Alexander Stum
- Added main function which leverages concurrent futures to perform 
parallel processing which gives more control and enables more explicit
error messaging
- adapted SmoothGen to be called by main
- Added arcpyErr and pyErr functions for more streamlined error handling

USDA is an equal opportunity provider, employer and lender.
"""

import arcpy
import sys
import os
import numpy as np
import concurrent.futures as cf
import itertools as it
import traceback
import multiprocessing as mp

from arcpy import Geometry as geometry
      

def arcpyErr(func):
    try:
        etype, exc, tb = sys.exc_info()
        line = tb.tb_lineno
        msgs = (
            f"ArcPy ERRORS:\nIn function: {func} on line: "
            f"{line}\n{arcpy.GetMessages(2)}\n"
        )
        return msgs
    except:
        return "Error in arcpyErr method"


def pyErr(func):
    try:
        etype, exc, tb = sys.exc_info()
      
        tbinfo = traceback.format_tb(tb)[0]
        msgs = (
            "PYTHON ERRORS:\nTraceback info:\nIn function: "
            f"{func}\n{tbinfo}\nError Info:\n{exc}"
        )
        return msgs
    except:
        return "Error in pyErr method"


def main(fn, max_concurrency, iterSets, constSets ):
    """
    Adapted from 
    https://github.com/alexwlchan/concurrently/blob/main/concurrently.py
    

    Generates (input, output) tuples as the calls to ``fn`` complete.

    See https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/ 
    for an explanation of how this function works.
    
    Parameters
    ----------
    fn : function
        The function that it to be run in parallel.
        
    max_concurrency : int
        Maximum number of processes, parameter for itertools islice 
        function.
        
    iterSets : iterable
        An iterable object, specifically an arcpy Search Cursor object.
        
    constSets : dict
        Dictionary of parameters that constant for each iteration of 
        the function ``fn``. Dictionary keys must align with 
        function keywords.

    Yields
    ------
    A tuple with the original input parameters and the results 
    from the called
    function ``fn``.
    """
    try:
        # Make sure we get a consistent iterator throughout, rather than
        # getting the first element repeatedly.
        # count = len(iterSets)
        mp.set_executable(
            os.path.dirname(sys.executable) 
            + '/Python/envs/arcgispro-py3/pythonw.exe'
        )
        fn_inputs = iter(iterSets)
    
        with cf.ProcessPoolExecutor(max_concurrency) as executor:
            # initialize first set of processes
            futures = {
                executor.submit(fn, **{'cur_set': params}, **constSets):
                    params[1]
                    for params in it.islice(fn_inputs, max_concurrency)
            }
            # Wait for a future to complete, returns sets of complete 
            # and incomplete futures
            while futures:
                done, _ = cf.wait(
                    futures, return_when = cf.FIRST_COMPLETED
                )
    
                for fut in done:
                    # once process is done clear it out, 
                    # yield results and params
                    original_input = futures.pop(fut)
                    output = fut.result()
                    del fut
                    yield original_input, output
                
                # Sends another set of processes equivalent in size to 
                # those just completed to executor to keep it at 
                # max_concurrency in the pool at a time,
                # to keep memory consumption down.
                futures.update({
                    executor.submit(fn, **{'cur_set': params}, **constSets): 
                        params[1]
                        for params in it.islice(fn_inputs, len(done))
                })
    except GeneratorExit:
        raise
        # arcpy.AddError("Need to do some clean up.")
        # yield 2, 'yup'
    except arcpy.ExecuteError:
        # for fut in futures:
        #     del fut
        # for fut in done:
        #     del fut
        func = sys._getframe(  ).f_code.co_name
        msgs = arcpyErr(func)
        yield [0, {'error': msgs}]
    except:
        # for fut in futures:
        #     del fut
        # for fut in done:
        #     del fut
        func = sys._getframe(  ).f_code.co_name
        msgs = pyErr(func)
        yield [0, {'error': msgs}]
if __name__ == '__main__':
    main()