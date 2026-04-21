# Soil Data Development Toolbox for ArcGIS Pro Beta version
Suite of tools used to interact with [Soil Survey Geographic Databases (SSURGO)](https://www.nrcs.usda.gov/resources/data-and-reports/gridded-soil-survey-geographic-gssurgo-database)
This python toolbox was designed to process and manage SSURGO downloads from Web Soil Survey and use them to create gSSURGO databases. Also includes tools to aggregate and map soils information within gSSURGO. 
This toolbox is still under development.

The Download and Construct Databases scripts are fully developed.

The Summarize Soil Information (analogous to the Create Soil Map functionality in the ArcMap version of SDDT) is now operational and the key aggregation logic in place. 
Recently Added:
1. Flooding and Ponding Frequency
2. Improved mapping functionality
3. Raster Lookup functionality
4. Table merging functionality
5. Component Crop Yield

The following elements are still planned to be complete:
2. Aggrgation needs to be enabled for the following tables
   - Component Moisture
   - Component Ecoclass
   - Component diagnostic horizons
   - Component Restrictions
   - CH Texture Grp
   - Component Parent Material grp
   - CH Ashsto
   - CH Unified
   - Component forprod
   - fragments 
   - Component Soil temp class
   - CH designation suffix
   - Component Taxonomic Moisture Class
   - CH Structure
   - CH Pores
   - Component Parent Material
   - Component Canopy Cover
   - Component Plants
4. Add metadata to summary tables
5. Add SDV category filters
6. Handle multiple depth layers?

Other tools that will be added to the toolbox:
1. Expand mapping functionality to gpkg version
2. Clip databases by a aerial units
3. Add/Delete soil survey areas
