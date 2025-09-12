# Soil Data Development Toolbox for ArcGIS Pro Beta version
Suite of tools used to interact with [Soil Survey Geographic Databases (SSURGO)](https://www.nrcs.usda.gov/resources/data-and-reports/gridded-soil-survey-geographic-gssurgo-database)
This python toolbox was designed to process and manage SSURGO downloads from Web Soil Survey and use them to create gSSURGO databases. Also includes tools to aggregate and map soils information within gSSURGO. 
This toolbox is still under development. 
The Download and Construct Databases scripts are fully developed.
The Summarize Soil Information (analogous to the Create Soil Map functionality in the ArcMap version of SDDT) is now operational and the key aggregation logic in place. The following elements are still planned to be complete:
1) Symbolize SSURGO feature once added to map
2) Aggrgation needs to be enabled for the following tables
  a. Component Month
  b. Component Moisture
  c. Component Ecoclass
  d. Component Parent Material grp
  e. CH Ashsto
  f. CH Texture Grp
  g. CH Unified
  h. Component Crop Yield
  i. Component forprod
  j. fragments
  k. Component diagnostic horizons 
  l. Component Soil temp class
  m. CH designation suffix
  n. Component Restrictions
  o. Component Taxonomic Moisture Class
  p. CH Structure
  q. CH Pores
  s. Component Parent Material
  t. Component Canopy Cover
  u. Component Plants
4) Add SDV category filters
5) Handle multiple depth layers?

Other tools that will be added to the toolbox:
1) Expand mapping functionality to gpkg version
2) Clip databases by a aerial units
3) Add/Delete soil survey areas
