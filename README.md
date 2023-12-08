# Data collection
### * pyroCb_events.csv contains the location, time, and other basic information of the pyroCb events
### * elevation_data_export.csv contains the elevation data for each location exported from GEE
### * code files starting with "wget" or contain "presssure level"/'single level' can be run to download ERA5 hourly and monthly averaged hour data on single levels and pressure levels
# Pre-processing: extract_info_from_nc.ipynb
### * extract_info_from_nc.ipynb does the intial processing of the ERA5 nc files downloaded from Copernicus Climate Data Store
### * pickles.zip contains the outputs of extract_info_from_nc.ipynb
### * skewT.zip contains the skewT-logP plots produced by extract_info_from_nc.ipynb
### * the timeseries figures (wchich amount to 1.4 Gb) produced by extract_info_from_nc.ipynb take up too much volume and cannot be uploaded to Github
# Analysis
### * Final_analysis.ipynb will read in the pickle files and produce the box plots in plots.zip
# Plots
### * skewT.zip contains the skewT-logP plots for all of the pyroCb atmospheric profiles
### * plots.zip contains the box plots 2/3/4/5/6 hrs before pyroCb events
# Paper
### * CEE263C_Paper.doc and CEE263C_Paper.pdf
