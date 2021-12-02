# NDVI calculation using SentinelHub Statistical API

## 1. Description
This code uses the SentinelHub Statistical API to calculate the NDVI time series for a set of polygons.

The polygons must be in a geojson although it the code could be easily adapted to work with a .csv file with WKT geometries.

Clouds have already been masked

## 2.- Prerequisites
Apart from the libraries in requirements.txt, must have the sentinelhub python package installed, Instructions are: https://sentinelhub-py.readthedocs.io/en/latest/install.html

Must have a Sentinel  Hub account with an Oath client configured: https://sentinelhub-py.readthedocs.io/en/latest/configure.html

The coordinates reference systems supported are: https://docs.sentinel-hub.com/api/latest/api/process/crs/

## 3.- Code structure
The script to run is the ndvi_plot.py.

The sentinel_api_utils.py script contains the necessary functions to request the API, transform the json response to a csv file, and get the main NDVI time series for crop.

The graph_utils.py contains the necessary functions to plot.

There’s also a multiprocessing version of the code at:  [multiprocessing](https://github.com/xpascuet/ndvi/multiprocessing)
