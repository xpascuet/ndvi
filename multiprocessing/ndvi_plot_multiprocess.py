"""
Created on Oct 08 18:42:49 2021

Script to plot ndvi time series for a set of poligons within a geodataframe.
Uses multiprocessing and the Sentinel Hub Statistical API

@autor: Xavi Pascuet
"""

import os
import logging
import geopandas as gpd
from sentinelhub import SHConfig
import matplotlib
matplotlib.interactive(False)

geodf = gpd.read_file("dun2021.geojson")

id_column = "id"
crop_column = "PRODUCTE"
request_size = 200  # Number of polygons of each request
n_processes = 3  # Number of processes to split the tasks into
plot_title = "NDVI 2021"


config = SHConfig()

logging.basicConfig(filename="ndvi_processes.log", level=logging.INFO)  # DEBUG

cdir = os.getcwd()

dest_dir = os.path.join(cdir, r'ndvi')
if not os.path.exists(dest_dir):
    os.mkdir(dest_dir)


sentinel_api_utils.plot_ndvi_multiprocess(
    geodf, id_column, crop_column, dest_dir, cdir, plot_title, n_processes, request_size)
