#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Script to get ndvi time series for a geojson using Sentinel's Statistical API.


# Author: Xavi Pascuet

import os
import logging
import geopandas as gpd
from sentinelhub import SHConfig
import graph_utils
import sentinel_api_utils
import matplotlib
matplotlib.interactive(False)

# Import SentinelHub configuration
config = SHConfig()

geodf = gpd.read_file("dun2021.geojson")
id_column = "id"
crop_column = "PRODUCTE"

logging.basicConfig(filename="ndvi_processes.log", level=logging.INFO)

cdir = os.getcwd()
dest_dir = os.path.join(cdir, r'ndvi')
if not os.path.exists(dest_dir):
    os.mkdir(dest_dir)

plot_title = "NDVI 2021"
S = 100  #Number of polygons for request

# Iterate throw n subdataframes with len= S
for i in range(int(len(geodf)/S) + (len(geodf) % S > 0)):
    # Get subdataframe
    subdf = geodf.iloc[i*S:(i+1)*S]
    logging.info("\tStarting API request number:{}".format(i))

    try:
        # Get ndvi stats for sub_geodataframe
        ndvi_stats = sentinel_api_utils.sentinelapi_request(subdf)
        # Iterate throw geometries in subgeodataframe
        for parcel_id, crop, rec_stats in zip(subdf[id_column], subdf[crop_column], ndvi_stats):
            try:
                # Parse API response into a Dataframe
                ndvi_df = sentinel_api_utils.stats_to_df(rec_stats)
                # Rename columns acording to Cbm script
                ndvi_df.rename(columns={'interval_from': 'acq_date', 'ndvi_B0_mean': 'ndvi_mean',
                                        'ndvi_B0_stDev': 'ndvi_std'}, inplace=True)
                # Export csv
                ndvi_df.to_csv(dest_dir + "/" + str(parcel_id) + "_ndvi.csv")
                # Plot ndvi time series
                ndvi_profile = graph_utils.display_ndvi_profiles(
                    parcel_id, crop, plot_title, cdir, add_error_bars=True)
            except Exception as e:
                logging.error('Polygon number {} failed: {}'.format(parcel_id, e))
                continue
    except Exception as e:
        logging.error('Request number {} failed: {}'.format(i, e))
        continue
