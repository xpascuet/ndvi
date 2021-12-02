#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Author: Xavi Pascuet

import os
import pandas as pd
import numpy as np
from sentinelhub import SentinelHubStatistical, DataCollection, CRS,  \
    Geometry, SHConfig, parse_time, SentinelHubStatisticalDownloadClient

config = SHConfig()


def stats_to_df(stats_data):
    """ Transform Statistical API response into a pandas.DataFrame
    """
    df_data = []

    for single_data in stats_data['data']:
        df_entry = {}
        is_valid_entry = True

        df_entry['interval_from'] = parse_time(
            single_data['interval']['from']).date()
        df_entry['interval_to'] = parse_time(
            single_data['interval']['to']).date()

        for output_name, output_data in single_data['outputs'].items():
            for band_name, band_values in output_data['bands'].items():

                band_stats = band_values['stats']
                if band_stats['sampleCount'] == band_stats['noDataCount']:
                    is_valid_entry = False
                    break

                for stat_name, value in band_stats.items():
                    col_name = f'{output_name}_{band_name}_{stat_name}'
                    if stat_name == 'percentiles':
                        for perc, perc_val in value.items():
                            perc_col_name = f'{col_name}_{perc}'
                            df_entry[perc_col_name] = perc_val
                    else:
                        df_entry[col_name] = value

        if is_valid_entry:
            df_data.append(df_entry)

    return pd.DataFrame(df_data)


ndvi_evalscript = """
// returns NDVI masking cloud pixels

function setup() {
  return {
    input: [
      {
        bands: ["B04", "B08", "CLM", "CLP", "dataMask"]
      }
    ],
    output: [
      {
        id: "ndvi",
        bands: 1
      },
      {
        id: "masks",
        bands: ["CLM"],
        sampleType: "UINT16"
      },
      {
        id: "dataMask",
        bands: 1
      }
    ]
  }
}

function evaluatePixel(samples) {
    // cloud probability normalized to interval [0, 1]
    let CLP = samples.CLP / 255.0;
    // masking cloudy pixels
    let combinedMask = samples.dataMask
    if (samples.CLM > 0) {
        combinedMask = 0;
    }
    return {
      ndvi: [index(samples.B08, samples.B04)],
      masks: [samples.CLM],
      dataMask: [combinedMask]
    };
}
"""


def sentinelapi_request(geodf):
    """ Request ndvi yearly time series for a colletion of polygons(geodataframe)
    Args:
        geodf: GeopandasDataframe

    Returns:
        ndvi_stats: Sentinel Satistical API's response on json format
    """

    ndvi_requests = []  # List of requests
	# Iterate throw polygons creating a request for each
    for geo_shape in geodf.geometry.values:
        ndvi_request = SentinelHubStatistical(
            aggregation=SentinelHubStatistical.aggregation(
                evalscript=ndvi_evalscript,
                time_interval=('2021-01-01', '2021-11-30'),
                aggregation_interval='P1D',
                resolution=(10, 10)),
            input_data=[SentinelHubStatistical.input_data(
                DataCollection.SENTINEL2_L2A, maxcc=0.8)],
            geometry=Geometry(geo_shape, CRS(geodf.crs)),
            config=config)

        ndvi_requests.append(ndvi_request)

    download_requests = [ndvi_request.download_list[0]
                         for ndvi_request in ndvi_requests]
    # Set client
    client = SentinelHubStatisticalDownloadClient(config=config)
    # Download from API
    ndvi_stats = client.download(download_requests)

    return ndvi_stats


def get_crop_mean_ndvi(df, id_column, crop_column, base_dir):
    """ Get mean ndvi for crop and export into csv files
    Args:
        df: Pandas Dataframe
        id_column: (int) Polygon identifier
        crop_column: (str) Polygon crop name
        base_dir: (str) base directory

    Returns:
        None
    """
    dest_dir = base_dir + "/crop_mean_ndvi"
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    for product in df[crop_column].unique():
        # Get dataframe for product
        df_product = df[df[crop_column] == product]
        # List of dataframes
        df_list = []
        # Iterate all id in product and read csv files
        for _id in df_product[id_column]:
            filename = base_dir + "/ndvi/" + str(_id) + "_ndvi.csv"
            ndvi_profile = pd.read_csv(filename)
            df_list.append(ndvi_profile)
        # Get total's dataframe
        df_total = pd.concat(df_list)
        # Group by day and calculate means
        df_total = df_total.groupby(["acq_date"], as_index=False)[
            ["ndvi_mean", "ndvi_std"]].mean()
        # Transfor string tate to datetime
        df_total['acq_date'] = pd.to_datetime(df_total.acq_date)
        # transform datetime to numeric (days since epoch)
        date_num = df_total['acq_date'].apply(lambda x: x.value)
        # Get polynomic regression for mean values
        poly = np.polyfit(date_num, df_total['ndvi_mean'], 5)
        df_total["ndvi_mean"] = np.poly1d(poly)(date_num)
        # Get polynomic regression for standard deviation
        poly = np.polyfit(date_num, df_total['ndvi_std'], 5)
        df_total["ndvi_std"] = np.poly1d(poly)(date_num)
        # Rename columns
        df_total.rename(columns={'ndvi_std': 'ndvi_stdev'}, inplace=True)
        # Export
        df_total.to_csv((dest_dir + "/" + product + ".csv"), index=False)
