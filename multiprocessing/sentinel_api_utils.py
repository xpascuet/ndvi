#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Xavi Pascuet

import pandas as pd
from multiprocessing import Process, JoinableQueue, Queue
import logging
from sentinelhub import SentinelHubStatistical, DataCollection, CRS,  \
    Geometry, SHConfig, parse_time, SentinelHubStatisticalDownloadClient
import graph_utils
from time import sleep

config = SHConfig()


def stats_to_df(stats_data):
    """ Transform a Sentinel Hub Statistical API response into a pandas.DataFrame

    Args: stats_data: Sentinel Hub Statistica response on json format

    Returns: Pandas Dataframe

    """
    # Define empty list to store dictionaris
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
    """ 
    Request ndvi yearly time series for a collection of polygons(geodataframe)

    Args:
        geodf: GeopandasDataframe

    Returns:
        ndvi_stats: Sentinel Satistical API's response on json format
    """
    # List of requests
    ndvi_requests = []
    # Iterate throw polygons creating a request for each
    for geo_shape in geodf.geometry.values:
        ndvi_request = SentinelHubStatistical(
            aggregation=SentinelHubStatistical.aggregation(
                evalscript=ndvi_evalscript,
                time_interval=('2021-01-01', '2021-11-30'),
                aggregation_interval='P1D',
                resolution=(100, 100)),
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


def plot_ndvi(geodf, id_column, crop_column, dest_dir, cdir, plot_title, n_request, request_size):
    """
    Plot ndvi yearly time series for a collection of polygons

    Args:
        geodf: GeoPandas Dataframe
        id_column: (int) Polygon identifier
        crop_column: (str) Polygon crop name
        dest_dir: (str) destination directory
        cdir: (str) base directory 
        plot_title: (str) Plot title
        n_request: (int) Request ordinary number 
        request_size: (int) Number of polygons that contains each API request 

    Returns:
        None
    """
    # Get subdataframe
    subdf = geodf.iloc[(n_request-1)*request_size:(n_request)*request_size]

    try:
        logging.info("\tStarting API request number:{}".format(n_request))
        # Get ndvi stats for sub_geodataframe
        ndvi_stats = sentinelapi_request(subdf)
        # Iterate throw geometries in subgeodataframe
        for parcel_id, crop, rec_stats in zip(subdf[id_column], subdf[crop_column], ndvi_stats):
            try:
                # Parse API response into a Dataframe
                ndvi_df = stats_to_df(rec_stats)
                # Rename columns acording to Cbm script
                ndvi_df.rename(columns={'interval_from': 'acq_date', 'ndvi_B0_mean': 'ndvi_mean',
                                        'ndvi_B0_stDev': 'ndvi_std'}, inplace=True)
                # Export csv
                ndvi_df.to_csv(dest_dir + "/" + str(parcel_id) + "_ndvi.csv")
                # Plot ndvi time series
                graph_utils.display_ndvi_profiles(parcel_id, crop, plot_title, cdir,
                                                  add_error_bars=True)
            except Exception as e:
                logging.error(
                    'Polygon number {} failed: {}'.format(parcel_id, e))
                continue

    except Exception as e:
        logging.error('Request number {} failed: {}'.format(n_request, e))


def get_plot_proc(geodf, id_column, crop_column, request_size, dest_dir, cdir, plot_title, p_index, requests_q, results_q):
    """
    Get tasks (ndvi graph's to plot) from request_q queue, and store results on results_q.
    End when there isn't anymore tasks.

    Args:
        geodf: GeoPandas Dataframe
        id_column: (int) Polygon identifier
        crop_column: (str) Polygon crop name
        request_size: (int) Number of polygons that contains each API request 
        dest_dir: (str) destination directory
        cdir: (str) base directory 
        plot_title: (str) Plot title
        p_index: (int) Process number
        requests_q: (JoinableQueue) requests's queue
        results_q: (Queue) results queue

    Return:
        None
    """

    logging.info("[P{}]\tStarted".format(p_index))
    # Get first task
    request = requests_q.get()
    # While pending tasks
    while request:
        logging.info(
            "[P{}]\tStarting to work on request number:{}".format(p_index, request))
        plot_ndvi(geodf, id_column, crop_column, dest_dir,
                  cdir, plot_title, request, request_size)
        logging.info("[P{}]\tPDone".format(p_index))
        # Store result
        results_q.put(request)
        # Indicate task is done
        requests_q.task_done()
        # Get next task
        request = requests_q.get()

    logging.info("[P{}]\tEnding".format(p_index))
    # End last task (was None indicator)
    requests_q.task_done()
    logging.info("[P{}]\tProcess ended".format(p_index))


def plot_ndvi_multiprocess(geodf, id_column, crop_column, dest_dir, cdir, plot_title, n_processes, request_size):
    """
    Plot ndvi yearly time series for a collection of polygons ('geodf') using 'n_processes' processes
    to request Sentinel Statistical API by sets of  "request_size" polygons

    Args:
        geodf: GeoPandas Dataframe
        id_column: (int) Polygon identifier
        crop_column: (str) Polygon crop name
        dest_dir: (str) destination directory
        cdir: (str) base directory 
        plot_title: (str) Plot title
        n_processes: (int) number of processes to split the task into
        request_size: (int) Number of polygons that contains each API request 

    Returns:
        r_list: (list) Result's list'
    """
    results_q = Queue()
    requests_q = JoinableQueue()
    # Create request's queue
    for request in range(1, (int(len(geodf)/request_size) + (len(geodf) % request_size > 0)) + 1):
        requests_q.put(request)
    # Add an ending indicador for each process
    for _ in range(n_processes):
        requests_q.put(None)
    # Starts n_processes plotting procedures
    for i in range(n_processes):
        process = Process(target=get_plot_proc, args=(geodf, id_column, crop_column,
                          request_size, dest_dir, cdir, plot_title, i, requests_q, results_q))
        process.start()
        sleep(60)
    # Wait all processes to end
    logging.info("[M]\tWaiting to join processes")
    requests_q.join()
    logging.info("[M]\tProcesses joined!")
    # Create result's list
    r_list = []
    while not results_q.empty():
        r_list.append(results_q.get())

    return r_list


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
