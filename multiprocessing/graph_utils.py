#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Modified version of:
# This file is part of CbM (https://github.com/ec-jrc/cbm).
# Author    : Csaba Wirnhardt
# Credits   : GTCAP Team
# Copyright : 2021 European Commission, Joint Research Centre
# License   : 3-Clause BSD


import datetime
import logging
import calendar
import os
import time
import numpy as np
import pandas as pd
import matplotlib.dates as mdates
from matplotlib import pyplot
import matplotlib.ticker as ticker

def get_ndvi_profiles_from_csv(csv_file):
    ndvi_profile = pd.read_csv(csv_file)
    return ndvi_profile


def get_current_list_of_months(first_year_month, number_of_year_months):
    textstrs_tuples = [
        ("202001", "2020\nJAN"),
        ("202002", "2020\nFEB"),
        ("202003", "2020\nMAR"),
        ("202004", "2020\nAPR"),
        ("202005", "2020\nMAY"),
        ("202006", "2020\nJUN"),
        ("202007", "2020\nJUL"),
        ("202008", "2020\nAUG"),
        ("202009", "2020\nSEP"),
        ("202010", "2020\nOCT"),
        ("202011", "2020\nNOV"),
        ("202012", "2020\nDEC"),
        ("202101", "2021\nJAN"),
        ("202102", "2021\nFEB"),
        ("202103", "2021\nMAR"),
        ("202104", "2021\nAPR"),
        ("202105", "2021\nMAY"),
        ("202106", "2021\nJUN"),
        ("202107", "2021\nJUL"),
        ("202108", "2021\nAUG"),
        ("202109", "2021\nSEP"),
        ("202110", "2021\nOCT"),
        ("202111", "2021\nNOV"),
        ("202112", "2021\nDEC"),
    ]

    # find the index of the first occurrence of first_year_month in textstrs_tuples
    # and return the rest secend elements of the tuples of the list

    i = 0
    first_year_month_index = i
    for textstrs_tuple in textstrs_tuples:
        if first_year_month == textstrs_tuple[0]:
            first_year_month_index = i
        i += 1

    current_textstrs = []
    for i in range(first_year_month_index, first_year_month_index + number_of_year_months):
        current_textstrs.append(textstrs_tuples[i][1])

    return current_textstrs


def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def display_ndvi_profiles(parcel_id, crop, plot_title, out_tif_folder_base,
                          add_error_bars=False):
    """
    this function plots the NDVI profile and saves the figures to the outputFolder
    """
    y_tick_spacing = 0.1
    start = time.time()
    chip_folder = str(parcel_id)
    ndvi_folder = out_tif_folder_base + "/ndvi"
    ndvi_csv_file = ndvi_folder + "/" + chip_folder + "_ndvi.csv"
    output_graph_folder = out_tif_folder_base + "/ndvi_graphs"
    if not os.path.exists(output_graph_folder):
        os.makedirs(output_graph_folder)
    ndvi_profile = pd.read_csv(ndvi_csv_file)

    ndvi_profile['acq_date'] = pd.to_datetime(ndvi_profile.acq_date)
    ndvi_profile = ndvi_profile.sort_values(by=['acq_date'])
    # rename the column names from 'ndvi_mean' to more meaningful name
    ndvi_profile = ndvi_profile.rename(columns={'ndvi_mean': 'S2 NDVI'})
    ndvi_profile = ndvi_profile.rename(columns={'acq_date': 'date'})

    # check if there are real NDVI values and stdev values in the dataframe
    # (for very small parcels the values in the csv can be None which evaluates as object in
    # the dataframe, insted of dtype float64
    if not ndvi_profile['S2 NDVI'].dtypes == "float64" or \
            not ndvi_profile['ndvi_std'].dtypes == "float64":
        return

    # plot the time series
    ax0 = pyplot.gca()

    if not ndvi_profile.empty:
        if add_error_bars:
            #ndvi_profile.plot(kind='scatter', marker='+', x='date',y='S2 NDVI', yerr='ndvi_std', color = 'blue', ax=ax0)

            ndvi_profile.plot(kind='line', marker='+', x='date', y='S2 NDVI', yerr='ndvi_std', color='blue', ax=ax0,
                              label='pol mean', capsize=4, ecolor='grey', barsabove='True')
        else:
            ndvi_profile.plot(kind='line', marker='+', x='date',
                              y='S2 NDVI', color='blue', label='pol mean', ax=ax0)

    # Smooth line using polynomial regression
    date_num = mdates.date2num(ndvi_profile['date'])
    poly = np.polyfit(date_num, ndvi_profile['S2 NDVI'], 5)
    ndvi_profile['pol_regr'] = np.poly1d(poly)(date_num)
    ndvi_profile.plot(kind='line', x='date', y='pol_regr',
                      color='red', label='trend', ax=ax0)

    # format the graph a little bit
    pyplot.ylabel('NDVI')
    #parcelNumber = ndvi_profile.iloc[0]['id']
    pyplot.title(plot_title + ", Id: " + str(parcel_id) + ", " + crop)
    ax0.legend()
    ax0.set_ylim([0, 1])
    ax0.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax0.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    ax0.xaxis.grid()  # horizontal lines
    ax0.yaxis.grid()  # vertical lines

    fig = pyplot.gcf()
    fig.autofmt_xdate()  # Rotation
    fig_size_x = 13
    fig_size_y = 7
    fig.set_size_inches(fig_size_x, fig_size_y)

    min_month = min(ndvi_profile['date']).date().month
    min_year = min(ndvi_profile['date']).date().year

    max_month = max(ndvi_profile['date']).date().month
    max_year = max(ndvi_profile['date']).date().year

    number_of_months = diff_month(
        max(ndvi_profile['date']).date(), min(ndvi_profile['date']).date()) + 1
    ax0.set_xlim([datetime.date(min_year, min_month, 1),
                  datetime.date(max_year, max_month,
                                calendar.monthrange(max_year, max_month)[1])])

    min_year_month = str(min_year) + ('0' + str(min_month))[-2:]
#     start_x = 0.045
    step_x = 1/number_of_months
    start_x = step_x/2  # positions are in graph coordinate system between 0 and 1
    # so first year_month label is at half the size of the widht of
    # one month

    loc_y = 0.915

    current_year_month_text = get_current_list_of_months(
        min_year_month, number_of_months)

    for current_year_month_index in range(0, number_of_months):
        t = current_year_month_text[current_year_month_index]
        loc_x = start_x + (current_year_month_index) * step_x
        ax0.text(loc_x, loc_y, t, verticalalignment='bottom', horizontalalignment='center', transform=ax0.transAxes,
                 color='blue', fontsize=13)

    ax0.yaxis.set_major_locator(ticker.MultipleLocator(y_tick_spacing))

    # save the figure to a jpg file
    fig.savefig(output_graph_folder + '/' + str(parcel_id) + '_NDVI.jpg')
    pyplot.close(fig)
    logging.info((datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\t" + str(parcel_id) + "\tgraph_utils.display_ndvi_profiles:\t" +
                  "{0:.3f}".format(time.time() - start)))

    return ndvi_profile


def display_ndvi_profiles_with_mean_profile_of_the_crop(parcel_id, crop, plot_title, out_tif_folder_base, logfile,
                                                        add_error_bars=False):
    """
    this function plots the NDVI profile and saves the figures to the outputFolder
    """
    mean_profile_folder = "c:/Users/Csaba/ownCloud/GTCAP/cbm_qa/be_fl/notebooks/output_csv_selected_v02"
    start = time.time()
    chip_folder = str(parcel_id) + '_' + crop
    ndvi_folder = out_tif_folder_base + "/ndvi"
    ndvi_csv_file = ndvi_folder + "/" + chip_folder + "_ndvi.csv"
    mean_ndvi_csv_file = mean_profile_folder + "/" + crop  # + ".csv"
    output_graph_folder = out_tif_folder_base + "/ndvi_graphs_with_mean"

    if not os.path.exists(output_graph_folder):
        os.makedirs(output_graph_folder)
    ndvi_profile = pd.read_csv(ndvi_csv_file)

    ndvi_profile['acq_date'] = pd.to_datetime(ndvi_profile.acq_date)
    ndvi_profile = ndvi_profile.sort_values(by=['acq_date'])
    # rename the column names from 'ndvi_mean' to more meaningful name
    ndvi_profile = ndvi_profile.rename(columns={'ndvi_mean': 'S2 NDVI'})
    ndvi_profile = ndvi_profile.rename(columns={'acq_date': 'date'})

    mean_ndvi_csv_file_exists = False
    if os.path.isfile(mean_ndvi_csv_file):
        mean_ndvi_csv_file_exists = True
        mean_ndvi_profile = pd.read_csv(mean_ndvi_csv_file)
        mean_ndvi_profile['acq_date'] = pd.to_datetime(
            mean_ndvi_profile.acq_date)
        mean_ndvi_profile = mean_ndvi_profile.sort_values(by=['acq_date'])
        # rename the column names from 'ndvi_mean' to more meaningful name
        mean_ndvi_profile = mean_ndvi_profile.rename(
            columns={'ndvi_mean': 'S2 NDVI mean'})
        mean_ndvi_profile = mean_ndvi_profile.rename(
            columns={'acq_date': 'date'})

    # check if there are real NDVI values and stdev values in the dataframe
    # (for very small parcels the values in the csv can be None which evaluates as object in
    # the dataframe, insted of dtype float64
    if not ndvi_profile['S2 NDVI'].dtypes == "float64" or \
            not ndvi_profile['ndvi_std'].dtypes == "float64":
        return

    # plot the time series
    ax0 = pyplot.gca()

    if not ndvi_profile.empty:
        if add_error_bars:
            ndvi_profile.plot(kind='line', marker='+', x='date', y='S2 NDVI', yerr='ndvi_std', color='blue', ax=ax0,
                              capsize=4, ecolor='grey', barsabove='True')
        else:

            ndvi_profile.plot(kind='line', marker='+', x='date',
                              y='S2 NDVI', color='blue', ax=ax0)
            if mean_ndvi_csv_file_exists:
                mean_ndvi_profile.plot(
                    kind='line', marker='+', x='date', y='S2 NDVI mean', color='red', ax=ax0)

    # format the graph a little bit
    pyplot.ylabel('NDVI')
    parcelNumber = ndvi_profile.iloc[0]['Field_ID']
    pyplot.title(plot_title + ", Parcel id: " + str(parcelNumber) + " " + crop)
    ax0.set_ylim([0, 1])
    ax0.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax0.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    ax0.xaxis.grid()  # horizontal lines
    ax0.yaxis.grid()  # vertical lines

    fig = pyplot.gcf()
    fig.autofmt_xdate()  # Rotation
    fig_size_x = 13
    fig_size_y = 7
    fig.set_size_inches(fig_size_x, fig_size_y)

    min_month = min(ndvi_profile['date']).date().month
    min_year = min(ndvi_profile['date']).date().year

    max_month = max(ndvi_profile['date']).date().month
    max_year = max(ndvi_profile['date']).date().year

    number_of_months = diff_month(
        max(ndvi_profile['date']).date(), min(ndvi_profile['date']).date()) + 1
    ax0.set_xlim([datetime.date(min_year, min_month, 1),
                  datetime.date(max_year, max_month,
                                calendar.monthrange(max_year, max_month)[1])])

    min_year_month = str(min_year) + ('0' + str(min_month))[-2:]
    step_x = 1/number_of_months
    start_x = step_x/2  # positions are in graph coordinate system between 0 and 1
    # so first year_month label is at half the size of the widht of
    # one month

    loc_y = 0.915

    current_year_month_text = get_current_list_of_months(
        min_year_month, number_of_months)

    for current_year_month_index in range(0, number_of_months):
        t = current_year_month_text[current_year_month_index]
        loc_x = start_x + (current_year_month_index) * step_x
        ax0.text(loc_x, loc_y, t, verticalalignment='bottom', horizontalalignment='center', transform=ax0.transAxes,
                 color='blue', fontsize=13)

    # save the figure to a jpg file
    fig.savefig(output_graph_folder + '/parcel_id_' +
                str(parcel_id) + '_NDVI.jpg')
    pyplot.close(fig)
    logging.info((datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\t" + parcel_id +
                 "\tgraph_utils.display_ndvi_profiles_with_mean_profile_of_the_crop:\t" + "{0:.3f}".format(time.time() - start)))
    return ndvi_profile


def display_ndvi_profiles_with_mean_profile_of_the_crop_with_std(parcel_id, crop, plot_title, out_tif_folder_base,
                                                                 mean_profile_folder, add_error_bars=False,
                                                                 mean_color='blue', current_color='red'):
    """
    this function plots the NDVI profile and saves the figures to the outputFolder
    """

    start = time.time()
    chip_folder = str(parcel_id)
    ndvi_folder = out_tif_folder_base + "/ndvi"
    ndvi_csv_file = ndvi_folder + "/" + chip_folder + "_ndvi.csv"

    mean_ndvi_csv_file = mean_profile_folder + "/" + crop + ".csv"
    output_graph_folder = out_tif_folder_base + "/ndvi_graphs_with_mean"

    if not os.path.exists(output_graph_folder):
        os.makedirs(output_graph_folder)
    ndvi_profile = pd.read_csv(ndvi_csv_file)

    ndvi_profile['acq_date'] = pd.to_datetime(ndvi_profile.acq_date)
    ndvi_profile = ndvi_profile.sort_values(by=['acq_date'])
    # rename the column names from 'ndvi_mean' to more meaningful name
    ndvi_profile = ndvi_profile.rename(columns={'ndvi_mean': 'S2 NDVI'})
    ndvi_profile = ndvi_profile.rename(columns={'acq_date': 'date'})

    mean_ndvi_csv_file_exists = False
    if os.path.isfile(mean_ndvi_csv_file):
        mean_ndvi_csv_file_exists = True
        mean_ndvi_profile = pd.read_csv(mean_ndvi_csv_file)
        mean_ndvi_profile['acq_date'] = pd.to_datetime(
            mean_ndvi_profile.acq_date)
        mean_ndvi_profile = mean_ndvi_profile.sort_values(by=['acq_date'])
        # rename the column names from 'ndvi_mean' to more meaningful name
        mean_ndvi_profile = mean_ndvi_profile.rename(
            columns={'ndvi_mean': 'S2 NDVI mean'})
        mean_ndvi_profile = mean_ndvi_profile.rename(
            columns={'acq_date': 'date'})

    # check if there are real NDVI values and stdev values in the dataframe
    # (for very small parcels the values in the csv can be None which evaluates as object in
    # the dataframe, insted of dtype float64
    if not ndvi_profile['S2 NDVI'].dtypes == "float64" or \
            not ndvi_profile['ndvi_std'].dtypes == "float64":
        return

    # plot the time series
    ax0 = pyplot.gca()

    if not ndvi_profile.empty:
        if add_error_bars:
            ndvi_profile.plot(kind='line', marker='+', x='date', y='S2 NDVI', yerr='ndvi_std', color=current_color, ax=ax0,
                              capsize=4, ecolor='grey', barsabove='True', label="NDVI polygon")

        else:
            ndvi_profile.plot(kind='line', marker='+', x='date',
                              y='S2 NDVI', color=current_color, ax=ax0)

        if mean_ndvi_csv_file_exists:
            mean_ndvi_profile.plot(kind='line', x='date', y='S2 NDVI mean',
                                   color=mean_color, ax=ax0, label="crop's mean NDVI")

            pyplot.fill_between(mean_ndvi_profile['date'],
                                mean_ndvi_profile['S2 NDVI mean'] -
                                mean_ndvi_profile['ndvi_stdev'],
                                mean_ndvi_profile['S2 NDVI mean'] +
                                mean_ndvi_profile['ndvi_stdev'],
                                alpha=0.2, color=mean_color)

    # format the graph a little bit
    pyplot.ylabel('NDVI')
    #parcelNumber = ndvi_profile.iloc[0]['id']
    pyplot.title(plot_title + ", Id: " + str(parcel_id) + ", " + crop)
    ax0.legend()
    ax0.set_ylim([0, 1])
    ax0.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax0.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    ax0.xaxis.grid()  # horizontal lines
    ax0.yaxis.grid()  # vertical lines

    fig = pyplot.gcf()
    fig.autofmt_xdate()  # Rotation
    fig_size_x = 13
    fig_size_y = 7
    fig.set_size_inches(fig_size_x, fig_size_y)

    min_month = min(ndvi_profile['date']).date().month
    min_year = min(ndvi_profile['date']).date().year

    max_month = max(ndvi_profile['date']).date().month
    max_year = max(ndvi_profile['date']).date().year

    number_of_months = diff_month(
        max(ndvi_profile['date']).date(), min(ndvi_profile['date']).date()) + 1
    ax0.set_xlim([datetime.date(min_year, min_month, 1),
                  datetime.date(max_year, max_month,
                                calendar.monthrange(max_year, max_month)[1])])

    min_year_month = str(min_year) + ('0' + str(min_month))[-2:]
    step_x = 1/number_of_months
    start_x = step_x/2  # positions are in graph coordinate system between 0 and 1
    # so first year_month label is at half the size of the widht of
    # one month

    loc_y = 0.915

    current_year_month_text = get_current_list_of_months(
        min_year_month, number_of_months)

    for current_year_month_index in range(0, number_of_months):
        t = current_year_month_text[current_year_month_index]
        loc_x = start_x + (current_year_month_index) * step_x
        ax0.text(loc_x, loc_y, t, verticalalignment='bottom', horizontalalignment='center', transform=ax0.transAxes,
                 color='blue', fontsize=13)

    # save the figure to a jpg file
    fig.savefig(output_graph_folder + '/parcel_id_' +
                str(parcel_id) + '_NDVI.jpg')
    pyplot.close(fig)
    logging.info((datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\t" + parcel_id +
                 "\tgraph_utils.display_ndvi_profiles_with_mean_profile_of_the_crop:\t" + "{0:.3f}".format(time.time() - start)))

    return ndvi_profile
