#!/usr/bin/env python3


import os
import logging

import pandas as pd
from itertools import repeat
from psycopg2.errors import UniqueViolation
from concurrent.futures import ThreadPoolExecutor, ALL_COMPLETED, wait
from multiprocessing import Pool, Queue, Lock
from tqdm import tqdm

from src.utils import *
from src.GdeltDownloader import GdeltDownloader
from src.DataIntegrator import DataIntegrator

from typing import List, Generator, Tuple, Dict, Optional


class GdeltIntegrator(DataIntegrator):
    """
    Class description.
    """

    def __init__(self, start_date: Tuple[int, int, int], end_date: Tuple[int, int, int], url: str = "http://data.gdeltproject.org/events/", dl_path: str = "./data/gdelt"):
        self.downloader = GdeltDownloader(start_date, end_date, url, dl_path)
        self.table_script = "./schema/prepare_database.psql"
        self.data = "./data/20191027.export.CSV"

        self.table_names = ["data_management_fields", "event_geo", "actor", "actor1", "actor2",
                            "country", "income", "tourist", "influence_income", "event_action", "eventid_and_date"]

        self.headers = ["GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",                            "Actor1Code", "Actor1Name",
                        "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode", "Actor1Religion1Code",
                        "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code", "Actor2Code",
                        "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
                        "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code",
                        "Actor2Type3Code", "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
                        "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone", "Actor1Geo_Type",
                        "Actor1Geo_FullName", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_Lat",
                        "Actor1Geo_Long", "Actor1Geo_FeatureID", "Actor2Geo_Type", "Actor2Geo_FullName",
                        "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code", "Actor2Geo_Lat", "Actor2Geo_Long",
                        "Actor2Geo_FeatureID", "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
                        "ActionGeo_ADM1Code", "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID", "DATEADDED",
                        "SOURCEURL"]

        self.tables = {
            "data_management_fields": {
                "headers": [self.headers[i] for i in [0, 56, 57]],
                "attributes": ["GlobalEventID", "DATEADDED", "SOURCEURL"],
                "uniques": []  # FIXME ["SOURCEURL"]
            },
            "event_geo": {
                "headers": [[self.headers[i] for i in
                             [38, 41, 37, 39, 40, 35, 36]], [self.headers[i] for i in
                                                             [45, 48, 44, 46, 47, 42, 43]], [self.headers[i] for i in
                                                                                             [52, 55, 51, 53, 54, 49, 50]]],
                "attributes": ["ADM1Code", "FeatureID", "CountryCode", "Lat", "Long", "Type", "FullName"],
                "uniques": ["Actor1Geo_ADM1Code", "Actor2Geo_ADM1Code", "ActionGeo_ADM1Code"]
            },
            "actor": {
                "attributes": ["Code", "Name", "KnownGroupCode", "Religion1Code", "Religion2Code", "CountryCode", "Type1Code", "Type2Code", "Type3Code", "EthnicCode"],
                "uniques": ["Code"]
            },
            "actor1":  {
                "headers": [self.headers[i]
                            for i in [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 38]],
                "attributes": ["Code", "Name", "KnownGroupCode", "Religion1Code", "Religion2Code", "CountryCode", "Type1Code", "Type2Code", "Type3Code", "EthnicCode", "ADM1Code"],
                "uniques": ["Actor1Code"]
            },
            "actor2": {
                "headers": [self.headers[i]
                            for i in [15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 45]],
                "attributes": ["Code", "Name", "KnownGroupCode", "Religion1Code", "Religion2Code", "CountryCode", "Type1Code", "Type2Code", "Type3Code", "EthnicCode", "ADM1Code"],
                "uniques": ["Actor2Code"]
            },
            "country": {
                "headers": [],
                "attributes": ["CID", "Geo_Name"],
                "uniques": []
            },
            "income": {
                "headers": [],
                "attributes": ["IID", "CID", "Value", "Unit", "AgeGroup", "Type", "Year", "Sex"],
                "uniques": []
            },
            "tourist": {
                "headers": [],
                "attributes": ["TID", "CID", "Value", "Time", "Accommodation", "Unit"],
                "uniques": []
            },
            "influence_income": {
                "headers": [],
                "attributes": ["TID", "CID"],
                "uniques": []
            },
            "event_action": {
                "headers": [self.headers[i] for i in [
                    0, 26, 5, 15, 52, 27, 28, 25, 30, 29, 34, 31, 32, 33]],
                "attributes": ["GLobalEventID", "EventCode", "Actor1Code", "Actor2Code", "ADM1Code", "EventBaseCode", "EventRootCode", "IsRootEvent", "GoldsteinScale", "QuadClass", "AvgTone", "NumMentions", "NumSources", "NumArticles"],
                "uniques": []
            },
            "eventid_and_date": {
                "headers": [self.headers[i] for i in [0, 4, 56, 2, 3]],
                "attributes": ["GlobalEventID", "FractionDate", "Day", "MonthYear", "Year"],
                "uniques": ["GLOBALEVENTID"]
            }
        }
        super().__init__(self.table_names, self.tables)

    def download_and_integrate(self, max_workers: Optional[int] = None, table_names: List[str] = None) -> None:
        _ = input("Press 'Enter' to start download and extraction process ...")
        table_names = table_names if table_names else self.table_names

        file_list = self.downloader.get_file_links()

        # Assume we want to do everything in the main thread.
        if not max_workers:
            for file in tqdm(file_list, desc="Downloading and integrating GDELT files ...", mininterval=5.0):
                logging.info(file)
                self.gdelt_wrapper(file, self.downloader.dl_path, table_names)

        # Lets start some threads, shall we?
        else:
            logging.info(f"Starting {max_workers} thread(s).")
            executor = ThreadPoolExecutor(max_workers=max_workers)
            results = tqdm(executor.map(
                self.gdelt_wrapper, file_list, repeat(self.downloader.dl_path)), desc="Downloading and integrating GDELT files ...")

    def gdelt_wrapper(self, file: Dict, dl_path: str, table_names: List[str]) -> Optional[Tuple]:
        result = self.downloader.download_file(file, dl_path)
        if result:
            csv_file, success = result
            self.insert_wrapper2(csv_file, self.headers, seperator="\t",
                                 table_names=table_names)
            os.remove(csv_file)

        return result


if __name__ == "__main__":
    start_date = (2015, 1, 1)
    end_date = (2019, 12, 31)
    integrator = GdeltIntegrator(start_date, end_date)
    # compare_stuff(integrator) # Make sure 'attributes' and 'headers' have the same length.

    _ = input(f"Press 'Enter' to start the integration process ...")
    integrator.execute_script(integrator.table_script)  # Create the tables.
    table_names = ["data_management_fields", "event_geo", "actor1", "actor2",
                   "event_action", "eventid_and_date"]  # Smaller table list to fill.
    # Actually insert the data.
    integrator.insert_wrapper(
        integrator.data, headers=integrator.headers, seperator="\t", table_names=table_names)
