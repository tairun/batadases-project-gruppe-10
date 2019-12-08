#!/usr/bin/env python3

import logging
from argparse import ArgumentParser
from concurrent.futures import as_completed
from dotenv import load_dotenv

from src.GdeltDownloader import GdeltDownloader
from src.GdeltIntegrator import GdeltIntegrator
from src.IncomeIntegrator import IncomeIntegrator
from src.TourismIntegrator import TourismIntegrator

logging.basicConfig(level=logging.WARNING)
load_dotenv()


def main():
    # Download the GDELTv1.0 dataset from their website
    # ---
    # Define the URI, start and end date for the downloader.
    base_url = 'http://data.gdeltproject.org/events/'
    start_date = (2015, 1, 1)
    end_date = (2019, 12, 31)

    gdelt_raw_data = "./raw_data/gdelt"  # Specify the download directory
    # Create the downloader object
    gdelt = GdeltDownloader(base_url, start_date, end_date)
    # Parse the download page and extract all the links
    link_list = gdelt.get_file_links()
    # Download the individual files and extract the content from the zip archives
    results = gdelt.download_links(
        link_list, gdelt_raw_data, gdelt.max_threads)
    # TODO: Apply as_completed() to results list.

    _ = input(f"Press 'Enter' to start the integration process ...")
    integrator1 = GdeltIntegrator()
    table_names_1 = ["data_management_fields", "event_geo", "actor1", "actor2",
                     "event_action", "eventid_and_date"]  # Smaller table list to fill.
    # Actually insert the data.
    integrator1.execute_script(integrator1.table_script)  # Create the tables.

    integrator1.insert_wrapper(
        integrator1.data, headers=integrator1.headers, seperator="\t", table_names=table_names_1)

    integrator2 = IncomeIntegrator()
    table_names_2 = ["income"]  # Smaller table list to fill.

    integrator2.insert_wrapper(
        integrator2.data, headers=integrator2.headers, table_names=table_names_2)

    integrator3 = TourismIntegrator()
    table_names_3 = ["tourist"]  # Smaller table list to fill.

    integrator3.insert_wrapper(
        integrator3.data, headers=integrator3.headers, table_names=table_names_3)


if __name__ == "__main__":
    main()
