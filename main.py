#!/usr/bin/env python3

import logging
from argparse import ArgumentParser
from dotenv import load_dotenv

from src.GdeltDownloader import GdeltDownloader
from src.GdeltIntegrator import GdeltIntegrator
from src.IncomeIntegrator import IncomeIntegrator
from src.TourismIntegrator import TourismIntegrator

logging.basicConfig(level=logging.INFO)
# logging.getLogger().disabled = True


def main():
    # Download the GDELTv1.0 dataset from their website
    # ---
    # Define the URI, start and end date for the downloader.
    base_url = 'http://data.gdeltproject.org/events/'
    start_date = (2015, 1, 1)
    end_date = (2017, 12, 31)

    gdelt_raw_data = "./data/gdelt"  # Specify the download directory
    # Create the downloader object
    # Parse the download page and extract all the links
    # Download the individual files and extract the content from the zip archives

    _ = input(f"Press 'Enter' to start the integration process ...")
    integrator1 = GdeltIntegrator(start_date, end_date)
    table_names_1 = ["data_management_fields", "event_geo", "actor1", "actor2",
                     "event_action", "eventid_and_date"]  # Smaller table list to fill.
    # Actually insert the data.
    integrator1.execute_script(integrator1.table_script)  # Create the tables.

    # integrator1.insert_wrapper2(
    #    integrator1.data, headers=integrator1.headers, seperator="\t", table_names=table_names_1)
    # results = integrator1.download_and_integrate(table_names=table_names_1)

    # integrator2 = IncomeIntegrator()
    # table_names_2 = ["income"]  # Smaller table list to fill.

    # integrator2.insert_wrapper(
    # integrator2.data, headers = integrator2.headers, table_names = table_names_2)

    integrator3 = TourismIntegrator()
    table_names_3 = ["tourist"]  # Smaller table list to fill.

    integrator3.insert_wrapper(
        integrator3.data, headers=integrator3.headers, table_names=table_names_3)


if __name__ == "__main__":
    main()
