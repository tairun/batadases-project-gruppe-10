#!/usr/bin/env python3

import os
import logging
from tqdm import tqdm
from dotenv import load_dotenv
import pandas as pd
from psycopg2.errors import UniqueViolation

from utils import *
from DataIntegrator import DataIntegrator

from typing import List, Generator, Tuple

logging.getLogger().disabled = True


class GdeltIntegrator(DataIntegrator):
    """
    Class description.
    """

    def __init__(self):
        self.table_script = "./schema/prepare_database.psql"

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
                "headers": [self.headers[i] for i in [56, 57]],
                "attributes": ["DATEADDED", "SOURCEURL"],
                "uniques": ["SOURCEURL"]
            },
            "event_geo": {
                "headers": [[self.headers[i] for i in
                             [38, 41, 37, 39, 40, 35, 36]], [self.headers[i] for i in
                                                             [45, 48, 44, 46, 47, 42, 43]], [self.headers[i] for i in
                                                                                             [52, 55, 51, 53, 54, 49, 50]]],
                "attributes": ["ADM1Code", "FeatureID", "CountryCode", "Lat", "Long", "Type", "FullName"],
                "uniques": ["ADM1Code"]
            },
            "actor": {
                "attributes": ["Code", "Name", "KnownGroupCode", "Religion1Code", "Religion2Code", "CountryCode", "Type1Code", "Type2Code", "Type3Code", "EthnicCode"],
                "uniques": []
            },
            "actor1":  {
                "headers": [self.headers[i]
                            for i in [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 38]],
                "attributes": ["Code", "Name", "KnownGroupCode", "Religion1Code", "Religion2Code", "CountryCode", "Type1Code", "Type2Code", "Type3Code", "EthnicCode", "ADM1Code"],
                "uniques": []
            },
            "actor2": {
                "headers": [self.headers[i]
                            for i in [15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 45]],
                "attributes": ["Code", "Name", "KnownGroupCode", "Religion1Code", "Religion2Code", "CountryCode", "Type1Code", "Type2Code", "Type3Code", "EthnicCode", "ADM1Code"],
                "uniques": []
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
                    26, 5, 15, 52, 27, 28, 25, 30, 29, 34, 31, 32, 33]],
                "attributes": ["EventCode", "Actor1Code", "Actor2Code", "ADM1Code", "EventBaseCode", "EventRootCode", "IsRootEvent", "GoldsteinScale", "QuadClass", "AvgTone", "NumMentions", "NumSources", "NumArticles"],
                "uniques": []
            },
            "eventid_and_date": {
                "headers": [self.headers[i] for i in [0, 57, 26, 4, 56, 2, 3]],
                "attributes": ["GlobalEventID", "SOURCEURL", "EventCode", "FractionDate", "Day", "MonthYear", "Year"],
                "uniques": []
            }
        }

    def insert_data(self, conn, cur, row: pd.DataFrame, table_name: str) -> None:
        """
        Inserts the row into the database specified by conn, cur. The {table_name} specified the table to be inserted into.
        """
        #print(f"Inserting in to table: {table_name}")

        try:
            columns = self.tables[table_name]["headers"]
        except KeyError as e:
            logging.error(
                f"You forgot to specify the 'headers' on table {table_name}.")
            raise e

        # Check if the element is a nested list and extract it.
        if isinstance(columns[0], list):
            for more_columns in columns:
                # Generate enough parameters for query string.
                parameter_string = ','.join(['%s']*len(more_columns))

                # Extract relevant columns from dataframe and cast it to a list.
                row_list = row[more_columns].values.tolist()

                # Do not perform insert if the first value is empty (which is always the primary key).
                if not row_list[0]:
                    continue

                # Cast empty values to None for DB adapter to work.
                row_list = [None if not x else x for x in row_list]

                try:
                    attribute_string = ','.join(
                        self.tables[table_name]["attributes"])
                    unique_string = ','.join(
                        self.tables[table_name]["uniques"])
                except KeyError as e:
                    logging.error(
                        f"Your forgot to specify either the 'attributes' or the 'uniques' ont the table {table_name}")
                    raise e

                # Catch uniqueness constraint with "UPSERT" method
                if not unique_string:
                    insert_string = f"INSERT INTO {table_name} ({attribute_string}) VALUES ({parameter_string})"
                else:
                    insert_string = f"INSERT INTO {table_name} ({attribute_string}) VALUES ({parameter_string}) ON CONFLICT ({unique_string}) DO NOTHING"

                try:
                    cur.execute(insert_string, row_list)
                except UniqueViolation as e:
                    print(
                        f"Something went wrong during inserting the following data: {row_list}")
        else:
            # Generate enough parameters for query string.
            parameter_string = ','.join(['%s']*len(columns))

            # Extract relevant columns from dataframe and cast it to a list.
            row_list = row[columns].values.tolist()

            # Do not perform insert if the first value is empty (which is always the primary key).
            if not row_list[0]:
                return

            # Cast empty values to None for DB adapter to work.
            row_list = [None if not x else x for x in row_list]

            try:
                attribute_string = ','.join(
                    self.tables[table_name]["attributes"])
                unique_string = ','.join(
                    self.tables[table_name]["uniques"])
            except KeyError as e:
                logging.error(
                    f"Your forgot to specify either the 'attributes' or the 'uniques' ont the table {table_name}")
                raise e

            # Catch uniqueness constraint with "UPSERT" method
            if not unique_string:
                insert_string = f"INSERT INTO {table_name} ({attribute_string}) VALUES ({parameter_string})"
            else:
                insert_string = f"INSERT INTO {table_name} ({attribute_string}) VALUES ({parameter_string}) ON CONFLICT ({unique_string}) DO NOTHING"

            try:
                cur.execute(insert_string, row_list)
            except UniqueViolation as e:
                logging.error(
                    f"Something went wrong during inserting the following data: {row_list}")

            # def insert_data_management_fields(self, row, conn, cur) -> None:
            #     parameter_list = ['%s']*len(self.gdelt_data_management_fields)
            #     insert_string = f"INSERT INTO data_management_fields (dateadded, sourceurl) VALUES ({','.join(parameter_list)}) ON CONFLICT (sourceurl) DO NOTHING"
            #     row_list = row[self.gdelt_data_management_fields].values.tolist()

            #     try:
            #         cur.execute(insert_string, row_list)
            #     except UniqueViolation as e:
            #         print("Duplicate URL!!!")

            # def insert_action_geo(self, row, conn, cur) -> None:
            #     parameter_list = ['%s']*11
            #     insert_string = f"INSERT INTO actor1 VALUES ({','.join(parameter_list)})"
            #     row_list = row[self.gdelt_actor1].values.tolist()
            #     cur.execute(insert_string, row_list)

            # def insert_actor1(self, row, conn, cur) -> None:
            #     parameter_list = ['%s']*11
            #     insert_string = f"INSERT INTO actor1 VALUES ({','.join(parameter_list)})"
            #     row_list = row[self.gdelt_actor1].values.tolist()
            #     cur.execute(insert_string, row_list)

    def insert_wrapper(self, file_path, table_names: List[str] = None) -> None:
        table_names = table_names if table_names else self.table_names

        num_rows = bufcount(file_path)
        conn, cur = self.connect_database(autocommit=True)
        for row in tqdm(self.read_csv(file_path, headers=self.headers, limit=None), desc="Inserting rows into table ...", total=num_rows, mininterval=5.0, miniters=1000):
            for table_name in table_names:
                self.insert_data(conn, cur, row, table_name)

        cur.close()
        conn.close()


if __name__ == "__main__":
    load_dotenv()

    file = "raw_data/20191027.export.CSV"
    integrator = GdeltIntegrator()
    # compare_stuff(integrator) # Make sure 'attributes' and 'headers' have the same length.

    _ = input(f"Press 'Enter' to start the integration process ...")
    integrator.execute_script(integrator.table_script)  # Create the tables.
    table_names = ["data_management_fields", "event_geo", "actor1", "actor2",
                   "event_action", "eventid_and_date"]  # Smaller table list to fill.
    # Actually insert the data.
    integrator.insert_wrapper(file, table_names=table_names)
