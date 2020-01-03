#!/usr/bin/env python3

import io
import os
import csv
from tqdm import tqdm
import logging
import psycopg2
import pandas as pd
from getpass import getpass
from dotenv import load_dotenv
from psycopg2.errors import UniqueViolation, InFailedSqlTransaction

from src.utils import *

from typing import Tuple, Generator, List
logging.basicConfig(filename='./raw_data/gdelt.log',
                    filemode='w', level=logging.ERROR)


class DataIntegrator(object):
    """
    Base class for data integrator classes. Provides database connection and file reader functionality.
    """

    def __init__(self, table_names, tables):
        super().__init__()
        self.table_script = "./schema/prepare_database.psql"
        self.table_names = table_names
        self.tables = tables

    def read_csv(self, file_path: str, headers: List[str] = None, limit: int = None, seperator: str = ",") -> pd.DataFrame:
        """
        Generator that yields lines of a file.
        """
        with open(file_path, mode="r", encoding="utf8") as f:
            if limit:
                data = pd.read_csv(f, nrows=limit, sep=seperator,
                                   names=headers, encoding="utf8", na_filter=False, na_values=":", thousands=" ", low_memory=False)
            else:
                data = pd.read_csv(
                    f, sep=seperator, names=headers, encoding="utf8", na_filter=False, na_values=":", thousands=" ", low_memory=False)

            return data

    def connect_database(self, host: str = "localhost", port: int = 5432, dbname: str = None, user: str = None, passwd: str = None, autocommit: bool = False) -> Tuple:
        load_dotenv()
        try:
            dbname = dbname if dbname else os.environ["POSTGRES_DB"]
            user = user if user else os.environ["POSTGRES_USER"]
            passwd = passwd if passwd else os.environ["POSTGRES_PASSWORD"]
        except KeyError as e:
            print(e)
            logging.error(
                "Couldn't load database credentials from environment. Using defaults.")
            dbname = "db-proj-hs19"
            user = "db-proj"
            passwd = getpass(
                f"Enter password for database {dbname.upper()} and user {user.upper()}:")

        try:
            conn = psycopg2.connect(
                host=host, port=port, dbname=dbname, user=user, password=passwd)
            if autocommit:
                conn.set_session(autocommit=True)
            cur = conn.cursor()

            cur.execute("SELECT version()")
            logging.info(f"Connected to: {cur.fetchone()}")

            return conn, cur

        except Exception as e:
            print(e)
            logging.error("Could not connect to database!")
            return None, None

    def execute_script(self, script_path: str) -> None:
        logging.info(f"Executing script '{script_path}' ...")
        conn, cur = self.connect_database(autocommit=True)

        with open(script_path, "r") as script_handle:
            cur.execute(script_handle.read())

        cur.close()
        conn.close()

    def insert_data(self, conn, cur, row: pd.DataFrame, table_name: str) -> None:
        """
        Inserts the row into the database specified by conn, cur. The {table_name} specifies the table to be inserted into.
        """
        try:
            print(self.tables)
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
                # print(f"--- {row_list}")

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
                        f"Something went wrong during inserting into: {table_name}")
                    logging.info(e)
                except InFailedSqlTransaction as e:
                    logging.error(
                        f"Transaction stopped. Rolling back...: {table_name}")
                    logging.info(e)
                    # conn.rollback()
                    pass

        else:
            # Generate enough parameters for query string.
            parameter_string = ','.join(['%s']*len(columns))

            # Extract relevant columns from dataframe and cast it to a list.
            print(row)
            row_list = row[columns].values.tolist()

            # Do not perform insert if the first value is empty (which is always the primary key).
            if not row_list[0]:
                return

            # Cast empty values to None for DB adapter to work.
            row_list = [None if not x else x for x in row_list]
            # print(f"--- {row_list}")

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
                    f"Something went wrong during inserting data into: {table_name}")
                logging.info(e)
            except InFailedSqlTransaction as e:
                logging.error(
                    f"Transaction stopped. Rolling back...: {table_name}")
                logging.info(e)
                # conn.rollback()
                pass

    def insert_data2(self, conn, cur, df: pd.DataFrame, table_name: str) -> None:
        """
        Inserts the row into the database specified by conn, cur. The {table_name} specifies the table to be inserted into.
        """
        try:
            columns = self.tables[table_name]["headers"]
        except KeyError as e:
            logging.error(
                f"You forgot to specify the 'headers' on table {table_name}.")
            raise e

        # Check if the element is a nested list and extract it.
        if isinstance(columns[0], list):
            for i, more_columns in enumerate(columns):
                #print("These are the headers:", more_columns)
                new_df = df[more_columns]

                if self.tables[table_name]["uniques"]:
                    uniq = self.tables[table_name]["uniques"][i]
                    #print("The unique value is:", uniq)

                    # new_df = new_df[new_df[self.tables[table_name]
                    #               ["uniques"][i]].notnull()]
                    new_df = new_df[new_df[self.tables[table_name]
                                           ["uniques"][i]] != ""]
                    new_df = new_df.drop_duplicates(subset=[self.tables[table_name]
                                                            ["uniques"][i]])
                    # print("This is the new data:\n", new_df[self.tables[table_name]
                    #               ["uniques"][i]])

                s_buf = io.StringIO()  # Create string buffer
                # Export data to csv
                new_df.to_csv(s_buf, header=False, index=False,
                              sep="\t", quoting=csv.QUOTE_MINIMAL)
                s_buf.seek(0)  # Reset read head to start of buffer
                try:
                    cur.copy_from(s_buf, table_name, sep="\t", null="",
                                  columns=self.tables[table_name]["attributes"])
                except Exception as e:
                    logging.error(e)
                    return

        # Columns isn't a nested list, just use it.
        else:
            new_df = df[columns]

            if self.tables[table_name]["uniques"]:
                uniq = self.tables[table_name]["uniques"][0]
                # print(uniq)

                new_df = new_df[new_df[self.tables[table_name]
                                       ["uniques"][0]] != ""]
                new_df = new_df.drop_duplicates(subset=[self.tables[table_name]
                                                        ["uniques"][0]])
                # print(new_df[self.tables[table_name]
                #               ["uniques"][0]])

            s_buf = io.StringIO()  # Create string buffer
            # Export data to csv
            new_df.to_csv(s_buf, header=False, index=False,
                          sep="\t", quoting=csv.QUOTE_MINIMAL)
            s_buf.seek(0)  # Reset read head to start of buffer
            try:
                cur.copy_from(s_buf, table_name, sep="\t", null="",
                              columns=self.tables[table_name]["attributes"])
            except Exception as e:
                logging.error(e)
                return

    def insert_wrapper(self, file_path, headers: List[str], seperator: str = ",", table_names: List[str] = None) -> None:
        table_names = table_names if table_names else self.table_names

        num_rows = bufcount(file_path)
        conn, cur = self.connect_database(autocommit=False)

        for index, row in tqdm(self.read_csv(file_path, seperator=seperator, headers=headers, limit=None).iterrows(), desc=f"Inserting {file_path} ...", total=num_rows, mininterval=5.0, miniters=1000):
            for table_name in table_names:
                self.insert_data(conn, cur, row, table_name)

        cur.close()
        conn.commit()
        conn.close()

    def insert_wrapper2(self, file_path, headers: List[str], seperator: str = ",", table_names: List[str] = None) -> None:
        table_names = table_names if table_names else self.table_names

        num_rows = bufcount(file_path)
        conn, cur = self.connect_database(autocommit=False)

        df = self.read_csv(file_path, seperator=seperator,
                           headers=headers, limit=None)

        for table_name in table_names:
            logging.info("The current table is:", table_name)
            self.insert_data2(conn, cur, df, table_name)

        cur.close()
        conn.commit()
        conn.close()


if __name__ == "__main__":
    print(f"No module code in {__name__}.")
