#!/usr/bin/env python3

import os
from tqdm import tqdm
import logging
import psycopg2
import pandas as pd
from getpass import getpass
from psycopg2.errors import UniqueViolation

from src.utils import *

from typing import Tuple, Generator, List


class DataIntegrator(object):
    """
    Base class for data integrator classes. Provides database connection and file reader functionality.
    """

    def __init__(self):
        super().__init__()
        self.table_script = "./schema/prepare_database.psql"
        self.table_names: List[str] = []
        self.tables: Dict = {}

    def read_csv(self, file_path: str, headers: List[str] = None, limit: int = None, seperator: str = ",") -> Generator:
        """
        Generator that yields lines of a file.
        """
        with open(file_path, mode="r", encoding="utf8") as f:
            if limit:
                data = pd.read_csv(f, nrows=limit, sep=seperator,
                                   names=headers, encoding="utf8", na_filter=False, na_values=":", thousands=" ")
            else:
                data = pd.read_csv(
                    f, sep=seperator, names=headers, encoding="utf8", na_filter=False, na_values=":", thousands=" ")

            for index, row in data.iterrows():
                yield row

    def connect_database(self, host: str = "localhost", port: int = 5432, dbname: str = None, user: str = None, passwd: str = None, autocommit: bool = False) -> Tuple:
        try:
            dbname = dbname if dbname else os.environ["POSTGRES_DB"]
            user = user if user else os.environ["POSTGRES_USER"]
            passwd = passwd if passwd else os.environ["POSTGRES_PASSOWRD"]
            print(f"Password: {passwd}")
        except KeyError as e:
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
            print(f"Connected to: {cur.fetchone()}")

            return conn, cur
        except Exception as e:
            logging.error("Could not connect to database!")
            conn.close()
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
                #print(f"--- {row_list}")

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
            #print(f"--- {row_list}")

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

    def insert_wrapper(self, file_path, headers: List[str], seperator: str = ",", table_names: List[str] = None) -> None:
        table_names = table_names if table_names else self.table_names

        num_rows = bufcount(file_path)
        conn, cur = self.connect_database(autocommit=True)

        for row in tqdm(self.read_csv(file_path, seperator=seperator, headers=headers, limit=None), desc="Inserting rows into table ...", total=num_rows, mininterval=5.0, miniters=1000):

            for table_name in table_names:
                self.insert_data(conn, cur, row, table_name)

        cur.close()
        conn.close()


if __name__ == "__main__":
    print(f"No module code in {__name__}.")
