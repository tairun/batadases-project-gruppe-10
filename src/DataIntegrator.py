#!/usr/bin/env python3

import os
import pandas as pd
import psycopg2
import logging
from getpass import getpass
from typing import Tuple, Generator, List


class DataIntegrator(object):
    """
    Base class for data integrator classes. Provides database connection and file reader functionality.
    """

    def __init__(self):
        super().__init__()

    def read_csv(self, file_path: str, headers: List[str] = None, limit: int = None) -> Generator:
        """
        Generator that yields lines of a file.
        """
        with open(file_path, mode="r", encoding="utf8") as f:
            if limit:
                data = pd.read_csv(f, nrows=limit, sep="\t",
                                   names=headers, encoding="utf8", na_filter=False)
            else:
                data = pd.read_csv(
                    f, sep="\t", names=headers, encoding="utf8", na_filter=False)

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
        conn, cur = self.connect_database(autocommit=True)

        with open(script_path, "r") as script_handle:
            cur.execute(script_handle.read())

        cur.close()
        conn.close()
