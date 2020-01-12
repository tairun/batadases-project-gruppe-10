#!/usr/bin/env python3

from src.utils import *
from typing import Dict, List, Tuple, Optional, Union, Iterator
from tqdm import tqdm
from bs4 import BeautifulSoup as bs
import requests
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
from datetime import datetime
from itertools import repeat
from zipfile import ZipFile
from pprint import pprint
from hashlib import md5
import os
import re
import sys
import shutil
import logging


class GdeltDownloader(object):
    """
    Class description.
    """

    def __init__(self, start_date: Tuple[int, int, int], end_date: Tuple[int, int, int], url: str = "http://data.gdeltproject.org/events/", dl_path: str = "./data/gedlt"):
        super().__init__()
        now = datetime.now()
        start_date if start_date else (1970, 1, 1)
        end_date if end_date else (now.year, now.month, now.day)

        self.base_url = url
        self.dl_path = dl_path
        self.start_date = datetime(*start_date)
        self.end_date = datetime(*end_date)
        self.max_workers = int(os.cpu_count()) * 2
        self.dl_dir = TemporaryDirectory(prefix="batadase_", dir=".")

    def get_file_links(self) -> List[Dict]:
        '''
        Gets all links from the Gdelt website to the zip files with the data.
        '''

        dateRegex = r"(\d{4})(\d{2})(\d{2})"
        zipRegex = r""  # using href attribute
        sizeRegex = r"(\d+\.\d)MB"
        md5regex = r"([a-fA-F\d]{32})"

        file_links = []
        response = requests.get(self.base_url)

        soup = bs(response.text, features="lxml")

        links = soup.find_all('li')

        for link in tqdm(links, desc="Parsing links ..."):
            try:
                file = link.find('a')['href']
                date_string: Tuple[int, int, int] = tuple(int(x)
                                                          for x in re.findall(dateRegex, file)[0])
                date = datetime(*date_string)

                # Only add file link if the date is withing the range
                if date <= self.end_date and date >= self.start_date:
                    link_obj = {
                        "file": file,
                        "day": date,
                        "md5": re.findall(md5regex, link.text)[0],
                        "size": float(re.findall(sizeRegex, link.text)[0])
                    }
                    file_links.append(link_obj)

            except Exception as e:
                logging.info(f"Link '{link}' was empty!", e)
                continue  # Skip link if one of the properties was not found

        print(
            f"{len(file_links)} links found in date range '{self.start_date} - {self.end_date}'!")
        return file_links

    # TODO: Change return type to a dict.
    def download_file(self, file_obj: Dict, dl_path: str, extract: bool = True, remove: bool = True, retries: int = 5) -> Optional[Tuple[str, bool]]:
        """Downloads and extract a given file. Skips the file if it already exists.

        Parameters
        ----------
        file_obj : Dict
            Information about file to download.
        dl_path : str
            Path to download file to.

        Returns
        -------
        Optional[Tuple[str, bool]]
            Return a tuple with information about the download. First item is the local filename, second item is a bool which is set to true if the md5 sum of the file matches. Can be none if the download was skipped.
        """
        filename = file_obj['file'].split(
            '/')[-1]  # Get filename from file_obj for later use.
        # Compose relative file path. This is where the file gets saved.
        zip_local_path = os.path.join(dl_path, filename)

        result = None

        try:
            size = os.path.getsize(zip_local_path)
            if size == 0:
                os.remove(zip_local_path)
        except os.error as e:
            logging.debug(f"File '{zip_local_path}' does not exist.")

        # Skip download if .zip or .csv file already exists.
        is_md5_equal = False
        if not os.path.isfile(zip_local_path) and not os.path.isfile(os.path.splitext(zip_local_path)[0]):
            uri = os.path.join(self.base_url, filename)

            with requests.get(uri, stream=True) as r:
                with open(zip_local_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
                    is_md5_equal = (
                        self.md5sum(zip_local_path) == file_obj['md5'])

            if not is_md5_equal and retries > 0:
                logging.info(
                    f"MD5 mismtach. Retrying download for file '{filename}'. {retries} attempts left.")
                return self.download_file(file_obj, dl_path, extract, remove, retries=retries-1)

            csv_local_path = os.path.join(
                dl_path, os.path.splitext(filename)[0])
            result = (zip_local_path, is_md5_equal)
        else:
            logging.debug(f"Skipping download for: {zip_local_path}.")

        if extract and not os.path.isfile(os.path.splitext(zip_local_path)[0]):
            self.unzip(zip_local_path, dl_path)
            csv_local_path = os.path.join(
                dl_path, os.path.splitext(filename)[0])
            result = (csv_local_path, is_md5_equal)
        else:
            logging.debug(f"Skipping extraction of {zip_local_path}.")

        return result

    def download_links(self, files: List[Dict], dl_path: str, thread_count: int = 1) -> Iterator[Optional[Tuple[str, bool]]]:
        """Takes a dict and dispatches the download links to different threads to download.

        Parameters
        ----------
        files : List[Dict]
            Contains the files to download, along with more information about the files (md5, size and date)
        dl_path : str
            Path to download files to.
        thread_count : int, optional
            Number of threads, by default 1

        Returns
        -------
        Iterator[Optional[Tuple[str, bool]]]
            Iterator object with download results.
        """

        _ = input("Press 'Enter' to start download and extraction process ...")
        executor = ThreadPoolExecutor(max_workers=thread_count)
        results = tqdm(executor.map(
            self.download_file, files, repeat(dl_path)), desc="Queuing downloads ...")

        return results

    def md5sum(self, file_path: str) -> str:
        """Calculate md5 sum of given file.

        Parameters
        ----------
        file_path : str
            Full path of file.

        Returns
        -------
        str
            md5 hexdigest
        """
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5().update(chunk)

        return md5().hexdigest()

    def unzip(self, file_path: str, extract_path: str, remove: bool = True) -> None:
        """Unzip a file to the specified path and delete the file after.

        Parameters
        ----------
        file_path : str
            Full path of file to be extracted.
        extract_path : str
            Destination path for extracted file.
        """
        logging.debug(f"Unpacking file '{file_path}' to '{extract_path}'")

        with ZipFile(file_path, mode="r") as zipobj:
            zipobj.extractall(path=extract_path)

        if remove:
            os.remove(file_path)  # Remove file after unzipping is done.


if __name__ == "__main__":
    pass
