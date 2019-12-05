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
logging.basicConfig(level=logging.INFO)


class GdeltDownloader(object):

    def __init__(self, url: str, start_date: Tuple[int, ...], end_date: Tuple[int, ...]):
        now = datetime.now()
        start_date if start_date else (1970, 1, 1)
        end_date if end_date else (now.year, now.month, now.day)

        self.base_url: str = url
        self.start_date = datetime(*start_date)
        self.end_date = datetime(*end_date)
        self.max_threads = 2 * os.cpu_count() - 1
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
        # print(response.content)

        soup = bs(response.text, features="lxml")

        links = soup.find_all('li')
        # print(links)

        for link in tqdm(links, desc="Parsing links ..."):
            try:
                file = link.find('a')['href']
                date_string = tuple(int(x)
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
                # print("Link was empty!", e)
                continue  # Skip link if one of the properties was not found

        print(f"{len(file_links)} links found!")
        return file_links

    def download_file(self, file_obj: Dict, dl_path: str, extract: bool = True, remove: bool = True) -> Optional[Tuple[str, bool]]:
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
        local_filename = file_obj['file'].split('/')[-1]
        file_path = os.path.join(dl_path, local_filename)

        result = None

        logging.info(f"{file_path}, exists: {os.path.isfile(file_path)}")

        if not os.path.isfile(file_path):
            with requests.get(file_path, stream=True) as r:
                with open(file_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
                    is_md5_equal = (
                        self.md5sum(file_path) == file_obj['md5'])

            result = (local_filename, is_md5_equal)
        else:
            print(f"Skipping download for: {file_path}")

        if extract:
            self.unzip(file_path, dl_path)

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

        logging.info(files[0:2])
        _ = input("Press any key to start download and extraction process ...")
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
        with ZipFile(file_path, mode="r") as zipobj:
            zipobj.extractall(path=extract_path)

        if remove:
            os.remove(file_path)  # Remove file after done unzipping


if __name__ == "__main__":
    pass
