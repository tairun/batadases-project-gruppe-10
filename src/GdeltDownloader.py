import os
import re
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from hashlib import md5
from pprint import pprint
from tempfile import TemporaryDirectory
from typing import Dict, List, Tuple
from itertools import repeat

import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm


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

        for link in tqdm(links, desc="Parsing links..."):
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
                #print("Link was empty!", e)
                continue  # Skip link if one of the properties was not found

        print(f"{len(file_links)} links found!")
        return file_links

    def download_file(self, file_obj: Dict, dl_path: str) -> Tuple[str, bool]:
        local_filename = file_obj['file'].split('/')[-1]
        #print(f"Downloading: {file_obj}")
        with requests.get(f"{self.base_url}{file_obj['file']}", stream=True) as r:
            # with self.dl_dir as tempdir:
            with open(os.path.join(dl_path, local_filename), 'wb') as f:
                shutil.copyfileobj(r.raw, f)
                is_md5_equal = (
                    self.md5sum(os.path.join(dl_path, local_filename)) == file_obj['md5'])

        return local_filename, is_md5_equal

    def download_links(self, files: List[Dict], dl_path: str, thread_count: int = 1) -> Tuple[str, bool]:
        print(self.dl_dir)
        _ = input()
        executor = ThreadPoolExecutor(max_workers=thread_count)
        # with self.dl_dir as temp:
        results = executor.map(self.download_file, files, repeat(dl_path))

        return results

    def md5sum(self, fname: str):
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5().update(chunk)
        return md5().hexdigest()

    def unzip(self, fname: str):
        pass


if __name__ == "__main__":
    pass
