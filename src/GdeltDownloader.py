import re
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from hashlib import md5
from os import cpu_count, path
from pprint import pprint
from tempfile import TemporaryDirectory
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm


class GdeltDownloader():

    def __init__(self, url: str, start_date: Tuple[int], end_date: Tuple[int]):
        now = datetime.now()
        start_date if start_date else (1970, 1, 1)
        end_date if end_date else (now.year, now.month, now.day)

        self.base_url: str = url
        self.start_date = datetime(*start_date)
        self.end_date = datetime(*end_date)
        self.max_threads = 2 * cpu_count() - 1
        self.dl_dir = TemporaryDirectory(prefix="batadase_")

    def get_file_links(self, url: str, start_date: datetime, end_date: datetime) -> List[Dict]:
        '''
        :param start_date Specify which files to download
        :param end_date
        '''

        dateRegex = r"(\d{4})(\d{2})(\d{2})"
        zipRegex = r""  # using href attribute
        sizeRegex = r"(\d+\.\d)MB"
        md5regex = r"([a-fA-F\d]{32})"

        file_links = []
        response = requests.get(url)
        # print(response.content)

        soup = bs(response.text, features="lxml")

        links = soup.find_all('li')
        # print(links)

        for link in tqdm(links, desc="Parsing links..."):
            try:
                file = link.find('a')['href']
                date_string = list(int(x)
                                   for x in re.findall(dateRegex, file)[0])
                date = datetime(*date_string)

                # Only add file link if the date is withing the range
                if date <= end_date and date >= start_date:
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

    def download_file(self, file_obj: Dict, dir: TemporaryDirectory) -> Tuple[str, bool]:
        print(dir)
        local_filename = file_obj['file'].split('/')[-1]
        with requests.get(f"{base_url}/{file_obj['file']}", stream=True) as r:
            with dir as tempdir:
                with open(path.join(tempdir, local_filename), 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
                    is_md5_equal = (
                        md5sum(path.join(tempdir, local_filename)) == file_obj['md5'])

        return local_filename, is_md5_equal

    def download_links(self, files: List[Dict], dir, thread_count: int = 1) -> None:
        executor = ThreadPoolExecutor(max_workers=thread_count)
        with dir as temp:
            executor.map(download_file, files, dir)

    def md5sum(self, fname: str):
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5().update(chunk)
        return md5().hexdigest()


if __name__ == "__main__":
    all_links = get_file_links(start_date=start_date, end_date=end_date)
    download_file(all_links[0], dl_dir)
    lala = input()
