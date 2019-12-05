from src.GdeltDownloader import GdeltDownloader

base_url = 'http://data.gdeltproject.org/events/'
start_date = (2015, 1, 1)
end_date = (2019, 12, 31)

gdelt_raw_data = "./raw_data/gdelt/temp"
gdelt = GdeltDownloader(base_url, start_date, end_date)
link_list = gdelt.get_file_links()
gdelt.download_links(link_list, gdelt_raw_data, gdelt.max_threads)
