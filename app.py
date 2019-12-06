from src.GdeltDownloader import GdeltDownloader


# Download the GDELTv1.0 dataset from their website
# ---
# Define the URI, start and end date for the downloader.
base_url = 'http://data.gdeltproject.org/events/'
start_date = (2015, 1, 1)
end_date = (2019, 12, 31)

gdelt_raw_data = "./raw_data/gdelt/temp"  # Specify the download directory
# Create the downloader object
gdelt = GdeltDownloader(base_url, start_date, end_date)
# Parse the download page and extract all the links
link_list = gdelt.get_file_links()
# Download the individual files and extract the content from the zip archives
gdelt.download_links(link_list, gdelt_raw_data, gdelt.max_threads)
