import BeautifulSoup4 as bs
import urllib2
import re


gdelt_url = 'http://data.gdeltproject.org/events/index.html'
date_range = [(2015), (2019)]
threads = 4


def get_links(date_range):
    html_page = urllib2.urlopen("https://arstechnica.com")

    soup = bs(html_page)
    for link in soup.findAll('a', attrs={'href': re.compile("^http://")}):
        print(link.get('href'))

get_links()