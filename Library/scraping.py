import requests
from bs4 import BeautifulSoup


class Scraper:
    MODIFIER = '?offset={}'
    TARGET = {"class": "css-1179hly"}

    @staticmethod
    def scrape_page(url):
        data = []
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find("table", Scraper.TARGET)
        if table:
            table_body = table.find('tbody')
            for row in table_body.find_all('tr'):
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                data.append([ele for ele in cols if ele])
        return data

    @staticmethod
    def generate_urls(base_url, limit=1):
        urls = []
        iterator_list = [str(x) if x > 0 else '' for x in range(0, limit, 50)]
        for i in iterator_list:
            url = '{}{}'.format(base_url, Scraper.MODIFIER.format(str(i))) if i else base_url
            urls.append(url)
        return urls
