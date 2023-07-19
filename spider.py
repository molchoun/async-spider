import hashlib
import re
import urllib
import traceback
from urllib.error import HTTPError
from utils.log import get_logger
from bs4 import BeautifulSoup
import asyncio
import aiohttp
from datetime import datetime
import pandas as pd
import time
from collections import namedtuple
from itertools import zip_longest

URL_HOME = 'https://www.list.am/en'

DEFAULT_HEADER = {
    'User-Agent':
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}


_Request = namedtuple(
    "Request", ["method", "url", "header", "data", "callback", "params"])


def Request(method, url, header=DEFAULT_HEADER, data=None, callback=None, params=None):
    return _Request(method=method, url=url, header=header, data=data, callback=callback, params=params)


class Spider:
    item_base_url = "https://www.list.am/en/item/"
    name = "ListSpider"
    urls = set()
    next_page_urls = []
    logger = get_logger(name)
    df = pd.DataFrame()
    df_urls = pd.DataFrame(columns=['cat_id', 'reg_id', 'url'])

    def __init__(
            self,
            conn=aiohttp.TCPConnector(limit_per_host=100, limit=0, ttl_dns_cache=300),
            session=aiohttp.ClientSession,
            loop=asyncio.new_event_loop(),
            concurrent_requests=250
    ):
        self.total_timeout = aiohttp.ClientTimeout(total=60 * 60 * 24)
        # self.conn = conn
        self.loop = loop
        if self.loop.is_closed() or not isinstance(self.loop, asyncio.BaseEventLoop):
            self.loop = asyncio.new_event_loop()

        asyncio.set_event_loop(self.loop)
        self.session = session(loop=self.loop, timeout=self.total_timeout)
        self.pending = asyncio.Queue()
        self.visited = set()
        self.active = []
        self.concurrent_requests = concurrent_requests

    def __enter__(self):
        return self

    async def fetch_html(self, url, **kwargs):
        headers = {
            'User-Agent':
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }
        try:
            async with self.session.get(url, allow_redirects=False, headers=headers, **kwargs) as response:
                html = await response.text(encoding="utf-8")
        except (aiohttp.ClientError, aiohttp.http.HttpProcessingError) as e:
            self.logger.error(
                "aiohttp exception for %s [%s]: %s",
                url,
                getattr(e, "status", None),
                getattr(e, "strerror", None),
            )
        except Exception as e:
            self.logger.exception(
                "Non-aiohttp exception occured:  %s",
                getattr(e, "__dict__", {})
            )
        else:
            if response.status >= 300:
                self.logger.info("Nothing to fetch")
                return
            self.logger.info("Got response [%s] for URL: %s", response.status, url)
            return html

    def fetch_next_page_urls(self, html):
        next_page_element = re.compile(r'href="([^"]*)">Next >').search(html)
        if next_page_element:
            next_page_url = next_page_element.group(1)
            url = URL_HOME + next_page_url
            self.next_page_urls.append(url)

    async def aget_all_regions(self, url):
        """
        Returns all region names and query string in a dictionary.
        e.g. {'Yerevan': '?n=1', 'Armavir': '?n=23', ...}
        """
        # url to fetch all region paths
        html = await self.fetch_html(url)
        soup = BeautifulSoup(html, "html.parser")

        # select all `divs` that contain region names
        data_searchname = soup.find_all('div', {'class': 'i', 'data-name': re.compile('^[A-z]')})  # data-name - to
        # filter out city names
        loc_dict = dict()
        for data in data_searchname[1:len(data_searchname) - 1]:  # slicing to exclude option 'All'
            if data['data-name'] not in loc_dict:
                loc_dict[data['data-name']] = '?n=' + data['data-value']
        return loc_dict

    async def aget_all_categories(self, url):
        """Returns all categories' names and paths in a dictionary.
        e.g. {'Apartments for sale': '/category/60', 'Houses for rent: '/category/63', ...} """
        # arbitrary category url to fetch all categories paths
        html = await self.fetch_html(url)
        soup = BeautifulSoup(html, 'html.parser')
        section_cat = soup.select('div.s')
        categories_dict = dict()
        for cat in section_cat:
            tmp = cat.next.lower().strip()
            if tmp == 'for rent':
                for elem in cat.select('a'):
                    categories_dict[elem.text.strip().replace(' ', '_').lower() + '_for_rent'] = elem['href']
            elif tmp == 'for sale':
                for elem in cat.select('a'):
                    categories_dict[elem.text.strip().replace(' ', '_').lower() + '_for_sale'] = elem['href']
            elif tmp == 'new construction':
                for elem in cat.select('a'):
                    categories_dict[elem.text.strip().replace(' ', '_').lower() + '_new_construction'] = elem['href']

        return categories_dict


    async def parse_urls(self, url, **kwargs):
        page_urls = set()
        html = await self.fetch_html(url)
        cat_id = kwargs['cat_id']
        reg_id = kwargs['reg_id']
        if html:
            for link in re.compile(r'href="/en/item/(.*?)"').findall(html):
                try:
                    abslink = urllib.parse.urljoin(self.item_base_url, link)
                except (urllib.error.URLError, ValueError):
                    self.logger.exception("Error parsing URL: %s", link)
                    pass
                else:
                    page_urls.add(abslink)
        self.logger.info("Found %d links on the page", len(page_urls), )
        # self.urls.update(page_urls)
        df = pd.DataFrame(columns=['url'], data=page_urls)
        df['cat_id'] = cat_id
        df['reg_id'] = reg_id
        df_urls = pd.concat([self.df_urls, df])

    async def parse_item(self, url, **kwargs):
        cols = ['description', 'prepayment', 'number_of_guests', 'lease_type', 'minimum_rental_period',
                'noise_after_hours', 'mortgage_is_possible', 'handover_date', 'places_nearby']
        html = await self.fetch_html(url)
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            property_type = soup.select_one('ol li:nth-child(4) span').text
            purchase = soup.select_one('#crumb ol div span').text
            actual_cat_name = '_'.join([property_type, purchase]).lower().replace(' ', '_')

            # titles of apartment descriptive information: e.g. construction type, floor area, number of rooms etc.
            div_title = soup.find_all('div', {'class': 't'})
            div_value = soup.find_all('div', class_='i')  # values of titles
            data_dict = {div_title[i].text.strip().replace(' ', '_').lower(): div_value[i].text
                         for i in range(len(div_title)) if
                         div_title[i].text.strip().replace(' ', '_').lower() not in cols}

            if actual_cat_name != kwargs["cat_name"]:
                data_dict["cat_id"] = kwargs["cat_id"]
            if soup.find('meta', {'itemprop': 'priceCurrency'}) is None:
                currency = 'unknown'
            else:
                currency = soup.find('meta', {'itemprop': 'priceCurrency'})['content']
            if soup.find('div', class_="loc") is None:
                address = ''
            else:
                address = soup.find('div', class_="loc").text
            price = soup.find('span', class_='price')
            datetime_now = datetime.now()
            date_renewed = soup.select_one('.footer span:nth-child(3)')
            date_posted = soup.select_one('span[itemprop="datePosted"]')
            if date_renewed:
                date_str = date_renewed.text.strip().split()[1]
                date_obj = datetime.strptime(date_str, "%d.%m.%Y")
                data_dict['date_posted'] = date_obj
            else:
                date_str = date_posted.text.strip().split()[1]
                date_obj = datetime.strptime(date_str, "%d.%m.%Y")
                data_dict['date_posted'] = date_obj
            data_dict['address'] = address
            data_dict['currency'] = currency
            data_dict['datetime'] = datetime_now
            if price:
                data_dict['price'] = price.text
            else:
                price = 0
                data_dict['price'] = price
            # data_dict['url_id'] = url_id
            data_dict['reg_id'] = kwargs["reg_id"]
            try:
                id_string = str(price) + str(soup.select_one("#uinfo a")['href']) + str(address)
                id_hash = hashlib.sha256(id_string.encode('utf-8')).hexdigest()
                data_dict['id'] = id_hash
            except Exception as e:
                price = 0
                print('')

            self.df = pd.concat([self.df, pd.DataFrame.from_records([data_dict])])
            # url_set_as_retrieved(url_id)

    async def gather_with_concurrency(self, task, urls, n=60, **kwargs):
        from tqdm import tqdm
        tasks = []
        semaphore = asyncio.Semaphore(n)
        async with semaphore:
            for url in urls:
                t = getattr(self, task)
                tasks.append(t(url, **kwargs))
            pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks))
            # val = await asyncio.gather(*tasks)
            val = [await t for t in pbar]
        return val

    def start_spider(self, task, urls, **kwargs):
        result = self.loop.run_until_complete(self.gather_with_concurrency(task=task, urls=urls, **kwargs))
        return result

    def add_request(self, url, callback, method="GET", params=None):
        if url in self.visited:
            return
        self.visited.add(url)
        request = Request(method=method, url=url, callback=callback, params=params)
        self.pending.put_nowait(request)
        self.logger.info("Add url: {} to queue.".format(url))

    def add_requests(self, urls, callbacks):
        for item in urls:
            params = {
                'cat_id': item['cat_id'],
                'reg_id': item['reg_id'],
                'cat_name': item['cat_name'],
                'reg_name': item['reg_name']
            }
            for url in item['urls']:
                self.add_request(url, callbacks, params=params)

    async def request_with_callback(self, request: _Request, callback=None):
        if not callback:
            callback = request.callback
        try:
            async with self.session.request(method=request.method, url=request.url, allow_redirects=False, headers=request.header) as resp:
                if resp.status >= 300:
                    self.logger.info("Request redirected, nothing to fetch")
                    return
                await callback(resp, request.params)
                self.logger.info("Request [{method}] `{url}` finished.(There are still {num})".format(
                    method=request.method, url=request.url, num=self.pending.qsize()))
        except (aiohttp.ClientError, aiohttp.http.HttpProcessingError) as e:
            self.logger.error(
                "aiohttp exception for %s [%s]: %s",
                request.url,
                getattr(e, "status", None),
                getattr(e, "strerror", None),
            )
        except Exception as e:
            self.logger.error(
                "Non-aiohttp exception occured in request [{method}]: `{url}`, request is ignored\n{error}".format(
                    error=traceback.format_exc(), url=request.url, method=request.method)
            )
        else:
            if resp.status >= 300:
                self.logger.info("Nothing to fetch")
                return

    async def load(self):
        import tqdm.asyncio
        try:
            while True:
                request = await self.pending.get()
                self.logger.info("Loading url: {} from queue.".format(request.url))
                await self.request_with_callback(request, request.callback)
                self.pending.task_done()
        except asyncio.CancelledError:
            pass

    async def __start(self):
        for _ in range(self.concurrent_requests):
            self.active.append(asyncio.ensure_future(
                self.load(), loop=self.loop))
        self.logger.info("Waiting for all requests to finish.")
        await self.pending.join()
        self.logger.info("Requests have finished.")

    def start(self, urls, callbacks):
        self.add_requests(urls, callbacks)
        self.logger.info("Spider started.")
        self.loop.run_until_complete(self.__start())
        self.logger.info("All tasks done. Spider starts to shutdown.")

    @staticmethod
    def construct_url(cat_path, reg_query):
        url = [URL_HOME + cat_path + '/' + str(i) + reg_query for i in range(1, 251)]
        return url

    def _cancel(self):
        for task in self.active:
            task.cancel()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cancel()
        if not self.session.closed:
            self.loop.run_until_complete(self.session.close())
        if not self.loop.is_closed():
            self.loop.stop()
            self.loop.run_forever()
            self.loop.close()
        self.logger.info("Spider shutdown.")
