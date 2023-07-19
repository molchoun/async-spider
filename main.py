import hashlib
import time
import urllib
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import re

from bs4 import BeautifulSoup

from spider import Spider
from db import DB
from pipelines import PostgresPipeline, DataCleaningPipeline
from utils.log import get_logger
from utils.utils import construct_urls


logger = get_logger("ETL")


def extract_urls():
    with DB() as db:
        categories = db.select_categories()
        regions = db.select_regions()

    urls_list = construct_urls(categories, regions)

    with Spider() as spider:
        df_urls = pd.DataFrame(columns=['cat_id', 'reg_id', 'url'])

        async def parse_urls(response, params):
            item_base_url = "https://www.list.am/en/item/"
            page_urls = set()
            cat_id = params['cat_id']
            reg_id = params['reg_id']
            nonlocal df_urls
            html = await response.text()
            if html:
                for link in tqdm(re.compile(r'href="/en/item/(.*?)"').findall(html), ncols=100, desc=f"Links found on page - {params['cat_name']} in {params['reg_name']}"):
                    try:
                        abslink = urllib.parse.urljoin(item_base_url, link)
                        page_urls.add(abslink)
                    except (urllib.error.URLError, ValueError):
                        logger.exception("Error parsing URL: %s", link)
            df = pd.DataFrame(columns=['url'], data=page_urls)
            df['cat_id'] = cat_id
            df['reg_id'] = reg_id
            df_urls = pd.concat([df_urls, df])
            logger.info("Found %d links on the page", len(page_urls), )
        spider.start(urls_list[1:3], parse_urls)

    df_urls = df_urls.drop_duplicates(subset='url', keep='first')
    print(f"Links found in total: {len(df_urls)}")

    load_urls_todb(df_urls)
    return df_urls


def load_urls_todb(df):
    postgres_pl = PostgresPipeline()
    postgres_pl.process_urls(df)



def extract_items():
    postgres_pl = PostgresPipeline()
    urls_list = postgres_pl.select_not_retrieved_urls()
    with Spider() as spider:
        df = pd.DataFrame()
        df_dict = {}

        async def parse_item(response, params):
            nonlocal df
            cols = ['description', 'prepayment', 'number_of_guests', 'lease_type', 'minimum_rental_period',
                    'noise_after_hours', 'mortgage_is_possible', 'handover_date', 'places_nearby']
            html = await response.text(encoding="utf-8")
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

                if actual_cat_name != params["cat_name"]:
                    data_dict["cat_id"] = params["cat_id"]
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
                data_dict['reg_id'] = params["reg_id"]
                data_dict['cat_name'] = params['cat_name']
                try:
                    id_string = str(price) + str(soup.select_one("#uinfo a")['href']) + str(address)
                    id_hash = hashlib.sha256(id_string.encode('utf-8')).hexdigest()
                    data_dict['id'] = id_hash
                except Exception as e:
                    price = 0

                df = pd.concat([df, pd.DataFrame.from_records([data_dict])])
                logger.info(f"dataframe is {len(df)}")

        for key in urls_list:
            spider.start(urls_list[key], parse_item)
            df_dict[key] = df
            df = pd.DataFrame()

    df_dict = clean_dataframe(df_dict)
    load_items_todb(df_dict)
    return df_dict


def clean_dataframe(df_dict):
    cleaning_pl = DataCleaningPipeline()
    for key in tqdm(df_dict):
        cleaning_pl.df = df_dict[key]
        df_dict[key] = cleaning_pl.clean_df()
    return df_dict


def load_items_todb(df_dict):
    postgres_pl = PostgresPipeline()
    for key in df_dict:
        postgres_pl.process_items(df_dict[key], key)
        postgres_pl.url_set_as_retrieved()


def etl():
    # with DB() as db:
    #     categories = db.select_categories()
    #     regions = db.select_regions()
    #
    # urls_list = construct_urls(categories, regions)

    t1_before = time.perf_counter()
    extract_urls()
    extract_items()
    # df = extract_urls(urls_list[1:8])
    t1 = time.perf_counter() - t1_before
    print(t1)
    # urls_list = load_urls_todb(df)
    # df_dict = extract_items(urls_list)
    # df_dict = clean_dataframe(df_dict)
    #
    # load_items_todb(df_dict)

etl()

def main():
    logger = get_logger("Main")
    postgres_pl = PostgresPipeline()
    cleaning_pl = DataCleaningPipeline()

    with DB() as db:
        categories = db.select_categories()
        regions = db.select_regions()

    urls_list = construct_urls(categories, regions)

    with Spider() as spider:
        t2_before = time.perf_counter()
        for urls in urls_list[1:8]:
            spider.start_spider(task="parse_urls", urls=urls["urls"], cat_id=urls['cat_id'], reg_id=urls['reg_id'])
            urls_to_insert = tuple(spider.urls)
            # postgres_pl.process_urls(urls_to_insert, urls["cat_id"], urls["reg_id"])
            # urls_to_parse = postgres_pl.select_not_retrieved_urls(urls["cat_id"], urls["reg_id"])
            # if urls_to_parse:
            #     spider.start_spider("parse_item", urls_to_parse, cat_name=urls["cat_name"], cat_id=urls["cat_id"], reg_id=urls["reg_id"])
            #     cleaning_pl.df = spider.df
            #     df = cleaning_pl.clean_df()
            #     postgres_pl.process_items(df, urls['cat_name'])
            #     postgres_pl.url_set_as_retrieved()
            #
            # spider.urls = set()
            # spider.df = pd.DataFrame()

        t2 = time.perf_counter() - t2_before
        print(t2)
# if __name__ == '__main__':
#     main()
