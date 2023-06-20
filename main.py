import pandas as pd

from spider import Spider
from db import DB
from pipelines import PostgresPipeline, DataCleaningPipeline


def main():
    postgres_pl = PostgresPipeline()
    cleaning_pl = DataCleaningPipeline()
    urls_list = []
    with DB() as db:
        categories = db.select_categories()
        regions = db.select_regions()
    with Spider() as spider:
        # For each category pages (Apartments, Houses, Lands, etc.) go over every region (Yerevan, Armavir, Ararat, etc.)
        for cat_name, cat_id, cat_path in categories:
            for reg_name, reg_id, reg_path in regions:
                urls = spider.construct_url(cat_path, reg_path)
                d = {
                    "urls": urls,
                    "cat_name": cat_name,
                    "cat_id": cat_id,
                    "reg_name": reg_name,
                    "reg_id": reg_id
                }
                urls_list.append(d)
        for urls in urls_list:
            spider.start_spider(task="parse_urls", urls=urls["urls"])
            urls_to_insert = tuple(spider.urls)
            postgres_pl.process_urls(urls_to_insert, urls["cat_id"], urls["reg_id"])
            urls_to_parse = postgres_pl.select_not_retrieved_urls(urls["cat_id"], urls["reg_id"])
            if urls_to_parse:
                spider.start_spider("parse_item", urls_to_parse, cat_name=urls["cat_name"], cat_id=urls["cat_id"], reg_id=urls["reg_id"])
                cleaning_pl.df = spider.df
                df = cleaning_pl.clean_df()
                postgres_pl.process_items(df, urls['cat_name'])
                postgres_pl.url_set_as_retrieved()

            spider.urls = set()
            spider.df = pd.DataFrame()


if __name__ == '__main__':
    main()
