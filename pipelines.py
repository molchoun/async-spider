import itertools
import operator
from io import StringIO

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine

from config import config
from utils.log import get_logger
import pandas as pd
import numpy as np
from db import DB


class DataCleaningPipeline:
    name = "CleaningPipeline"
    logger = get_logger(name)
    df = pd.DataFrame()

    def dropna_cols(self, df):
        df = df.dropna(thresh=int(df.shape[0] * 0.2), axis=1)
        return df

    def clean_currency(self, x):
        """ If the value is a string, then remove currency symbol and delimiters
        otherwise, the value is numeric and can be converted
        """
        # df_merged['Price'] = df_merged['Price'].replace({'\$': '', '֏': '', '₽': '', '€': '', ',': ''}, regex=True)
        if isinstance(x, str):
            return x.replace('$', '').replace('֏', '').replace('₽', '').replace('€', '').replace(',', '')

    def split_price_col(self, df):
        df[['price', 'duration']] = df.price.str.split(expand=True)
        df['price'] = df.loc[:, 'price'].fillna(0).astype(int)
        cols = df.columns.to_list()
        idx_currency = cols.index('currency')
        idx_duration = cols.index('duration')
        cols.insert(idx_currency + 1, 'duration')
        cols.pop()
        df = df[cols]
        return df

    def clean(self, df: pd.DataFrame):
        df_cols = df.columns
        df['date_posted'] = pd.to_datetime(df['date_posted'], dayfirst=True, format='%b-%d-%Y')
        for col in ['floor_area', 'land_area', 'room_area']:
            if col in df_cols:
                df[col] = df[col].str.extract('(\d+)').fillna(0).astype(int)
                # df[col] = df[col].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
        for col in ['floors_in_the_building', 'number_of_rooms', 'number_of_bathrooms', 'floor']:
            if col in df_cols:
                df[col] = df[col].replace({'\+': ''}, regex=True).fillna(0).astype(int)
        for col in ['children_are_welcome', 'pets_allowed']:
            if col in df_cols:
                df[col] = df[col].replace({'No': 10, 'Yes': 11, 'Negotiable': 12}, regex=True).fillna(0).astype(int)
        if 'ceiling_height' in df_cols:
            df['ceiling_height'] = df['ceiling_height'].fillna(0)
            df['ceiling_height'] = df['ceiling_height'].str.extract('(\d+(?:\.\d+)?)').astype(float)
            # df['ceiling_height'] = df['ceiling_height'].astype('str').str.extractall('(\d+(?:\.\d+)?)').unstack().fillna('').sum(axis=1).astype(float)
        if 'utility_payments' in df_cols:
            df['utility_payments'] = df['utility_payments'].replace(
                {'Not included': 10, 'Included': 11, 'By Agreement': 12}, regex=True).fillna(0).astype(int)
        if 'new_construction' in df_cols:
            df['new_construction'] = pd.Series(np.where(df['new_construction'].values == 'Yes', 1, 0), df.index,
                                               dtype=int)
        if 'elevator' in df_cols:
            df['elevator'] = pd.Series(np.where(df['elevator'].values == 'Available', 1, 0), df.index, dtype=int)
        return df

    def clean_df(self):
        self.df = self.dropna_cols(self.df)
        self.df["price"] = self.df['price'].apply(self.clean_currency)
        if self.df['price'].str.contains('daily').any() or self.df['price'].str.contains('monthly').any():
            self.df = self.split_price_col(self.df)
        self.df = self.clean(self.df)
        return self.df


class PostgresPipeline(DB):
    name = "PostgresPipeline"
    logger = get_logger(name)

    def __init__(self):
        # self.conn, self.cur = DB.connect()
        # self.conn.autocommit = False
        # self.engine = create_engine('postgresql+psycopg2://postgres:SecurePas$1@localhost/testdb')
        super().__init__()

    def process_urls(self, urls, cat_id=None, reg_id=None):
        create_table = ('''
                        CREATE TABLE IF NOT EXISTS urls
                          (id SERIAL PRIMARY KEY,
                          url VARCHAR(255) UNIQUE,
                          cat_id INT, 
                          reg_id INT,
                          retrieved INTEGER DEFAULT 0,
                          created_at TIMESTAMP DEFAULT now(),
                          CONSTRAINT fk_category 
                          FOREIGN KEY(category_id)
                          REFERENCES property_type(id),
                          CONSTRAINT fk_region
                          FOREIGN KEY(region_id)
                          REFERENCES regions(id));
                        ''')
        self.cur.execute(create_table)
        # tuples = ((url, cat_id, reg_id) for url in urls)
        tuples = [tuple(x) for x in urls.to_numpy()]
        cols = ','.join(list(urls.columns))
        query = f"""
            WITH t as (
                INSERT INTO urls(%s)
                VALUES %%s
                ON CONFLICT (url)
                DO UPDATE
                SET
                cat_id = EXCLUDED.cat_id,
                reg_id = EXCLUDED.reg_id,
                retrieved = 0
                RETURNING xmax
            )
            SELECT 
                SUM(CASE WHEN xmax = 0 THEN 1 ELSE 0 END) AS ins, 
                SUM(CASE WHEN xmax::text::int > 0 THEN 1 ELSE 0 END) AS upd 
            FROM t;""" % cols

        psycopg2.extras.execute_values(self.cur, query, tuples, page_size=len(urls))

        try:
            ins_count, upd_count = self.cur.fetchone()
            if ins_count or upd_count:
                self.logger.info("Inserted: %d, Updated: %d to the table urls.", ins_count, upd_count)
            else:
                self.logger.info("No new records were inserted.")
        except psycopg2.ProgrammingError as e:
            self.logger.info(str(e))

    def process_items(self, df, table_name):
        if not self.table_exists(table_name):
            self.construct_and_create_table(table_name, df.columns)
        try:
            self.cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' and table_name = %s;
            """, (table_name, ))
            columns_db = self.cur.fetchall()
            columns_to_insert = []
            for col in columns_db:
                if col[0] in df.columns:
                    columns_to_insert.append(col[0])

            tuples = [tuple(x) for x in df[columns_to_insert].to_numpy()]
            cols = ','.join(list(columns_to_insert))
            query = """
            WITH t as 
              (
                INSERT INTO %s(%s) VALUES %%s
                ON CONFLICT (id)
                DO NOTHING
                RETURNING xmax
              )
            SELECT 
                SUM(CASE WHEN xmax = 0 THEN 1 ELSE 0 END) AS ins 
            FROM t;
            """ % (table_name, cols)
            psycopg2.extras.execute_values(self.cur, query, tuples, page_size=len(df))
            ins_count = self.cur.fetchone()[0]
            if ins_count:
                self.logger.info("Inserted records: {}, table: {}".format(ins_count, table_name))
            else:
                self.logger.info("No new records were inserted.")
        except Exception as e:
            self.logger.info("Error: %", e)
            raise Exception

    def copy_from_stringio(self, df, table):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        df.to_csv(buffer, index_label='id', header=False)
        buffer.seek(0)

        try:
            self.cur.copy_from(buffer, table, sep=",")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            return 1

    def select_not_retrieved_urls(self, cat_id=None, reg_id=None):
        def group_by_cat_name(lst):
            res = {}
            for item in lst:
                res.setdefault(item['cat_name'], []).append(item)
            return res
        self.cur.execute("""
            SELECT cat_id, reg_id, p.name, array_agg(url), ARRAY_TO_STRING(ARRAY_AGG(DISTINCT(r.name)), ',')
            FROM urls u
            INNER JOIN property_type p
            ON u.cat_id = p.id
            INNER JOIN regions r
            ON u.reg_id = r.id
            WHERE retrieved = 0
            GROUP BY cat_id, reg_id, p.name
            ORDER BY cat_id, reg_id;
        """, (cat_id, reg_id))
        urls = [
            {
                'cat_id': rec[0],
                'reg_id': rec[1],
                'cat_name': rec[2],
                'urls': rec[3],
                'reg_name': rec[4]
            } for rec in self.cur.fetchall()]
        urls = group_by_cat_name(urls)

        return urls

    def url_set_as_retrieved(self):
        self.cur.execute(f'UPDATE urls SET retrieved = 1 WHERE created_at >= CURRENT_DATE;')

    def delete_new_urls(self, urls):
        self.cur.execute(f"""
        DELETE FROM urls WHERE url in {urls}
        """)
