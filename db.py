import asyncio
from typing import Tuple

import psycopg2
from sqlalchemy import create_engine
from io import StringIO
from config import config
from psycopg2 import sql
import psycopg2.extras
from utils.utils import get_from_env
from utils.log import get_logger
import numpy as np
import pandas as pd
from spider import Spider



class DB:
    name = "DB"
    logger = get_logger(name)

    def __init__(self, conn_string: str = None):
        self.conn, self.cur = self.connect(conn_string)
        self.conn.autocommit = True

    def __enter__(self):
        return self

    def table_exists(self, table_name):
        self.cur.execute("select exists"
                         "(select table_name "
                         "from information_schema.tables "
                         "where table_name=%s)", (table_name,))
        _ = self.cur.fetchone()[0]

        return _

    def create_table(self, name, columns):
        # name = "table_name"
        # columns = (("col1", "TEXT"), ("col2", "INTEGER"), ...)
        fields = []
        for col in columns:
            fields.append(sql.SQL("{} {}").format(sql.Identifier(col[0]), sql.SQL(col[1])))

        query = sql.SQL("CREATE TABLE IF NOT EXISTS {tbl_name} ( {fields} );").format(
            tbl_name=sql.Identifier(name),
            fields=sql.SQL(', ').join(fields)
        )
        self.cur.execute(query)
        self.conn.commit()

    def construct_and_create_table(self, table_name, field_names):
        fields = tuple()

        for col in field_names:
            if isinstance(col, int):
                col_type = "INTEGER"
            elif col == 'datetime':
                col_type = "DATE"
            elif col == 'price':
                col_type = "BIGINT"
            elif col == 'id':
                col_type = "CHAR(64) PRIMARY KEY"
            else:
                col_type = "TEXT"
            _t = (col, col_type)
            fields += (_t,)
        self.create_table(table_name, fields)

    def connect(self, conn_string=None):
        """ Connect to the PostgreSQL database server """

        try:
            if conn_string:
                conn = psycopg2.connect(conn_string)
            else:
                conn = psycopg2.connect(get_from_env("POSTGRES_URL"))
            cur = conn.cursor()
            return conn, cur
        except (Exception, psycopg2.DatabaseError) as error:
            # print('Error in transaction, reverting all changes using rollback, error is: ', error)
            self.logger.log('Error while connecting to database: ', error)

    def create_table_categories(self):
        url = 'https://www.list.am/en/category/54'
        create_table = ('''
                        CREATE TABLE IF NOT EXISTS property_type
                          (id SERIAL PRIMARY KEY,
                          name VARCHAR(255),
                          q_string VARCHAR(16) UNIQUE);
                        ''')
        self.cur.execute(create_table)
        insert_into_table = '''
                             INSERT INTO property_type (name, q_string)
                             VALUES (%s, %s)
                             ON CONFLICT (q_string) DO NOTHING;
                            '''
        with Spider() as spider:
            categories_dict = spider.start_spider("aget_all_categories", [url])
        for key, value in categories_dict[0].items():
            print(key, value)
            self.cur.execute(insert_into_table, (key, value))
        self.conn.commit()

    def create_table_regions(self):
        url = 'https://www.list.am/en/category/'
        create_table = ('''
                        CREATE TABLE IF NOT EXISTS property_type
                          (id SERIAL PRIMARY KEY,
                          name VARCHAR(255),
                          q_string VARCHAR(16) UNIQUE);
                        ''')
        self.cur.execute(create_table)
        insert_into_table = '''
                          INSERT INTO regions (name, q_string)
                          VALUES (%s, %s)
                          ON CONFLICT (q_string) DO NOTHING;
                          '''
        with Spider() as spider:
            regions_dict = spider.start_spider("aget_all_regions", [url])
        for key, value in regions_dict[0].items():
            self.cur.execute(insert_into_table, (key, value))
        self.conn.commit()

    def create_table_urls(self):
        create_table = ('''
                        CREATE TABLE IF NOT EXISTS urls
                          (id SERIAL PRIMARY KEY,
                          url VARCHAR(255) UNIQUE,
                          category_id INT, 
                          region_id INT,
                          retrieved INTEGER DEFAULT 0,
                          created_at TIMESTAMP DEFAULT NOW(),
                          CONSTRAINT fk_category 
                          FOREIGN KEY(category_id)
                          REFERENCES property_type(id),
                          CONSTRAINT fk_region
                          FOREIGN KEY(region_id)
                          REFERENCES regions(id));
                        ''')
        self.cur.execute(create_table)
        self.conn.commit()

    def select_categories(self, categories: Tuple[str] = None):
        if self.table_exists('property_type'):
            if not categories:
                self.cur.execute('''SELECT name, id, q_string
                               FROM property_type;''')
            else:
                self.cur.execute('''SELECT name, id, q_string
                               FROM property_type
                               WHERE name in %s;''', (categories,))
            cat_paths = self.cur.fetchall()
            if len(cat_paths) > 0:
                return cat_paths
        else:
            self.create_table_categories()
            return self.select_categories(categories)

    def select_regions(self, regions: Tuple[str] = None):
        if self.table_exists('regions'):
            if not regions:
                self.cur.execute('''SELECT name, id, q_string
                               FROM regions;''')
            else:
                self.cur.execute('''SELECT name, id, q_string
                               FROM regions
                               WHERE name in %s;''', (regions,))
            reg_paths = self.cur.fetchall()
            if len(reg_paths) > 0:
                return reg_paths
        else:
            self.create_table_regions()
            return self.select_regions(regions)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur and self.conn:
            self.cur.close()
            self.conn.close()