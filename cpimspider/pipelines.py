# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
from scrapy.utils.project import get_project_settings

from DBUtils.PooledDB import PooledDB


class SaveDataPipeline(object):

    # spider 开启时调用
    def open_spider(self, spider):
        settings = get_project_settings()

        db_name = settings.get("DB_NAME")
        host = settings.get("DB_HOST")
        port = settings.get("DB_PORT")
        user = settings.get("DB_USER")
        password = settings.get("DB_PASSWORD")
        self._pool = PooledDB(pymysql, 10, 60, database=db_name, host=host, port=port, user=user,
                              password=password)

        self._insert_sql = """insert into `test`.`stg_org_info` ( `qzbq`, `create_time`, `tyshxydm`, `hzrq`, `fddbr`, `jgdm`, `djjg`, `zczb`, `ssxy`, `nsrsbh`, `name`, `jyzt`, `jyqy`, `jyfw`, `org_type`, `zch`, `ssdq`, `address`, `zhbq`) values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    def process_item(self, item, spider):
        conn = self._pool.connection()
        with conn.cursor() as cur:
            self.insert_db(item, cur)
        conn.commit()
        conn.close()

    # spider 关闭时调用
    def close_spider(self, spider):
        if self._pool is not None:
            self._pool.close()

    def insert_db(self, item, cur):
        values = (item['qzbq'],
                  item['create_time'],
                  item['tyshxydm'],
                  item['hzrq'],
                  item['fddbr'],
                  item['jgdm'],
                  item['djjg'],
                  item['zczb'],
                  item['ssxy'],
                  item['nsrsbh'],
                  item['name'],
                  item['jyzt'],
                  item['jyqy'],
                  item['jyfw'],
                  item['org_type'],
                  item['zch'],
                  item['ssdq'],
                  item['address'],
                  item['zhbq'])
        cur.execute(self._insert_sql, values)


class CpimspiderPipeline(object):
    def process_item(self, item, spider):
        return item
