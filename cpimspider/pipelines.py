# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
from scrapy.utils.project import get_project_settings

from DBUtils.PooledDB import PooledDB
import logging
import json

import requests

settings = get_project_settings()

logger = logging.getLogger(__name__)


class LBSDataPipeline(object):

    # spider 开启时调用
    def open_spider(self, spider):
        self.url = settings.get("LBS_URL")

    def process_item(self, item, spider):
        if item['address'] is not None:
            rsp = requests.get(self.url, params={"address": item['address']})
            if rsp.status_code == 200:
                data = json.loads(rsp.text)
                if data['status'] == 0:
                    result = data["result"]
                    if result["location"] is not None:
                        location = result["location"]
                        item["lng"] = location["lng"]
                        item["lat"] = location["lat"]
                        item["geo_hash"] = location["geoHashCode"]
                    item["precise"] = result["precise"]
                    item["confidence"] = result["confidence"]
            else:
                logger.error("lbs service return error")
        else:
            logger.error("item[address] is null")

        return item


class SaveDataPipeline(object):

    # spider 开启时调用
    def open_spider(self, spider):
        db_name = settings.get("DB_NAME")
        host = settings.get("DB_HOST")
        port = settings.get("DB_PORT")
        user = settings.get("DB_USER")
        password = settings.get("DB_PASSWORD")

        self._pool = PooledDB(pymysql, 10, 60, database=db_name, host=host, port=port, user=user,
                              password=password)

        self._insert_org_info_sql = """insert into stg_org_info(`id`, `org_name`, `org_code`, `org_type`, `address`, `tyshxydm`, 
                                            `nsrsbh`, `reg_number`, `legal_rep`, `bus_status`, `reg_capital`, `create_time`, `reg_authority`, `op_period`, 
                                            `area`, `approval_date`, `bus_scope`, `industry`, `fwlk_label`, `eht_label`) values ( %s, %s, %s, %s, %s, %s, 
                                            %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        self._insert_org_address_sql = """INSERT INTO stg_org_address(id,org_name,address,lat,lng,geo_hash,
                    precise,confidence) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
        # 创建 org_info 表
        self.create_org_info_table()
        # 创建 org_address
        self.create_org_address_table()

    def process_item(self, item, spider):
        conn = self._pool.connection()
        with conn.cursor() as cur:
            self.insert_org_info(item, cur)
            self.insert_org_address(item, cur)
        conn.commit()
        conn.close()

        return item

    # spider 关闭时调用
    def close_spider(self, spider):
        if self._pool is not None:
            self._pool.close()

    # 不存在表则创建 stg_org_info
    def create_org_info_table(self):
        create_sql = """CREATE TABLE IF NOT EXISTS `stg_org_info` (
                  `id` varchar(32) NOT NULL,
                  `org_name` varchar(64) DEFAULT NULL,
                  `org_code` varchar(32) DEFAULT NULL COMMENT '机构代码',
                  `org_type` varchar(64) DEFAULT NULL COMMENT '企业类型',
                  `address` varchar(255) DEFAULT NULL COMMENT '企业地址',
                  `tyshxydm` varchar(64) DEFAULT NULL COMMENT '统一社会信用代码',
                  `nsrsbh` varchar(64) DEFAULT NULL COMMENT '纳税人识别号',
                  `reg_number` varchar(64) DEFAULT NULL COMMENT '注册号',
                  `legal_rep` varchar(16) DEFAULT NULL COMMENT '法定代表人',
                  `bus_status` varchar(16) DEFAULT NULL COMMENT '经营状态',
                  `reg_capital` varchar(32) DEFAULT NULL COMMENT '注册资本',
                  `create_time` date DEFAULT NULL,
                  `reg_authority` varchar(32) DEFAULT NULL COMMENT '登记机关',
                  `op_period` varchar(32) DEFAULT NULL COMMENT '经营期限',
                  `area` varchar(64) DEFAULT NULL COMMENT '所属地区',
                  `approval_date` varchar(16) DEFAULT NULL COMMENT '核准日期',
                  `bus_scope` varchar(1024) DEFAULT NULL COMMENT '经营范围',
                  `industry` varchar(1024) DEFAULT '' COMMENT '所属行业',
                  `fwlk_label` varchar(512) DEFAULT NULL COMMENT '前瞻标签',
                  `eht_label` varchar(512) DEFAULT NULL COMMENT '展会标签',
                   PRIMARY KEY (`id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

        conn = self._pool.connection()
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        conn.close()

    def create_org_address_table(self):
        create_sql = """CREATE TABLE IF NOT EXISTS `stg_org_address` (
              `id` varchar(32) NOT NULL,
              `org_name` varchar(64) DEFAULT NULL,
              `address` varchar(255) DEFAULT NULL COMMENT '企业地址',
              `lat` varchar(32) DEFAULT NULL COMMENT '纬度值',
              `lng` varchar(32) DEFAULT NULL COMMENT '经度值',
              `geo_hash` varchar(32) DEFAULT NULL COMMENT 'geoHash算法得到的编码串',
              `precise` int(11) DEFAULT NULL COMMENT '位置的附加信息，是否精确查找。1为精确查找，即准确打点；0为不精确，即模糊打点',
              `confidence` int(11) DEFAULT NULL COMMENT '可信度，描述打点准确度，大于80表示误差小于100m。该字段仅作参考，返回结果准确度主要参考precise参数。',
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

        conn = self._pool.connection()
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        conn.close()

    # 插入数据 stg_org_address
    def insert_org_address(self, item, cur):
        values = (item['id'],
                  item['org_name'],
                  item['address'],
                  item['lat'],
                  item['lng'],
                  item['geo_hash'],
                  item['precise'],
                  item['confidence'])
        cur.execute(self._insert_org_address_sql, values)

    # 插入数据 stg_org_info
    def insert_org_info(self, item, cur):
        values = (item['id'],
                  item['org_name'],
                  item['org_code'],
                  item['org_type'],
                  item['address'],
                  item['tyshxydm'],
                  item['nsrsbh'],
                  item['reg_number'],
                  item['legal_rep'],
                  item['bus_status'],
                  item['reg_capital'],
                  item['create_time'],
                  item['reg_authority'],
                  item['op_period'],
                  item['area'],
                  item['approval_date'],
                  item['bus_scope'],
                  item['industry'],
                  item['forward_looking_label'],
                  item['exhibition_label'])
        cur.execute(self._insert_org_info_sql, values)


class CpimspiderPipeline(object):
    def process_item(self, item, spider):
        return item
