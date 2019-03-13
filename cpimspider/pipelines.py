# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import random

import pymysql
from scrapy.utils.project import get_project_settings

from DBUtils.PooledDB import PooledDB
import logging
import json

import requests
from scrapy_redis import defaults
from scrapy_redis.pipelines import RedisPipeline, default_serialize

settings = get_project_settings()

logger = logging.getLogger(__name__)


class LBSDataPipeline(object):
    """
    地图服务 根据企业地址 获取geohash串
    """

    # spider 开启时调用
    def __init__(self):
        self.url = settings.get("LBS_URL")

    def open_spider(self, spider):
        pass

    def process_item(self, item, spider):

        item["lng"] = ""
        item["lat"] = ""
        item["geo_hash"] = ""
        try:
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
                    logger.error("lbs service return error! msg: " + rsp.text)
            else:
                logger.error("item[address] is null!")
        except Exception as e:
            logger.error("call " + self.url + " error! msg: " + rsp.text)

        return item


class QCMGetCropContactPipeline(object):
    """
    企查猫爬取联系信息
    """

    # spider 开启时调用
    def __init__(self):
        self.url = settings.get("QCH_GET_CONTACT_URL")
        self.user_agent = settings.get('USER_AGENT')

    def open_spider(self, spider):
        pass

    def process_item(self, item, spider):

        try:
            data_code = item['crop_data_code']
            # 不存在data_code 返回 不做处理
            if data_code.strip() == '':
                return item

            headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                       "X-Requested-With": "XMLHttpRequest", "Host": "www.qichamao.com",
                       "Cookie": item['cookie']}

            agent = random.choice(self.user_agent)

            headers['User-Agent'] = agent
            response = requests.post(self.url, headers=headers, data={"code": data_code})

            if response.status_code == 200:
                data = json.loads(response.text)
                if data['isSuccess']:
                    item["contacts"] = data["dataList"]
                    return item
                else:
                    logger.error("call " + self.url + " error! msg: " + response.text)

            else:
                logger.error("call " + self.url + " error! msg: " + response.text)
        except Exception as e:
            logger.error("QCMGetCropContactPipeline error !" + repr(e))

        return item


class SaveDataPipeline(object):
    """
    爬取的数据保存到数据库
    """

    # spider 开启时调用
    def __init__(self):
        db_name = settings.get("DB_NAME")
        host = settings.get("DB_HOST")
        port = settings.get("DB_PORT")
        user = settings.get("DB_USER")
        password = settings.get("DB_PASSWORD")

        self._pool = PooledDB(pymysql, 10, 60, database=db_name, host=host, port=port, user=user,
                              password=password)

        self._insert_corp_info_sql = """insert into stg_corp_info(`id`, 
                               `org_name`, `org_code`, `org_type`, `address`, `tyshxydm`, `nsrsbh`, `reg_number`, 
                               `legal_rep`, `bus_status`, `reg_capital`, `create_time`, `reg_authority`, `op_period`, 
                                `area`, `approval_date`, `bus_scope`, `industry`, `fwlk_label`, `eht_label`,
                                `crop_telephone`,`crop_email`,`crop_data_code`,
                                `insert_time`) values ( %s, %s, %s, %s, %s, %s, %s,%s,%s,
                                %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        self._insert_corp_address_sql = """INSERT INTO stg_corp_address(id,lat,lng,geo_hash,
                           precise,confidence,insert_time) VALUES (%s, %s, %s, %s, %s, %s, %s);"""

        self._insert_corp_contact_sql = """INSERT INTO stg_corp_contact(corp_id,contact_name,contact)
         VALUES (%s, %s, %s);"""

    def open_spider(self, spider):
        # 创建 org_info 表
        self.create_corp_info_table()
        # 创建 org_address
        self.create_corp_address_table()
        self.create_corp_contact_table()

    def process_item(self, item, spider):
        try:
            conn = self._pool.connection()
            with conn.cursor() as cur:
                self.insert_corp_info(item, cur)
                conn.commit()
                self.insert_corp_address(item, cur)
                conn.commit()
                self.insert_corp_contact(item, cur)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error("data write db error! data: " + item)
            logger.error(repr(e))

        return item

    # spider 关闭时调用
    def close_spider(self, spider):
        if self._pool is not None:
            self._pool.close()

    # 不存在表则创建 stg_corp_info
    def create_corp_info_table(self):
        create_sql = """CREATE TABLE IF NOT EXISTS `stg_corp_info` (
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
                  `crop_telephone` varchar(32) DEFAULT NULL COMMENT '企业电话',
                  `crop_email` varchar(64) DEFAULT NULL COMMENT '企业邮箱',
                  `crop_data_code` varchar(64) DEFAULT NULL COMMENT '企业联系方式查询代码',
                  `insert_time` varchar(32) DEFAULT NULL COMMENT '写入时间',
                   PRIMARY KEY (`id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

        conn = self._pool.connection()
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        conn.close()

    def create_corp_address_table(self):
        create_sql = """CREATE TABLE IF NOT EXISTS `stg_corp_address` (
              `id` varchar(32) NOT NULL,
              `lat` varchar(32) DEFAULT NULL COMMENT '纬度值',
              `lng` varchar(32) DEFAULT NULL COMMENT '经度值',
              `geo_hash` varchar(32) DEFAULT NULL COMMENT 'geoHash算法得到的编码串',
              `precise` int(11) DEFAULT NULL COMMENT '位置的附加信息，是否精确查找。1为精确查找，即准确打点；0为不精确，即模糊打点',
              `confidence` int(11) DEFAULT NULL COMMENT '可信度，描述打点准确度，大于80表示误差小于100m。该字段仅作参考，返回结果准确度主要参考precise参数。',
               `insert_time` varchar(32) DEFAULT NULL COMMENT '写入时间',
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

        conn = self._pool.connection()
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        conn.close()

    def create_corp_contact_table(self):
        create_sql = """CREATE TABLE IF NOT EXISTS `stg_corp_contact` (
                        `id` int(11) NOT NULL AUTO_INCREMENT, 
                        `corp_id` varchar(32) NOT NULL,
                        `contact_name` varchar(64) DEFAULT NULL COMMENT '联系人姓名',
                        `contact` varchar(32) DEFAULT NULL COMMENT '机构代码',
                         PRIMARY KEY (`id`)
                      ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"""

        conn = self._pool.connection()
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        conn.close()

    # 插入数据 stg_corp_address
    def insert_corp_address(self, item, cur):

        if item["lat"] == "" or item["lng"] == "":
            return

        values = (item['id'],
                  item['lat'],
                  item['lng'],
                  item['geo_hash'],
                  item['precise'],
                  item['confidence'],
                  item['insert_time'])
        cur.execute(self._insert_corp_address_sql, values)

    # 插入数据 stg_corp_info
    def insert_corp_info(self, item, cur):
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
                  item['exhibition_label'],
                  item['crop_telephone'],
                  item['crop_email'],
                  item['crop_data_code'],
                  item['insert_time'])
        cur.execute(self._insert_corp_info_sql, values)

    # 插入数据 stg_corp_contact
    def insert_corp_contact(self, item, cur):

        try:
            contacts = item['contacts']
            if contacts is None or not isinstance(contacts, list) or len(contacts) == 0:
                return

            for contact in contacts:
                values = (item['id'], contact["oc_contactName"], contact['oc_contact'])
                cur.execute(self._insert_corp_contact_sql, values)
        except Exception as e:
            logger.error("write stg_corp_contact table error! data :" + contacts)
            logger.error(repr(e))


class RedisPipelineOwn(RedisPipeline):
    """
    重写RedisPipeline方法 不在redis内保存item数据
    """

    def __init__(self, server,
                 key=defaults.PIPELINE_KEY,
                 serialize_func=default_serialize):
        RedisPipeline.__init__(server, key, serialize_func)

    def _process_item(self, item, spider):
        """
        不保存爬取的item数据
        :param item:
        :param spider:
        :return:
        """
        # key = self.item_key(item, spider)
        # data = self.serialize(item)
        # self.server.rpush(key, data)
        return item
