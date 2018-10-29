# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CpimBaseInfo(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # id
    id = scrapy.Field()
    # 统一社会信用代码
    tyshxydm = scrapy.Field()
    # 纳税人识别号
    nsrsbh = scrapy.Field()
    # 注册号
    reg_number = scrapy.Field()
    # 机构代码
    org_code = scrapy.Field()
    # 公司名称
    org_name = scrapy.Field()
    # 法定代表人
    legal_rep = scrapy.Field()
    # 企业类型
    org_type = scrapy.Field()
    # 经营状态
    bus_status = scrapy.Field()
    # 注册资本
    reg_capital = scrapy.Field()
    # 成立日期
    create_time = scrapy.Field()
    # 登记机关
    reg_authority = scrapy.Field()
    # 经营期限
    op_period = scrapy.Field()
    # 所属地区
    area = scrapy.Field()
    # 核准日期
    approval_date = scrapy.Field()
    # 企业地址
    address = scrapy.Field()
    # 经营范围
    bus_scope = scrapy.Field()
    # 所属行业
    industry = scrapy.Field()
    # 前瞻标签
    forward_looking_label = scrapy.Field()
    # 展会标签
    exhibition_label = scrapy.Field()
    # 纬度值
    lat = scrapy.Field()
    # 经度值
    lng = scrapy.Field()
    # geohash算法得到的编码串
    geo_hash = scrapy.Field()

    # 1为精确查找 0为打点
    precise = scrapy.Field()

    # 可信度
    confidence = scrapy.Field()


class CpimspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
