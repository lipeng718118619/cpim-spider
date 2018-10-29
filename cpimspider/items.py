# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CpimBaseInfo(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # 统一社会信用代码
    tyshxydm = scrapy.Field()
    # 纳税人识别号
    nsrsbh = scrapy.Field()
    # 注册号
    zch = scrapy.Field()
    # 机构代码
    jgdm = scrapy.Field()
    # 公司名称
    name = scrapy.Field()
    # 法定代表人
    fddbr = scrapy.Field()
    # 企业类型
    org_type = scrapy.Field()
    # 经营状态
    jyzt = scrapy.Field()
    # 注册资本
    zczb = scrapy.Field()
    # 成立日期
    create_time = scrapy.Field()
    # 登记机关
    djjg = scrapy.Field()
    # 经营期限
    jyqy = scrapy.Field()
    # 所属地区
    ssdq = scrapy.Field()
    # 核准日期
    hzrq = scrapy.Field()
    # 企业地址
    address = scrapy.Field()
    # 经营范围
    jyfw = scrapy.Field()
    # 所属行业
    ssxy = scrapy.Field()
    # 前瞻标签
    qzbq = scrapy.Field()
    # 展会标签
    zhbq = scrapy.Field()


class CpimspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
