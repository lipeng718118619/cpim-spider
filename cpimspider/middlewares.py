# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import time
import traceback
from urllib.parse import unquote
from scrapy.http import Request
from scrapy import signals
from scrapy.utils.project import get_project_settings
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
import random
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware
import base64

import logging
from selenium import webdriver
import os
from PIL import Image

from cpimspider.spiders.cpimspider import verification_code_check, COOKIE_JAR

logger = logging.getLogger(__name__)


def verification_code_identification(verify_url):
    """
    用于跳过企查猫验证码
    :param verify_url:
    :return:
    """
    try:
        logger.info("verify_code_url : " + unquote(verify_url, 'utf-8'))
        options = webdriver.ChromeOptions()
        options.headless = True  # 设置启动无界面化
        options.add_argument('window-size=1920,1080')
        driver = webdriver.Chrome(options=options)  # 启动时添加定制的选项

        driver.get(verify_url)
        time.sleep(5)

        timestamp = str(round(time.time() * 1000))
        code_image_filename = timestamp + ".png"
        driver.find_element_by_class_name("code-img").screenshot(code_image_filename)
        # 调整图片大小
        image = Image.open(code_image_filename)
        resize_image = image.resize((80, 35), Image.ANTIALIAS)
        resize_image.save(code_image_filename)

        with open(code_image_filename, 'rb') as f:
            code_image_bytes = f.read()

        os.remove(code_image_filename)
        code = verification_code_check(code_image_bytes)

        logger.info("verify_code: " + code)

        driver.find_element_by_id("verifycode").send_keys(code)

        driver.find_element_by_class_name("btnverify").click()
        time.sleep(5)

        if driver.title != '用户验证-企查猫(企业查询宝)':
            logger.info("verify_code return true")
            return True

        logger.info("verify_code return false")
        return False
    except Exception as e:
        logger.error(traceback.format_exc())
    finally:
        driver.close()
        driver.quit()


class CpimspiderDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        logger.debug("request url : " + request.url)
        logger.debug("response code : %s , url : %s", response.status, response.url)

        if "redirect_urls" not in request.meta or not request.meta['redirect_urls']:
            return response

        """
        需要重定向,验证码识别
        """
        logger.info("redirect_urls :" + str(request.meta['redirect_urls']))

        # 原url
        url = request.meta['redirect_urls'][0]
        # 重定向url
        verify_url = request.meta['redirect_urls'][1]

        """
        进行验证码验证
        """
        count = 0
        while count <= 10:
            if verification_code_identification(verify_url):
                """
                验证码验证成功
                """
                #
                # url_encode = response.url.split("ReturnUrl=")[1]
                # # 解码
                # url_decode = unquote(url_encode, 'utf-8')
                logger.info("ReturnUrl= " + url)

                return Request(url, meta={COOKIE_JAR: request.meta[COOKIE_JAR]}, dont_filter=True)
            else:
                count = count + 1

        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class CpimUserAgentMiddleware(UserAgentMiddleware):
    """""
    设置User-Agent
    """""

    def __init__(self, user_agent):
        self.user_agent = user_agent

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            user_agent=crawler.settings.get('USER_AGENT')
        )

    def process_request(self, request, spider):
        agent = random.choice(self.user_agent)
        request.headers['User-Agent'] = agent


settings = get_project_settings()
PROXY_USER = settings.get("PROXY_USER")
PROXY_PASS = settings.get("PROXY_PASS")
PROXY_SERVER = settings.get("PROXY_SERVER")

# for Python3
proxyAuth = "Basic " + base64.urlsafe_b64encode(bytes((PROXY_USER + ":" + PROXY_PASS), "ascii")).decode("utf8")


class RandomHttpProxyMiddleware(HttpProxyMiddleware):

    def process_request(self, request, spider):
        # # 使用代理的URL
        # if request.url.startswith("https://www.qichamao.com/orgcompany/searchitemdtl"):
        #     request.meta["proxy"] = PROXY_SERVER
        #
        #     request.headers["Proxy-Authorization"] = proxyAuth

        # 其他不使用代理

        # logger.info("download  url : %s" % request.url)
        pass
