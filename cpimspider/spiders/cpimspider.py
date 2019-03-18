# -*- coding: utf-8 -*-
import base64
import os
import time
import urllib
import traceback

from PIL import Image
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Request, FormRequest
import socket
import logging
from urllib.parse import unquote
from json import loads

from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisSpider
from selenium import webdriver

from ..items import CpimspiderItem

import uuid

logger = logging.getLogger(__name__)
settings = get_project_settings()

verification_code_server_host = settings.get("VERIFICATION_CODE_SERVER_HOST")
verification_code_server_port = settings.get("VERIFICATION_CODE_SERVER_PORT")

COOKIE_JAR = 'cookiejar'


def verification_code_check(img_content):
    """
    验证码识别
    :param img_content:
    :return:
    """
    content = base64.b64encode(img_content).decode()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((verification_code_server_host, verification_code_server_port))
    data = urllib.parse.quote_plus(content).encode()
    s.sendall(data)
    dat = s.recv(1024)
    s.close()
    return dat.decode()


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


class CpimSpider(RedisSpider):
    name = "cpim-spider"

    start_urls = ['https://www.qichamao.com/search/all/~?o=2']
    login_url = 'https://www.qichamao.com/usercenter/dologin'

    login_heads = {'Host': 'www.qichamao.com',
                   'Referer': 'https://www.qichamao.com',
                   'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 '
                                 '(KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'}

    login_form_data = {'userId': '17162188447', 'password': 'aa162760'}

    allowed_domains = ['www.qichamao.com']

    host = 'https://www.qichamao.com'

    def start_requests(self):
        yield Request(self.start_urls[0], meta={COOKIE_JAR: 1}, callback=self.check_login, dont_filter=True)

    # 校验是否需要登录
    def check_login(self, response):

        # css选取判断是否需要登录  true不需要重新登录  false 需要重新登录
        is_login = False if len(response.css('[data-title=登录]')) > 0 else True

        logger.info("check login return : " + is_login)

        # 需要重新登录
        if is_login is False:
            img_url = self.host + response.css('.code-img').attrib['src']

            yield FormRequest(img_url, meta={COOKIE_JAR: response.meta[COOKIE_JAR]},
                              callback=self._login, dont_filter=True)
        else:
            yield FormRequest(response.url, meta={COOKIE_JAR: response.meta[COOKIE_JAR]}, dont_filter=True)

    # 登录系统
    def _login(self, response):
        code = verification_code_check(response.body)

        self.login_form_data['VerifyCode'] = code
        self.login_form_data['sevenDays'] = 'false'
        logger.info("login qcm ,code: %s ", code)
        yield FormRequest(url=self.login_url, meta={COOKIE_JAR: response.meta[COOKIE_JAR]},
                          headers=self.login_heads, formdata=self.login_form_data,
                          callback=self._parse_login, dont_filter=True)

    # 解析登录结果
    def _parse_login(self, response):
        # 判断是否登录成功
        if loads(response.text)['isSuccess']:
            logger.info("login success")
        else:
            # 登录失败
            logger.error("login failed" + response.text)
            return
        # 登录成功开始爬去信息页
        for url in self.start_urls:
            yield Request(url=url, meta={COOKIE_JAR: response.meta[COOKIE_JAR]}, headers=self.login_heads,
                          dont_filter=True)

    # 开始爬去数据
    def parse(self, response):
        # 一级页面返回的结果
        if response.url.startswith("https://www.qichamao.com/search/all"):
            try:
                # 非会员只能下载10页
                is_limit = response.css('article.main').xpath('string(div)').extract_first().strip()
                if is_limit == '企查猫(企业查询宝)偷偷告诉你，开通VIP可查找10000+企业信息':
                    for url in self.start_urls:
                        yield Request(url=url, meta={COOKIE_JAR: response.meta[COOKIE_JAR]}, headers=self.login_heads,
                                      dont_filter=True)
            except Exception:
                raise IgnoreRequest

            logger.info("first level page %s" % response.url)
            urls = (orgAttr.attrib['href'] for orgAttr in response.css('.listsec_box.clf').css('.listsec_tit'))

            for url in urls:
                # 二级页面入队
                yield Request(self.host + url, meta={COOKIE_JAR: response.meta[COOKIE_JAR]}, callback=self.parse,
                              dont_filter=False)

            # 爬起下一个一级页面
            try:
                if 'href' in response.css('a.next')[0].attrib:
                    next_url = response.css('a.next')[0].attrib["href"]
                else:
                    next_url = "/search/all/~?o=2"
            except Exception as e:
                logger.error(traceback.format_exc())

            logger.info("next first level page %s" % next_url)
            yield Request(self.host + next_url, meta={COOKIE_JAR: response.meta[COOKIE_JAR]}, callback=self.parse,
                          dont_filter=True)

        # 二级url 爬去企业信息
        elif response.url.startswith("https://www.qichamao.com/orgcompany/searchitemdtl"):
            logger.info("climb url : %s " % response.url)

            base_info = CpimspiderItem()

            art_basics = response.css('section.pb-d2').css('[class=art-basic]').css('li')
            # 基本数据提取
            dict_data = {
                basic.css('.tit::text').extract_first().replace('：', ''): basic.xpath(
                    "string(span[@class='info'])").extract_first() for
                basic in art_basics}
            base_info['id'] = str(uuid.uuid1()).replace("-", "")
            base_info['tyshxydm'] = dict_data['统一社会信用代码']
            base_info['nsrsbh'] = dict_data['纳税人识别号']
            base_info['reg_number'] = dict_data['注册号']
            base_info['org_code'] = dict_data['机构代码']
            base_info['org_name'] = dict_data['名称']
            base_info['legal_rep'] = dict_data['法定代表人']
            base_info['org_type'] = dict_data['企业类型']
            base_info['bus_status'] = dict_data['经营状态']
            base_info['reg_capital'] = dict_data['注册资本']
            base_info['create_time'] = dict_data['成立日期']
            base_info['reg_authority'] = dict_data['登记机关']
            base_info['op_period'] = dict_data['经营期限']
            base_info['area'] = dict_data['所属地区']
            base_info['approval_date'] = dict_data['核准日期']
            base_info['address'] = dict_data['企业地址']
            base_info['bus_scope'] = dict_data['经营范围']

            art_basic_swots = response.css('ul.art-basic.art-basic-swot').css('li')
            art_basic_swot_dict = {art_basic_swot.css('.tit::text').extract_first().replace('：', ''):
                ";".join(
                    [info.extract() for info in art_basic_swot.css('.info').css('.blue::text')])
                for art_basic_swot in art_basic_swots}

            base_info['industry'] = art_basic_swot_dict['所属行业']
            base_info['forward_looking_label'] = art_basic_swot_dict['前瞻标签']
            base_info['exhibition_label'] = art_basic_swot_dict['展会标签']

            base_info['cookie'] = ""
            base_info['crop_telephone'] = ""
            base_info['crop_email'] = ""
            base_info['crop_data_code'] = ""
            base_info['contacts'] = ""

            # 联系方式提取

            base_info['insert_time'] = time.strftime("%Y%m%d%H%M", time.localtime())

            CpimSpider.set_contact_item(response, base_info)
            yield base_info

        elif response.url.startswith("https://www.qichamao.com/userCenter/UserVarify"):
            """
            进行验证码验证
            """
            count = 0
            while count <= 5:
                if verification_code_identification(response.url):

                    url_encode = response.url.split("ReturnUrl=")[1]
                    # 解码
                    url_decode = unquote(url_encode, 'utf-8')
                    logger.info("ReturnUrl= " + url_decode)

                    yield Request(url_decode, callback=self.parse, meta={COOKIE_JAR: response.meta[COOKIE_JAR]},
                                  dont_filter=True)
                    break
                else:
                    count = count + 1

    # 处理联系信息
    @staticmethod
    def set_contact_item(response, item):

        try:
            cookie = ""
            crop_telephone = ""
            crop_email = ""
            data_code = ""
            if response.request.headers.has_key('Cookie'):
                cookie = response.request.headers.get('Cookie').decode()

            # ["企业电话:139989999","企业邮箱":"88899@qq.com"]
            arthd_info_list = [arthd_info.css('span::text').extract_first() for arthd_info in
                               response.css('.arthd_info')]
            if not arthd_info_list is None and isinstance(arthd_info_list, list):

                for arthd_info in arthd_info_list:
                    if "电话" in arthd_info:
                        crop_telephone = arthd_info.split("：")[1]

                    if "邮箱" in arthd_info:
                        crop_email = arthd_info.split("：")[1]

            if response.css('button[id="arthd-mophone"]'):
                data_code = response.css('button[id="arthd-mophone"]')[0].attrib.get("data-code")

            item['cookie'] = cookie
            item['crop_telephone'] = crop_telephone
            item['crop_email'] = crop_email
            item['crop_data_code'] = data_code
        except TypeError:
            pass
        except Exception as e:
            logger.error("set_contact_item ignore exception : " + repr(e))
