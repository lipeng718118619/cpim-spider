# -*- coding: utf-8 -*-
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Request, FormRequest

import logging

from json import loads
from scrapy_redis.spiders import RedisSpider

from ..items import CpimBaseInfo

import uuid

logger = logging.getLogger(__name__)


class CpimSpider(RedisSpider):
    name = "cpim-spider"

    start_urls = ['https://www.qichamao.com/search/all/~?o=2']

    # start_urls = ['https://www.qichamao.com/search/all?o=2&p=9']

    login_url = 'https://www.qichamao.com/usercenter/dologin'

    login_heads = {'Host': 'www.qichamao.com',
                   'Referer': 'https://www.qichamao.com',
                   'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'}

    login_form_data = {'userId': '13165786609', 'password': 'Pengge757!?*'}

    allowed_domains = ['www.qichamao.com']

    host = 'https://www.qichamao.com'

    def start_requests(self):
        yield Request(self.start_urls[0], meta={'cookiejar': 1}, callback=self.check_login, dont_filter=True)

    # 校验是否需要登录
    def check_login(self, response):

        # css选取判断是否需要登录  true不需要重新登录  false 需要重新登录
        is_login = False if len(response.css('[data-title=登录]')) > 0 else True

        # 需要重新登录
        if is_login is False:
            img_url = self.host + response.css('.code-img').attrib['src']

            yield FormRequest(img_url, meta={"cookiejar": response.meta["cookiejar"]}, callback=self._login,
                              dont_filter=True)

        else:

            yield FormRequest(response.url, meta={"cookiejar": response.meta["cookiejar"]}, dont_filter=True)

    # 登录系统
    def _login(self, response):

        with open("code.gif", 'wb+') as f:
            f.write(response.body)
        print("input  verify code:")
        code = input()
        self.login_form_data['VerifyCode'] = code
        self.login_form_data['sevenDays'] = 'false'
        yield FormRequest(url=self.login_url, meta={"cookiejar": response.meta["cookiejar"]},
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
            yield Request(url=url, meta={"cookiejar": response.meta["cookiejar"]}, headers=self.login_heads,
                          dont_filter=True)

    # 开始爬去数据
    def parse(self, response):
        # 一级页面返回的结果
        if response.url.startswith("https://www.qichamao.com/search/all"):
            try:
                # 非会员只能下载10页
                islimit = response.css('article.main').xpath('string(div)').extract_first().strip()
                if (islimit == '企查猫(企业查询宝)偷偷告诉你，开通VIP可查找10000+企业信息'):
                    for url in self.start_urls:
                        yield Request(url=url, meta={"cookiejar": response.meta["cookiejar"]}, headers=self.login_heads,
                                      dont_filter=True)
            except Exception:
                raise IgnoreRequest

            logger.info("first level page %s" % response.url)
            urls = (orgAttr.attrib['href'] for orgAttr in response.css('.listsec_box.clf').css('.listsec_tit'))

            for url in urls:
                # 二级页面入队
                yield Request(self.host + url, callback=self.parse)
            # 爬起下一个一级页面
            next_url = response.css('a.next')[0].attrib['href']
            yield Request(self.host + next_url, meta={"cookiejar": response.meta["cookiejar"]}, callback=self.parse,
                          dont_filter=True)

        elif response.url.startswith("https://www.qichamao.com/orgcompany/searchitemdtl"):
            logger.info("climb url : %s " % response.url)

            base_info = CpimBaseInfo()

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
            yield base_info
