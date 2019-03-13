import datetime
import time

from scrapy_redis.dupefilter import RFPDupeFilter


class RFPDupeFilterOwn(RFPDupeFilter):

    def __init__(self, server, key, debug=False):
        self.start_time = time.strftime("%Y%m%d", time.localtime())

        self.yesterday_time = (datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y%m%d")

        RFPDupeFilter.__init__(self, server, key, debug)

        self.redis_key_today = self.key + self.start_time

        self.redis_key_yesterday = self.key + self.yesterday_time

        # 过期时间为两天
        self.server.sadd(self.redis_key_today, "created")
        self.server.expire(self.redis_key_today, 86400 * 2)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)

        if self.server.sismember(self.redis_key_yesterday, fp) == 1:
            return True

        # This returns the number of values added, zero if already exists.
        added = self.server.sadd(self.redis_key_today, fp)
        return added == 0
