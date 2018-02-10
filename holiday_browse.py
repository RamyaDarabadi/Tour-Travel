"""This crawler will extract data from holidayiq"""
from scrapy.selector import Selector
from scrapy.spider import BaseSpider
from scrapy.http import Request
from scrapy.mail import MailSender
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

#import MySQLdb
import pymysql
class Holiday(BaseSpider):
    name = 'holiday_browse'
    start_urls = ['http://www.holidayiq.com/holiday-packages/'] 
    def __init__(self, *args, **kwargs):
        self.conn = pymysql.connect(host="localhost", user="root", password='', db="holidaydb", charset='utf8mb4')
        self.cur = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)
    def spider_closed(self, spider):
        mailer = MailSender(mailfrom = "ramyalatha3004@gmail.com",smtphost  = "smtp.gmail.com", smtpport = 587, smtpuser = "ramyalatha3004@gmail.com", smtppass = "R01491a0237")
        mailer.send(to = ["raja@emmela.com"], subject = "Test mail : Report", body = "Run completed for Holiday Crawler ", cc = ["ramya@emmela.com","ramya@atad.xyz"])


    def parse(self, response):
        sel = Selector(response)
        #nodes = sel.xpath('//div/uli[@class="grid cs-style-3"]')
        nodes = sel.xpath('//div[@class="pkg-top-images-section"]/ul[@class="grid cs-style-3"]/li/a')
        for node in nodes:
            url_text = "".join(node.xpath('./@href').extract())
            url = 'http://www.holidayiq.com' + url_text
            image = "".join(node.xpath('./figure/img/@src').extract())
            title = "".join(node.xpath('./figure/span[@class="pkg-grid-txt"]/text()').extract())
            qry = 'insert into holi(title, image ,link) values (%s, %s, %s) on duplicate key update title = %s'
            values = (title, image, url, title)
            print qry%values
            self.cur.execute(qry, values)
            self.conn.commit()
