"""This code helps to gather all information from holidayiq.com"""

from scrapy.selector import Selector
from scrapy.spider import BaseSpider
from scrapy.http import Request
#import MySQLdb
from scrapy.mail import MailSender
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import pymysql
class Holiday(BaseSpider):
    name = 'holiday'
    #start_urls = ['http://www.holidayiq.com/holiday-packages/'] 
    def __init__(self):
        self.conn = pymysql.connect(host="localhost", user="root", password='', db="holidaydb", charset='utf8mb4')
        self.cur = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)


    def spider_closed(self, spider):
        mailer = MailSender(mailfrom = "ramyalatha3004@gmail.com",smtphost  = "smtp.gmail.com", smtpport = 587, smtpuser = "ramyalatha3004@gmail.com", smtppass = "R01491a0237")
        mailer.send(to = ["raja@emmela.com"], subject = "Test mail : Report", body = "Run completed for holiday Crawler ", cc = ["ramya@emmela.com","ramya@atad.xyz"])
    def start_requests(self):
        qry = 'select title,image,link from holi limit 1'
        self.cur.execute(qry)
        rows = self.cur.fetchall()
        for row in rows:
            title, image, link = row
            yield Request(link, callback=self.parse_place, meta={'image':image, 'title':title, 'link':link})
    def parse_place(self, response):
        sel = Selector(response)
        image = response.meta['image']
        place = response.meta['title']
        link = response.meta['link']
        nodes = sel.xpath('//div[@class="pkg-listing-right-main-section"]/div[@class="row"]')
        for node in nodes:
            image_pkg = "".join(node.xpath('./div[@class="image_hidden col-xs-12 col-sm-12 col-md-4 col-lg-4"]/span/text()').extract())
            title_pkg = "".join(node.xpath('./div[@class="col-xs-12 col-sm-12 col-md-8 col-lg-8"]/div[@class="row"]//h2[@class="pkg-list-hotel-name"]/a/text()').extract())
            name = title_pkg.replace("\n", "")
            price = "".join(node.xpath('.//div[@class="pkg-list-price-right"]/h4[@class="pkg-list-hotel-name"]/text()').extract())
            qry = 'insert into holidayiq_hotels(name, image, link, price, place) values (%s,%s, %s, %s, %s)on duplicate key update name = %s'
            values = (name, image_pkg, link, price, place, name)
            self.cur.execute(qry, values)
            self.conn.commit()
