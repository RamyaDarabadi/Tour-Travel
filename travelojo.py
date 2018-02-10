from scrapy.spider import BaseSpider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.mail import MailSender
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
#import mysql.connector as mariadb
import pymysql
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class Travelojo_Spider(BaseSpider):
    name = 'travelojo'
    start_urls = ['http://www.travelojo.in/India-tour.html']
                
    def __init__(self, *args, **kwargs):
        self.conn = pymysql.connect(host="localhost", user="root", password='', db="travelojodb", charset='utf8mb4')
        self.cur = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        mailer = MailSender(mailfrom = "ramyalatha3004@gmail.com",smtphost  = "smtp.gmail.com", smtpport = 587, smtpuser = "ramyalatha3004@gmail.com", smtppass = "R01491a0237")
        mailer.send(to = ["raja@emmela.com"], subject = "Test mail : Report", body = "Run completed for Travelojo Crawler ", cc = ["ramya@emmela.com","ramya@atad.xyz"])


    def parse(self, response):
        sel = Selector(response)
        nodes = sel.xpath('//div[@class="container"]/div[@class="banner-bottom-grids"]//div[@class="col-md-4 weekend-grids"]/div[@class="weekend-grid"]//div[@class="weekend-grid-info"]//h6[@class="package_h6"]')
        for node in nodes:
            title = "".join(node.xpath('./a/text()').extract())
            price = "".join(node.xpath('./a/i[@class="price"]/text()').extract())
            price = ''.join(re.findall('\d+', price))
            link =  "".join(node.xpath('./a/@href').extract())
            qry = 'insert into travel(title, link, price, created_at, modified_at, last_seen) values (%s,%s, %s, now(), now(), now() )on duplicate key update title = %s'
            values = (title ,link, price,title)
            print qry%values
            self.cur.execute(qry, values)
            self.conn.commit() 
