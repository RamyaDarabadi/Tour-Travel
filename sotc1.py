"""This is a Split crawler or getting Packages information"""
from scrapy.spider import BaseSpider
from scrapy.selector import Selector
from scrapy.mail import MailSender
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
#import MySQLdb
import pymysql
class Sotc(BaseSpider):
    """Stats name here which is unique"""
    name = 'sotc'
    start_urls = ['https://www.sotc.in/india-honeymoon-packages']
    def __init__(self):
        """Connecting to database"""
        self.conn = pymysql.connect(host="localhost", user="root", password='', db="sotcdb", charset='utf8mb4', use_unicode=True)
        self.cur = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        mailer = MailSender(mailfrom = "ramyalatha3004@gmail.com",smtphost  = "smtp.gmail.com", smtpport = 587, smtpuser = "ramyalatha3004@gmail.com", smtppass = "R01491a0237")
        mailer.send(to = ["raja@emmela.com"], subject = "Test mail : Report", body = "Run completed for ", cc = ["ramya@emmela.com","ramya@atad.xyz"])
    def parse(self, response):
        """Starting xpaths"""
        sel = Selector(response)
        nodes = sel.xpath('//div[@class="jcarousel"]/ul/li//div[@class="border_gray"]')
        for node in nodes:
            title = "".join(node.xpath(
                './/div[@class="col-lg-12 no-padding"]//h3[@class="package_h3"]/text()').extract())
            image = "".join(node.xpath(
                './/div[@class="col-lg-12 no-padding"]/a/img/@src').extract())
            links = "".join(node.xpath('.//div[@class="col-lg-12 no-padding"]/a/@href').extract())
            link = 'https://www.sotc.in'+links
            price = "".join(node.xpath(
                './/div[@class="col-lg-12 no-padding margin10"]//div[@class="col-md-6 col-xs-6 no-padding text_skyblue font22"]/text()').extract())
            qry = 'insert into sotc(title,link,image,price)values(%s, %s, %s, %s)on duplicate key update title = %s, link=%s'
            values = (title, link, image, price, title, link)
            print qry%values
            self.cur.execute(qry, values)
            self.conn.commit()
