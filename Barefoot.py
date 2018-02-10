"""This Crawler gets data from Barefootholiday website"""
from scrapy.spider import BaseSpider
from scrapy.selector import Selector
from scrapy.mail import MailSender
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
#import MySQLdb 
import pymysql
class Barefoot(BaseSpider):
    """Starts name of the Crawler here"""
    name = "Barefoot"
    start_urls = ['http://barefootholiday.com/holiday-packages/']
    def __init__(self):
        """Connecting to Database"""
        self.conn = pymysql.connect(
            host="localhost", user="root", passwd='',
            db="Barefootdb", charset='utf8mb4')
        self.cur = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        mailer = MailSender(mailfrom="ramyalatha3004@gmail.com",smtphost="smtp.gmail.com",smtpport=587,smtpuser="ramyalatha3004@gmail.com",smtppass="R01491a0237")
        mailer.send(to=["raja@emmela.com"], subject="Test mail : Report", body="Run completed for Barefoot", cc=["ramya@emmela.com", "ramya@atad.xyz"])

    def parse(self, response):
        """Nodes starts here"""
        sel = Selector(response)
        nodes = sel.xpath(
            '//div[@class="featured-packages"]/div[@class="row"]/div[@class="col-sm-6"]')
        for node in nodes:
            title = "".join(node.xpath(
                './/div[@class="layer"]/div[@class="heading"]/text()').extract())
            package = "".join(node.xpath(
                './/div[@class="layer"]/div[@class="sub-heading"]/text()').extract())
            descr = "".join(node.xpath('.//div[@class="description"]/p/text()').extract())
            price = "".join(node.xpath(
                './/div[@class="layer"]//div[@class="pricing"]/div[@class="text"]/text()').extract())
            image = "".join(node.xpath('.//div[@class="image"]/img/@src').extract())
            qry = 'insert into bare(title, package, descr, price, image)values(%s, %s, %s, %s, %s) on duplicate key update title=%s'
            values = (title, package, descr, price, image, title)
            self.cur.execute(qry, values)
            self.conn.commit()
