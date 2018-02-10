'''This crawler is for study purpose and it is crawling data from coxkings.com'''
from scrapy.spider import BaseSpider 
from scrapy.selector import Selector
from scrapy.mail import MailSender
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
#from scrapy.http import Request
#import MySQLdb
import pymysql
class Cox(BaseSpider):
    """Defining name of crawler"""
    name = 'coxkings'
    start_urls = [
        'http://www.coxandkings.com/bharatdeko/north-india/', 'http://www.coxandkings.com/bharatdeko/south-india/', 'http://www.coxandkings.com/bharatdeko/west-india/', 'http://www.coxandkings.com/bharatdeko/east-india/', 'http://www.coxandkings.com/bharatdeko/central-india/']
    def __init__(self):
        """connecting to database"""
        self.conn = pymysql.connect(
            host="localhost", user="root", password='', db="kingsdb", charset='utf8mb4')
        self.cur = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        mailer = MailSender(mailfrom = "ramyalatha3004@gmail.com",smtphost  = "smtp.gmail.com", smtpport = 587, smtpuser = "ramyalatha3004@gmail.com", smtppass = "R01491a0237")
        mailer.send(to = ["raja@emmela.com"], subject = "Test mail : Report", body = "Run completed for cox_kings Crawler", cc = ["ramya@emmela.com","ramya@atad.xyz"])
    def parse(self, response):
        """returns following nodes"""
        sel = Selector(response)
        nodes = sel.xpath('//div[@id="pacakage_container_bd"]/div[contains(@id, "table")]')
        for node in nodes:
            title = ''.join(node.xpath(
                './div[@class="floatl"]//div[@class="floatl padr5"]/h4/text()').extract())
            daynyt = ''.join(node.xpath(
                './div[@class="floatl"]//div[@class="floatl bd_days"]/text()').extract())
            cost = ''.join(node.xpath(
                './div[@class="floatl"]//div[@class="price"]/text()').extract())
            qry = 'insert into kings(title, daynyt_1, cost_1)values(%s, %s, %s)'
            values = (title, daynyt, cost)
            print qry%values
            self.cur.execute(qry, values)
            self.conn.commit()
