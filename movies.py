"""Crawler for Youtube Movies"""
from scrapy.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.mail import MailSender
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
#import MySQLdb
import pymysql
class Movies(CrawlSpider):
    """Nmae of Crawler"""
    name = 'movies'
    start_urls = ['https://www.youtube.com/movies']
    def __init__(self):
        """Connecting to Database"""
        self.conn = pymysql.connect(host="localhost", user="root", password='', db="Moviesdb", charset='utf8mb4')
        self.cur = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        mailer = MailSender(mailfrom = "ramyalatha3004@gmail.com",smtphost  = "smtp.gmail.com", smtpport = 587, smtpuser = "ramyalatha3004@gmail.com", smtppass = "R01491a0237")
        mailer.send(to = ["raja@emmela.com"], subject = "Test mail : Report", body = "Run completed for Movies Crawler ", cc = ["ramya@emmela.com","ramya@atad.xyz"])

    def parse(self, response):
        """Starting Nodes here"""
        sel = Selector(response)
        nodes =  sel.xpath('//ul[@id="browse-items-primary"]//li[contains(@class, "branded-page-box")]//div[@class="yt-uix-shelfslider-body"]/ul/li')
        for node in nodes:
            link = ''.join(node.xpath('.//div[@class="yt-lockup-thumbnail"]/a/@href').extract())
            url = 'https://www.youtube.com'+link
            yield Request(url, callback = self.parse_movie, meta={})
    def parse_movie(self, response):
        """Moving to next link"""
        sel = Selector(response)
        title = ''.join(sel.xpath('//div[@id="watch-headline-title"]/h1[@class="watch-title-container"]/span[@id="eow-title"]/text()').extract())
        publish = ''.join(sel.xpath('//div[@id="watch-uploader-info"]/strong/text()').extract())
        descr = ''.join(sel.xpath('//div[@id="watch-description-text"]/p[@id="eow-description"]/text()').extract())
        nodes = sel.xpath('//div[@id="watch-description-extras"]//li[contains(@class, "watch-meta-item ")]')
        for node in nodes:
            runtime = ''.join(node.xpath('./h4[contains(text(), "Running time")]/following-sibling::ul[1]/li/text()').extract())
            qry = 'insert into movies(title, publish, descr, runtime)values (%s, %s, %s, %s) on duplicate key update title = %s'
            values = (title, publish, descr, runtime, title)
            print qry%values
            self.cur.execute(qry, values)
            self.conn.commit()  
