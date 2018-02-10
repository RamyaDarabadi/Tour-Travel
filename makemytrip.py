"""This Crawler gets data from MMT"""
from scrapy.spider import BaseSpider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.mail import MailSender
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
#import MySQLdb
import pymysql
class MakeMyTrip(BaseSpider):
    """Starts name of the Crawler here"""
    name = "MMT"
    start_urls = ['https://www.makemytrip.com/blog/romantic-places']
    def __init__(self):
        """Connecting to Database"""
        self.conn = pymysql.connect(
            host="localhost", user="root", password='', 
            db="MMTdb", charset='utf8mb4')
        self.cur = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        mailer = MailSender(mailfrom = "ramyalatha3004@gmail.com",smtphost  = "smtp.gmail.com", smtpport = 587, smtpuser = "ramyalatha3004@gmail.com", smtppass = "R01491a0237")
        mailer.send(to = ["raja@emmela.com"], subject = "Test mail : Report", body = "Run completed for Makemytrip Crawler ", cc = ["ramya@emmela.com","ramya@atad.xyz"])
    def parse(self, response):
        """Nodes starts here"""
        sel = Selector(response)
        nodes = sel.xpath(
            '//div[@class="category_info row"]//div[@class="category_part col-sm-4 col-xs-12"]')
        for node in nodes:
            """Nodes for required"""
            title = "".join(node.xpath(
                './/div[@class="tile_detail_section append_bottom12"]/p[@class="din-ab text_partinfo search_blog_title append_bottom8"]/a/text()').extract())
            image = "".join(node.xpath('.//p[@class="append_bottom15"]/a/img/@data-src').extract())
            link = "".join(node.xpath(
                './/div[@class="tile_detail_section append_bottom12"]/p[@class="din-ab text_partinfo search_blog_title append_bottom8"]/a/@href').extract())
            publ = "".join(node.xpath(
                './/div[@class="tile_detail_section append_bottom12"]/p[@class="din-ab text_sub_partinfo"]/text()').extract())
            publish = publ.replace('\n\n', '')
            descr = "".join(node.xpath(
                './/p[@class="din-regular image_sub_titleone append_bottom16"]/text()').extract())
            qry = 'insert into mmt(title, descr, image, link, publish)values(%s, %s, %s, %s, %s) on duplicate key update title = %s'
            values = (title, descr, image, link, publish, title)
            print qry%values
            self.cur.execute(qry, values)
            self.conn.commit()
        next_page_link = "".join(sel.xpath(
            '//div[@class="pagination_section clearfix"]//nav[@class="text-center"]//ul[@class="pagination pagination-lg"]//li[@class="active"]//a/@href').extract())
        next_page_link = 'https://www.makemytrip.com/blog/' + next_page_link
        yield Request(next_page_link, callback=self.parse, meta={}) 
