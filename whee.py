from scrapy.http import  Request, FormRequest
from scrapy.spider import BaseSpider
from scrapy.selector import Selector
#import re
#import requests
import json
#import urllib
from ast import *
class WheelseyeNewBrowse(BaseSpider):
    name = 'wheelseyenew_browse'
    start_urls = ['https://wheelseye.com']
    #handle_httpstatus_list = [401, 404, 302, 303, 403, 500, 100]
    def parse(self, response):
        sel = Selector(response)
        res_headers = json.dumps(str(response.headers))
        res_headers = json.loads(res_headers)
        my_dict = literal_eval(res_headers)
        cookies = {}
        for i in my_dict.get('Set-Cookie', []):
	    key_ = i
            data = i.split(';')[0]
            if data:
                try : key, val = data.split('=', 1)
                except : continue
                cookies.update({key.strip():val.strip()})
        headers = {
           'cookie': '%s=%s'%('JSESSIONID', key_),
           'origin': 'https://wheelseye.com',
           'accept-encoding': 'gzip, deflate, br',
           'accept-language': 'en-US,en;q=0.8',
           'x-requested-with': 'XMLHttpRequest',
           'pragma': 'no-cache',
           'user-agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/59.0.3071.109 Chrome/59.0.3071.109 Safari/537.36',
           'content-type': 'application/json; charset=UTF-8',
           'accept': 'application/json, text/javascript, */*; q=0.01',
           'cache-control': 'no-cache',
           'authority': 'wheelseye.com',
           'referer': 'https://wheelseye.com/',
           'method':''
        }
        data = {"userName":"9899263153","password":"qwerty"}
        yield Request('https://wheelseye.com/admin/login', callback=self.parse_login, body=json.dumps(data), headers=headers, method="POST", meta={'headers':headers, 'body':data})

    def parse_login(self, response):
   	yield Request("https://wheelseye.com/dashboard", callback=self.parse_again)

    def parse_again(self, response):
	session_id = response.request.headers.get('Cookie').split('=')[-1]
	cookies = {"opid_trucking":"117", "uId":"117", "isConsignee":"false"}
	cookies.update({"JSESSIONID":session_id})
	headers1 = {
	    'accept-encoding': 'gzip, deflate, br',
	    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
	    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36',
	    'content-type': 'application/json; charset=utf-8',
	    'accept': 'application/json, text/javascript, */*; q=0.01',
	    'referer': 'https://wheelseye.com/dashboard',
	    'authority': 'wheelseye.com',
	    'x-requested-with': 'XMLHttpRequest',
	}
	yield Request("https://wheelseye.com/vehicle/getIdleVehicles?oId=117&pageNo=0&size=10", callback=self.parse_next, headers=headers1, cookies=cookies)

    def parse_next(self, response):
        sel = json.loads(response.body) 
        data_sel = sel.get('data', {})
        list_sel = data_sel.get('list', [])
        #print list_seli
        for i in list_sel:
            veh_name = i.get('vNo', '')
            loc_DTO = i.get('locDTO', {}).get('latLngDTO', {}).get('lat','')
            print loc_DTO
            
        #import pdb;pdb.set_trace() 
