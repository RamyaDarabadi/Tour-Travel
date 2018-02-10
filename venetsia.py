import scrapy
from scrapy.selector import Selector
from scrapy import FormRequest
from scrapy.http import Request
#import MySQLdb
class Venetsia(scrapy.Spider):
    name = 'Venetsia'

    start_urls = ['http://www.venetsia.net/VenetsiaAjaxWeb/login.html']

    def parse(self, response):
        return [FormRequest.from_response(response,
                               formdata={'name': 'rovo01', 'password': 'rovo7965'},
                               callback=self.after_login)]
    def after_login(self, response):
        url = 'http://www.venetsia.net/VenetsiaAjaxWeb/venetsiamain.html;jsessionid=4B8EA030EC24A288E92D3CCDCE47AE76'
        yield Request(url, callback = self.parse_login2)

    def parse_login2(self, response):
        import pdb;pdb.set_trace()

        headers_dict = {'Accept':'*/*','Accept-Encoding':'gzip,deflate','Accept-Language':'en-GB,en;q=0.9,en-US;q=0.8,te;q=0.7','Connection':'keep-alive','Content-Length':'1376','Content-Type':'application/x-www-form-urlencoded','Cookie':'JSESSIONID=E0ADE951403737D290F99EC768F4F342','Host':'www.venetsia.net','Origin':'http://www.venetsia.net','Referer':'http://www.venetsia.net/VenetsiaAjaxWeb/venetsiamain.html;jsessionid=B2FF378FAADCA74605A6F335A4BC86AD','User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36','X-Requested-With':'XMLHttpRequest'}

        form_data = {"request":{"task":"SearchMetadata","database":{"name":"TV1O","type":"OHJELMA"},"searchcriteria":{"free":"%28TV1%29.CCOD.%20%28o%29.BTYP.%20%28ohjelma%29.TYPE.%20%28%2525SDAT%253E%253D20171103%253C%253D20171222%29","operator":"AND","orderby":"CIDX,SDAT,BDAT,BTIM","pagenumber":1,"pagesize":1000,"linksearchcriteria":{"base":"YLEC","linkedfields":[{"valuefield":"PCOD","searchfield":"PCOD","conditionfield":"LINK","conditionvalue":"true","conditionrule":2},{"valuefield":"SCOD","searchfield":"SCOD","conditionfield":"LINK","conditionvalue":"true","conditionrule":2}],"searchmode":2,"retrievedfields":"KTUN,BASE,EXT,FILE,PCOD,SCOD,LINK","free":"","pagesize":100,"orderby":"","operator":"OR"}},"retrievetags":["KTUN","BASE","NIMI","NIM2","SIS","SIS2","CCOD","SDAT","VIDS","VIDD","BTIM","ETIM","BDAT","PCOD","LINK","CIDX","ONAM","COUN","YEAR","AGER"]}}

        yield FormRequest('http://www.venetsia.net/GreDi/xhp?id=MediaKsiAjaxSearchService', callback = self.parse_login3,headers = response.headers, method = 'POST', formdata =form_data)

    def parse_login3(self, response):
        import pdb;pdb.set_trace()	




# check login succeed before going on
        #if "authentication failed" in response.body:
         #   self.log("Login failed", level=log.ERROR)
          #  return
