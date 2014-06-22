

# *-* coding: utf-8 *-* 
  
from scrapy.selector import Selector 
from scrapy.http import Request 
from scrapy.spider import Spider 
from scrapy.crawler import Crawler 
from scrapy import signals 
from scrapy.utils.project import get_project_settings 
from twisted.internet import reactor 
import re 
import sys 
  
begin_parsing = sys.argv[1] 
  
csv = open('{0}.csv'.format(begin_parsing), 'a') 
delimiter = ";"
  
class LinkedIn(Spider): 
    name = "LinkedIn"
    allowed_domains = ["linkedin.com"] 
    start_urls = ["http://www.linkedin.com/directory/people-{0}".format(begin_parsing)] 
    all_links = [] 
  
    def parse(self, response): 
        sel = Selector(response) 
        print response.url 
        links = sel.xpath('//*[@id="body"]/div/ul[2]/li/a/@href').extract() 
        if links: 
            for link in links: 
                yield Request(url='http://www.linkedin.com' + link, 
                              callback=self.parse) 
        elif sel.xpath('//*[@id="result-set"]/li/h2//@href').extract(): 
            set_profile = sel.xpath('//*[@id="result-set"]/li/h2//@href').extract() 
            for link in set_profile: 
                yield Request(url=link, 
                              callback=self.parse) 
        else: 
            name = "".join(sel.xpath('//*[@id="name"]/span//text()').extract()).encode('utf-8') 
            job = "".join(sel.xpath('//*[@id="member-1"]/p/text()').extract()).encode('utf-8').strip() 
            current_job = " ".join(sel.xpath('//*[@class="summary-current"]//li/text()').extract()).encode('utf-8').strip() 
            current_job = re.sub("\n", " ", current_job) 
            place = " ".join(sel.xpath('//*[@id="headline"]/dd[1]/span/text()').extract()).encode('utf-8').strip() 
            industry = "".join(sel.xpath('//*[@class="industry"]/text()').extract()).encode('utf-8').strip() 
  
            csv.write(response.url + delimiter + name + delimiter + job + delimiter + current_job + delimiter + industry + delimiter + place + "\n") 
  
  
if __name__ == '__main__': 
    options = { 
        'CONCURRENT_ITEMS': 200, 
        'USER_AGENT': 'Googlebot/2.1 (+http://www.google.com/bot.html)', 
        'CONCURRENT_REQUESTS': 20, 
        'DOWNLOAD_DELAY': 0.5
    } 
  
    spider = LinkedIn() 
    settings = get_project_settings() 
    settings.overrides.update(options) 
    crawler = Crawler(settings) 
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed) 
    crawler.install() 
    crawler.configure() 
    crawler.crawl(spider) 
    crawler.start() 
    # log.start(logfile="results.log", loglevel=log.DEBUG, crawler=crawler, logstdout=False) 
    reactor.run() 