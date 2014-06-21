# *-* coding: utf-8 *-*

from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.spider import Spider
from scrapy.crawler import Crawler
from scrapy import log, signals
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

import re
import sys
import peewee

from peewee import *

db = MySQLDatabase('scrapy', user='root', passwd='')

# Create column to database
class LinkedInToBase(peewee.Model):
    url = peewee.TextField()
    name = peewee.CharField(max_length="40", verbose_name='Name')
    job = peewee.TextField(verbose_name='Job')
    current_job = peewee.TextField(verbose_name='Current job')
    place = peewee.CharField(max_length="40", verbose_name="Place of residence")
    industry = peewee.CharField(max_length="50", verbose_name="Industry")
    education = peewee.TextField(verbose_name="Education")
    skills = peewee.TextField(verbose_name='Skills')
    work_experience = peewee.TextField(verbose_name="Work experience")

    class Meta:
        database = db

try:
    LinkedInToBase.create_table()
    url_in_base = []
except peewee.InternalError:
    list_links = [column.url for column in LinkedInToBase.select()]

if len(sys.argv) > 1:
    begin_parsing = sys.argv[1]
else:
    begin_parsing = None

class LinkedIn(Spider):
    name = "LinkedIn"
    allowed_domains = ["linkedin.com"]
    if begin_parsing == None:
        start_urls = ["http://www.linkedin.com/directory/people-{0}".format(value)
                      for value in "abcdefghijklmnopqrstuvwxyz"]
    else:
        start_urls = ["http://www.linkedin.com/directory/people-{0}".format(begin_parsing)]

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
                yield Request(url=link, callback=self.parse)
        else:
            if response.url not in url_in_base:
                name = "".join(sel.xpath('//*[@id="name"]/span//text()').extract()).encode('utf-8')
                job = "".join(sel.xpath('//*[@id="member-1"]/p/text()').extract()).encode('utf-8').strip()
                current_job = " ".join(sel.xpath('//*[@class="summary-current"]//li/text()').extract()).encode('utf-8').strip()
                current_job = re.sub("\n", " ", current_job)
                place = " ".join(sel.xpath('//*[@id="headline"]/dd[1]/span/text()').extract()).encode('utf-8').strip()
                industry = "".join(sel.xpath('//*[@class="industry"]/text()').extract()).encode('utf-8').strip()

                education = ", ".join(self.clear(sel.xpath('//*[@class="summary-education"]/ul/li//text()').extract()))
                skills = ", ".join(self.clear(sel.xpath('//*[@id="skills-list"]/li/span/text()').extract()))

                # Work experience
                position = sel.xpath('//*[@class="content vcalendar"]/div/div/div/div/h3/span/text()').extract()
                name_company = sel.xpath('//*[@class="content vcalendar"]/div/div/div/div/h4/strong/a/span/text()').extract()
                begin_work = sel.xpath('//*[@class="content vcalendar"]/div/div/div/p/abbr[@class="dtstart"]/text()').extract()
                end_work = sel.xpath('//*[@class="content vcalendar"]/div/div/div/p/abbr[@class="dtend"]/text() | '
                                     '//*[@class="content vcalendar"]/div/div/div/p/abbr[@class="dtstamp"]/text()').extract()

                time_work = list(map(lambda x: " - ".join(x), zip(begin_work, end_work)))
                information_work = []
                for value in range(1, len(position) + 1):
                    info = sel.xpath('//*[@id="profile-experience"]/div[2]/div/div[{0}]/div/''p[3]/text()'.format(value)).extract()
                    clear_info = list(map(lambda x: x.strip().encode('utf-8'), info))
                    information_work.append(", ".join(clear_info))

                all_work_experience = [] # Work experience total
                for work in list(zip(position, name_company, time_work, information_work)):
                    all_work_experience.append("('Position': {0}, 'Name company': {1}, 'Time work': {2}, 'Information about work': {3})".format(*work))

                base = LinkedInToBase(
                    url=response.url,
                    name=name,
                    job=job,
                    current_job=current_job,
                    place=place,
                    industry=industry,
                    education=education,
                    skills=skills,
                    work_experience=all_work_experience
                )
                base.save()

    def clear(self, text):
        result = list(filter(lambda x: bool(x), list(map(lambda x: x.strip(), text))))
        total_result = list(map(lambda x: x.replace(u'\u2022\t', ''), result))
        return total_result

if __name__ == '__main__':
    options = {
        'CONCURRENT_ITEMS': 300,
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
    log.start(logfile="results.log", loglevel=log.DEBUG, crawler=crawler, logstdout=False)
    reactor.run()

