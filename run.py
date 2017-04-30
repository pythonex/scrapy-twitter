#!/usr/bin/python

import datetime
from scrapy import cmdline

today = str(datetime.date.today()).replace('-', '')

list_name = 'tweets_'.replace(' ', '_') + today + '.csv'

name = 'TwitterScr'

if __name__ == '__main__':

    command = u"scrapy crawl {0}".format(name).split()
    cmdline.execute(command)