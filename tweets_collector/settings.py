# -*- coding: utf-8 -*-

# Scrapy settings for food project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'twitter'

SPIDER_MODULES = ['tweets_collector.spiders']
NEWSPIDER_MODULE = 'tweets_collector.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent

RAW_PROXIES = list()

PROXIES = list()

def loadFileIntoLIst(fileName):
    with open(fileName) as f:
        _proxies = f.read().splitlines()
    return _proxies

RAW_PROXIES = loadFileIntoLIst('proxies.txt')

for proxy in RAW_PROXIES:
    if '@' in proxy:
        proxy_logins = proxy.split('@')
        ip_port = proxy_logins[0]
        user_password = proxy_logins[1]
        itm = {'ip_port': ip_port,
               'user_pass': user_password}
    else:
        itm = {'ip_port': proxy}

    PROXIES.append(itm)
RAW_PROXIES = [x.replace('@', ':') for x in RAW_PROXIES]

if len(PROXIES) > 0:
    DOWNLOADER_MIDDLEWARES = {
        #'tweets_collector.middlewares.ProxyMiddleware': 110,
    }

ITEM_PIPELINES = {
    'tweets_collector.pipelines.SaveItem': 300,
}

LOG_LEVEL = 'DEBUG'