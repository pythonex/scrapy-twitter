# -*- coding: utf-8 -*-

import re
import copy
import json
import csv
from urllib import urlencode
from urlparse import urlunparse

import copy
from scrapy import log
from scrapy import signals
from datetime import datetime, timedelta
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http.request import Request
from tweets_collector.items import TweetItem
from scrapy.xlib.pydispatch import dispatcher

class LeadsScraper(Spider):

    name = 'TwitterScr'
    allowed_domains = ['twitter.com']
    base_url = 'http://www.twitter.com/'
    keywords_file_name = 'input_keywords.txt'
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'en-US,en;q=0.5',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0'}

    # Specify a user agent to prevent Twitter from returning a profile card
    headers2 = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36'}

    max_tweets_no = 5000
    page_count = 0

    min_tweet_dict = {}

    no_of_followers = {}
    account_visited = set()

    search_type = 'within_tweets'

    keywords_file = 'n/a'

    keywords_dict = {}

    full_keywords_file = 'inputs.csv'

    def __init__(self, *args, **kwargs):
        super(LeadsScraper, self).__init__(*args, **kwargs)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        self.log_something('Done')

    tweet_start_date = datetime.now()
    tweet_end_date = datetime.now()

    def start_requests(self):
        yield Request(url='http://www.twitter.com/',
                      headers=self.headers,
                      callback=self.get_tweets_ids_in_range)

    def get_tweets_ids_in_range(self, response):

        with open(self.full_keywords_file, 'rb') as f:
            reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_ALL)
            for row in reader:

                if '"search"' in row:
                    row_key = row['"search"'].replace('"""', '"')
                elif 'search' in row:
                    row_key = row['search'].replace('"""', '"')
                row_key = row_key.replace('""', '"')
                self.keywords_dict[row_key] = row
                print row

        keywords_counter = 0

        for keyword, v in self.keywords_dict.iteritems():

            self.log_something('{{{{<<<<>>>>>}}}}Starting with keyword::' + keyword)

            keywords_counter += 1

            modified_keyword = ''
            twitter_accounts_in_keyword = re.findall('@*[#$%a-zA-Z0-9_+\s]{1,15}', keyword)
            for acc in twitter_accounts_in_keyword:

                if modified_keyword == '':
                    modified_keyword = acc
                else:
                    modified_keyword = modified_keyword + ' OR ' + acc

            if len(twitter_accounts_in_keyword) == 0: # no @, it is a keyword not a user name:
                modified_keyword = keyword

            self.log_something('>Keyword:' + modified_keyword)

            modified_keyword = modified_keyword.replace('"', '')

            search_twitter_url = self.construct_url(modified_keyword)

            search_twitter_url = search_twitter_url.replace('__', '%20')  # for now double underscore means one whitespace.

            tweet_start_date = v['start_date']
            tweet_end_date = v['end_date']

            tweet_start_date = datetime.strptime(tweet_start_date, '%d/%m/%Y')
            tweet_start_date = tweet_start_date.strftime('%Y-%m-%d')

            tweet_end_date = datetime.strptime(tweet_end_date, '%d/%m/%Y')
            tweet_end_date = tweet_end_date.strftime('%Y-%m-%d')

            yield Request(url=search_twitter_url,
                          headers=self.headers2,
                          meta={'search_id': v['id'],
                                'request_start_date': copy.deepcopy(tweet_start_date),
                                'request_end_date': copy.deepcopy(tweet_end_date),
                                'query': modified_keyword,
                                'keyword': keyword,
                                'search_twitter_url': search_twitter_url,
                                'first_round': 'OK'},
                          callback=self.search_page)

    def search_page(self, response):
        query = response.meta['query']
        keyword = response.meta['keyword']
        first_round = response.meta['first_round']
        data = json.loads(response.body)
        html_source = data['items_html'].strip()
        parse_json_data = self.parse_tweets(html_source)
        search_twitter_url = response.meta['search_twitter_url']

        b_meta = copy.deepcopy(response.meta)
        b_meta['first_round'] = 'Nope'

        # if only this is the first round for this keyword, that we did not get 20 tweets per request.
        if first_round == 'OK' and type(parse_json_data) is list and len(parse_json_data) < 20:
        #if first_round == 'OK' and parse_json_data and len(parse_json_data) < 20:
            next_end_date = datetime.strptime(response.meta['request_end_date'], '%Y-%m-%d') + timedelta(days=1)
            next_end_date = next_end_date.strftime('%Y-%m-%d')
            self.log_something('End Date for %s incremented to ' % keyword + next_end_date)
            search_twitter_url = re.sub('until%3A\d+\-\d+\-\d+&',
                                        'until%3A' + next_end_date + '&',
                                        search_twitter_url)
            yield Request(url=search_twitter_url,
                          headers=self.headers2,
                          dont_filter=True,
                          meta={'request_start_date': copy.deepcopy(response.meta['request_start_date']),
                                'request_end_date': copy.deepcopy(next_end_date),
                                'query': query,
                                'keyword': keyword,
                                'search_twitter_url': search_twitter_url,
                                'first_round': False,
                                'search_id': response.meta['search_id']},
                          callback=self.search_page)
        else:
        #if len(parse_json_data) == 20:
            items_list = []
            if parse_json_data and len(parse_json_data) > 0:
                for json_row in parse_json_data:
                    tweet_id = json_row['tweet_id']
                    itm = self.create_item_object(json_row, keyword, False, query, response)
                    if itm:
                        items_list.append(copy.deepcopy(itm))

                self.log_something('>Tweets Number:' + str(len(parse_json_data)))
                if query not in self.min_tweet_dict:
                    self.min_tweet_dict[query] = parse_json_data[0]

                # has_more_items = data['has_more_items']
                # if has_more_items:
                max_tweet = parse_json_data[-1]
                max_tweet_id = max_tweet['tweet_id']
                min_tweet_id = self.min_tweet_dict[query]['tweet_id']
                if min_tweet_id is not max_tweet_id:
                    max_position = "TWEET-%s-%s" % (max_tweet_id, min_tweet_id) + '-BD1UO2FFu9QAAAAAAAAETAAAAAcAAAASAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
                    #self.log_something('>>>>>>>>>>>>>> max_position::' + max_position + ' - min_tweet_id:' + min_tweet_id)
                    url = self.construct_url(query, max_position=max_position)
                    # Sleep for our rate_delay
                    # sleep(self.rate_delay)
                    yield Request(url=url,
                                  headers=self.headers2,
                                  meta=b_meta,
                                  callback=self.search_page)
                else:
                    self.log_something(' ' + query)
            else:
                self.log_something('No tweets found for query: ' + query + ' - query_url: ' + response.url)

            for item in items_list:
                tweet_url = item['tweet_url']
                meta = copy.deepcopy(response.meta)
                meta['itm'] = item
                meta['itms_no'] = str(len(items_list))
                yield Request(url=tweet_url,
                              headers=self.headers,
                              meta=meta,
                              callback=self.get_replies)

    def create_item_object(self, tweet, keyword, is_mention, query, response):

        item = TweetItem()

        user_id = unicode(tweet['user_id'])
        tweet_text = unicode(tweet['text'])
        if '"' in tweet_text:
            tweet_text = tweet_text.replace('"', "'")
        tweet_text = re.sub('[^\x00-\x7f]', '', tweet_text).strip()

        user_screen_name = unicode(tweet['user_screen_name'])

        retweets = unicode(tweet['retweets'])
        favorites = unicode(tweet['favorites'])
        user_name = unicode(tweet['user_name'])

        tweet_id = unicode(tweet['tweet_id'])
        account_name = u'@' + unicode(user_screen_name)
        tweet_url = 'https://twitter.com/%s/status/%s' % (account_name.replace('@', ''), tweet_id)

        #created_at = unicode(tweet['created_at'])
        t = datetime.fromtimestamp((tweet['created_at']/1000))
        fmt = "%Y-%m-%d %H:%M:%S"
        created_at = t.strftime(fmt)
        fmt_2 = '%d/%m/%Y'
        created_at_obj = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
        tweet_date_str = created_at_obj.strftime('%Y%m%d')
        tweet_date = datetime.strptime(tweet_date_str, '%Y%m%d')
        tweet_time_str = created_at_obj.strftime('%H:%M:%S')

        exclude_by_search_type_filter = False
        all_accouts_exists = True
        twitter_accounts_in_keyword = re.findall('@[a-zA-Z0-9_]{1,15}', keyword)
        if twitter_accounts_in_keyword and len(twitter_accounts_in_keyword) > 0:
            for c in twitter_accounts_in_keyword:
                if c not in tweet_text:
                    all_accouts_exists = False
                    #exclude_by_search_type_filter = False

        #all_hash_tags_exists = True
        hashtags = re.findall('#[a-zA-Z0-9_]{1,15}', tweet_text)
        if hashtags and len(hashtags) > 0:
            item[u'Hashtags'] = ','.join(hashtags)

        if not exclude_by_search_type_filter:

            item['NumberOfReplies'] = '0'
            item['FileID'] = response.meta['search_id']
            item['Query'] = query
            item['DateOfActivity'] = tweet_date_str
            item['TimeOfActivity'] = tweet_time_str
            item['QueryStartDate'] = response.meta['request_start_date']
            item['QueryEndDate'] = response.meta['request_end_date']
            item['UserScreenName'] = account_name
            item['Tweet'] = tweet_text
            item['Keyword'] = unicode(keyword.replace('__', ' '))
            item['tweet_id'] = tweet_id
            item['tweet_url'] = tweet_url
            item['UserID'] = user_id

            is_retweet = 'No'
            if tweet_text.startswith('RT'):
                is_retweet = 'Yes'
            if 'via %s' % keyword.lower() in tweet_text.lower():
                is_retweet = 'Yes'

            # Need to be check out...
            item['Re_tweet'] = is_retweet
            item['NumberOfRe_tweets'] = retweets
            item['NumberOfFavorites'] = favorites
            item['IsMention'] = '1' if is_mention else '0'
            item['is_verified'] = 'False'

            return item

    accounts_meta = {}

    def get_replies(self, response):

        sel = Selector(response)
        item = response.meta['itm']

        date_joined = re.findall('>Joined\s(\w+\s\d{4})</span>', response.body)
        if date_joined and len(date_joined) > 0:
            date_joined = date_joined[0]
        else:
            date_joined = ''

        is_verifed = sel.xpath('.//div[@class="permalink-header"]/descendant::*/text()')
        if is_verifed and len(is_verifed) > 0:
            is_verifed = is_verifed.extract()
            if any(x.lower().strip() == u'verified account' for x in is_verifed):
                item['is_verified'] = 'True'

        if item['UserScreenName'].lower() in item['Tweet'].lower():
            item['IsMention'] = '1'

        proxy = response.meta['proxy'] if 'proxy' in response.meta else '<<< NO PROXIES >>>'

        self.page_count += 1
        items_no = response.meta['itms_no']

        replies = sel.xpath('.//li[starts-with(@id, "stream-item-tweet-")]')
        if replies and len(replies) > 0:
            replies = str(len(replies.extract()))
        else:
            replies = '0'

        item['NumberOfReplies'] = replies
        item['DateJoined'] = date_joined

        item['total_number_of_words_in_the_tweet_including_URL'] = len(item['Tweet'])
        item['total_number_of_words_in_the_tweet_excluding_URL'] = len(item['Tweet'])

        user_screen_name = item['UserScreenName']

        tweets_links = response.xpath('//div[@class="js-tweet-text-container"]/.//a/@href')
        if tweets_links and len(tweets_links) > 0:
            tweets_links = tweets_links.extract()
            tweets_links = [x for x in tweets_links if x[0] != '/']
            if len(tweets_links) > 0:
                item['Urls'] = tweets_links
                tweet_escaped = item['Tweet'].replace('\n', ' ')
                tweet_without_links = re.sub('http[:./\w]+', '', tweet_escaped)
                tweet_without_links = re.sub('http[:./\w]+', '', tweet_escaped)
                tweet_without_links = tweet_without_links.strip()
                tweet_without_links_len = len(tweet_without_links)
                item['total_number_of_words_in_the_tweet_excluding_URL'] = tweet_without_links_len

        if user_screen_name not in self.accounts_meta:
            yield Request(url='https://twitter.com/' + user_screen_name.replace('@', ''),
                          headers=self.headers,
                          meta=response.meta,
                          callback=self.get_no_of_followers)
        else:
            user_meta = self.accounts_meta[user_screen_name]
            item['UserFollowersCount'] = user_meta['UserFollowersCount']
            item['UserFollowingCount'] = user_meta['UserFollowingCount']
            item['UserTweetsCount'] = user_meta['UserTweetsCount']
            item['LikesCount'] = user_meta['LikesCount']

            yield item

    def get_no_of_followers(self, response):
        itm = response.meta['itm']
        company_account = itm['UserScreenName'].lower()

        location = response.xpath('//span[@class="ProfileHeaderCard-locationText u-dir"]/text()')
        if location and len(location) > 0:
            location = location.extract()[0].strip()
        else:
            location = ''
        itm['Location'] = location

        website = response.xpath('//span[@class="ProfileHeaderCard-urlText u-dir"]/a/text()')
        if website and len(website) > 0:
            website = website.extract()[0].strip()
        else:
            website = ''
        itm['Website'] = website

        followers_no = response.xpath('.//li[@class="ProfileNav-item ProfileNav-item--followers"]/a/span[@class="ProfileNav-value"]/text()')
        if followers_no and len(followers_no) > 0:
            followers_no = followers_no.extract()[0].strip()
        else:
            followers_no = ''

        following_no = response.xpath('.//li[@class="ProfileNav-item ProfileNav-item--following"]/a/span[@class="ProfileNav-value"]/text()')
        if following_no and len(following_no) > 0:
            following_no = following_no.extract()[0].strip()
        else:
            following_no = ''

        tweets_count = response.xpath('//a[@data-nav="tweets"]/@title')
        if tweets_count and len(tweets_count) > 0:
            tweets_count = tweets_count.extract()[0].replace('Tweets', '').strip()
        else:
            tweets_count = ''

        likes_count = response.xpath('.//li[@class="ProfileNav-item ProfileNav-item--favorites"]/a/span[@class="ProfileNav-value"]/text()')
        if likes_count and len(likes_count) > 0:
            likes_count = likes_count.extract()[0].strip()
        else:
            likes_count = ''

        itm['UserFollowersCount'] = followers_no
        itm['UserFollowingCount'] = following_no
        itm['UserTweetsCount'] = tweets_count
        itm['LikesCount'] = likes_count

        self.accounts_meta[company_account] = {'UserFollowersCount': followers_no,
                                               'UserFollowingCount': following_no,
                                               'UserTweetsCount': tweets_count,
                                               'LikesCount': likes_count}
        yield itm

    def log_something(self, line):
        log.msg('LOG::' + line, log.INFO)

    def construct_url(self, query, max_position=None):
        """
        For a given query, will construct a URL to search Twitter with
        :param query: The query term used to search twitter
        :param max_position: The max_position value to select the next pagination of tweets
        :return: A string URL
        """

        params = {
            # Type Param
            #'f': 'realtime',
            'f': 'tweets',
            # Query Param
            'q': query,
            'include': 'retweets',
            'src': 'typd',
            'vertical': 'default'
        }

        # If our max_position param is not None, we add it to the parameters
        if max_position is not None:
            params['max_position'] = max_position

        url_tupple = ('https', 'twitter.com', '/i/search/timeline', '', urlencode(params), '')

        xxx = urlunparse(url_tupple)

        return xxx

    def parse_tweets(self, page_source):
        """
        Parses Tweets from the given HTML
        :param items_html: The HTML block with tweets
        :return: A JSON list of tweets
        """

        tweets = []
        sel = Selector(text=page_source)
        tweets_list = sel.xpath('.//li[starts-with(@class, "js-stream-item")]')
        if tweets_list and len(tweets_list) > 0:
            for tweet_object in tweets_list:

                tweet_id = tweet_object.xpath('@data-item-id')
                if tweet_id and len(tweet_id) > 0:
                    tweet_id = tweet_id.extract()[0].strip()
                else:
                    self.log_something('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Cannot find tweet id')
                    continue

                tweet = {
                    'tweet_id': tweet_id,
                    'text': None,
                    'user_id': None,
                    'user_screen_name': None,
                    'user_name': None,
                    'created_at': None,
                    'retweets': 0,
                    'favorites': 0
                }

                # Tweet Text
                tweet_text = tweet_object.xpath('.//p[contains(@class, "tweet-text")]/.//text()')
                if tweet_text and len(tweet_text) > 0:
                    tweet_text = tweet_text.extract()
                    tweet_text = ''.join(tweet_text)
                    tweet['text'] = tweet_text.strip()

                # Tweet date
                tweet_date = tweet_object.xpath('.//span[starts-with(@class, "_timestamp")]/@data-time-ms')
                if tweet_date and len(tweet_date) > 0:
                    tweet_date = tweet_date.extract()[0].strip()
                    tweet['created_at'] = int(tweet_date)

                # Tweet User ID, User Screen Name, User Name
                tweet_metadata = tweet_object.xpath('.//div[starts-with(@class, "tweet")]')
                if tweet_metadata and len(tweet_metadata) > 0:
                    data_user_id = tweet_metadata.xpath('@data-user-id').extract()[0]
                    data_screen_name = tweet_metadata.xpath('@data-screen-name').extract()[0]
                    data_name = tweet_metadata.xpath('@data-name').extract()[0]

                    tweet['user_id'] = data_user_id
                    tweet['user_screen_name'] = data_screen_name
                    tweet['user_name'] = data_name

                # Tweet Retweets
                retweet_span = tweet_object.css('span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount')
                if retweet_span and len(retweet_span) > 0:
                    retweet_span = retweet_span.xpath('@data-tweet-stat-count').extract()[0]
                    tweet['retweets'] = int(retweet_span)

                # Tweet Favourites
                favorite_span = tweet_object.css('span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount')
                if favorite_span and len(favorite_span) > 0:
                    favorite_span = favorite_span.xpath('@data-tweet-stat-count').extract()[0]
                    tweet['favorites'] = int(favorite_span)

                tweets.append(tweet)

            return tweets