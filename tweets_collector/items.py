# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.org/en/latest/topics/items.html


import scrapy

from scrapy.item import Item, Field


class TweetItem(Item):

    QueryStartDate = Field()
    QueryEndDate = Field()
    Query = Field()
    Keyword = Field()
    DateJoined = Field()
    IsMention = Field()
    is_replies_blocked = Field()
    UserID = Field()
    DateOfActivity = Field()
    TimeOfActivity = Field()
    UserScreenName = Field()
    Hashtags = Field()
    Re_tweet = Field()
    NumberOfReplies = Field()
    NumberOfRe_tweets = Field()
    NumberOfFavorites = Field()
    Tweet = Field()
    tweet_id = Field()
    tweet_url = Field()
    is_verified = Field()
    Urls = Field()

    UserFollowersCount = Field()
    UserFollowingCount = Field()
    UserTweetsCount = Field()
    LikesCount = Field()
    isProtected = Field()
    Location = Field()
    Website = Field()

    total_number_of_words_in_the_tweet_including_URL = Field()
    total_number_of_words_in_the_tweet_excluding_URL = Field()

    FileID = Field()





