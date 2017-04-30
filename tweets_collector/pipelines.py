import os
import unicodecsv as csv

class SaveItem(object):

    def __init__(self):
        self.tweets_monitor = {}

    def csv_dict_writer(self, path, fieldnames, data):
        """
        Writes a CSV file using DictWriter
        """

        add_header = True
        if os.path.exists(path):
            add_header = False

        with open(path, "ab") as out_file:
            writer = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
            if add_header:
                writer.writeheader()
            writer.writerow(data)

    def process_item(self, item, spider):

        keyword = item['Keyword']
        tweet_id = item['tweet_id']

        tweet = item['Tweet'].replace('\r', ' ')
        tweet = tweet.replace('\n', ' ').strip()

        add_tweet = False

        if keyword in self.tweets_monitor and tweet_id not in self.tweets_monitor[keyword]:

            add_tweet = True

            self.tweets_monitor[keyword].add(tweet_id)

        elif keyword not in self.tweets_monitor:

            add_tweet = True

            self.tweets_monitor[keyword] = set()

            self.tweets_monitor[keyword].add(tweet_id)

        else:

            print 'asdf'

        if add_tweet:
            headers = ['QueryStartDate','QueryEndDate','Query','Keyword','DateJoined','IsMention','is_replies_blocked','UserID','DateOfActivity','TimeOfActivity','UserScreenName','Hashtags','Re_tweet','NumberOfReplies','NumberOfRe_tweets','NumberOfFavorites','Tweet','tweet_id','tweet_url','is_verified','Urls','UserFollowersCount','UserFollowingCount','UserTweetsCount','LikesCount','isProtected','Location','Website','total_number_of_words_in_the_tweet_including_URL','total_number_of_words_in_the_tweet_excluding_URL','FileID']
            file_name = item['FileID'] + '.csv'
            self.csv_dict_writer(file_name, headers, item)
            return item