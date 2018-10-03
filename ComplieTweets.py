import json
import sys
import glob
import numpy
import pandas as pd
pd.options.display.float_format='{:.14}'.format
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

class Compile():
    def __init__(self):
        self.Files=self.ReadFile()
        self.dataframe=self.Sort(self.Files)
        self.dataframeupdated=self.filter(self.dataframe)
        self.CSVWrite(self.dataframeupdated)

    def ReadFile(self):

        return glob.glob("./StreamingFiles/*.json")

    def Sort(self, JsonFileNames):
        print len(JsonFileNames)
        id_str=[]
        replycount=[]
        favorite_count=[]
        retweet_count=[]
        timestamp_ms=[]
        userfollowCount=[]
        userfriendCount=[]
        created_at=[]
        tweettext=[]
        hashtagslist=[]
        mentionslist1=[]
        mentionslistid=[]
        Types=[]
        source_id=[]
        author_screen_name=[]
        author_id=[]
        for filename in JsonFileNames:

            with open(filename) as document:
                documentdata = json.load(document)
                id_str.append(documentdata['id_str'])
                replycount.append(documentdata['reply_count'])
                favorite_count.append(documentdata['favorite_count'])
                retweet_count.append(documentdata['retweet_count'])
                timestamp_ms.append(str(documentdata['timestamp_ms']))
                userfollowCount.append(documentdata['user']['followers_count'])
                userfriendCount.append(documentdata['user']['friends_count'])
                truncated = documentdata['truncated']
                #Date
                created_at.append(str(datetime.datetime.strptime(documentdata['created_at'], "%a %b %d %H:%M:%S +0000 %Y")))
                #Hashtags and Mentions

                if truncated:
                    extended_tweet_dict = documentdata['extended_tweet']
                    tweettext.append(extended_tweet_dict['full_text'])
                    entities = extended_tweet_dict['entities']
                    ##for #tag..............
                    if entities.has_key('hashtags'):
                        hashtags_list = entities['hashtags']  ## in list format from crawler
                        hashtags_list1 = []
                        for h in hashtags_list:
                            ht = h['text']
                            hashtags_list1.append(ht)
                        hashtagslist.append(hashtags_list1)

                    ##for mentions...........
                    if entities.has_key('user_mentions'):
                        mentions_list = entities['user_mentions']  ## in list format from crawler
                        mentions_list1 = []
                        mentions_list_id = []
                        for m in mentions_list:
                            sn = m['screen_name']
                            mentions_list1.append(sn)
                            mentions_list_id.append(m['id_str'])
                        mentionslist1.append(mentions_list1)
                        mentionslistid.append(mentions_list_id)
                else:
                    tweettext.append(documentdata['text'])
                    ##for #tag..............
                    if documentdata['entities'].has_key('hashtags'):
                        hashtags_list = documentdata['entities']['hashtags']  ## in list format from crawler
                        hashtags_list1 = []
                        for h in hashtags_list:
                            ht = h['text']
                            hashtags_list1.append(ht)
                        hashtagslist.append(hashtags_list1)

                    ##for mentions...........
                    if documentdata['entities'].has_key('user_mentions'):
                        mentions_list = documentdata['entities']['user_mentions']
                        mentions_list1 = []
                        mentions_list_id = []
                        for m in mentions_list:
                            sn = m['screen_name']
                            mentions_list1.append(sn)
                            mentions_list_id.append(m['id_str'])
                        mentionslist1.append(mentions_list1)
                        mentionslistid.append(mentions_list_id)

                #******if tweet is a retweet or quoted or reply or tweet*******

                #***********if tweet is a retweet of a TWEET........
                if documentdata.has_key('retweeted_status'):

                    if not isinstance(documentdata['retweeted_status'], dict):
                        documentdata['retweeted_status'] = documentdata['retweeted_status'].__dict__
                    Types.append('retweet')

                    source_id.append(documentdata['retweeted_status']['id_str'])


                #*****if tweet is a "RT of QT" and "QT"******
                if documentdata['is_quote_status']:

                    if not documentdata.has_key('quoted_status'):
                        try:
                           source_id.append(str(documentdata['quoted_status_id_str']))# original tweet ID
                           Types.append('retweet')

                        except:
                            pass


                    #*******if tweet is a retweet of a QTWEET**********
                    if documentdata.has_key('retweeted_status'):

                        if not isinstance(documentdata['retweeted_status'], dict):
                            documentdata['retweeted_status'] = documentdata['retweeted_status'].__dict__
                        Types.append('retweet')

                        source_id.append((documentdata['retweeted_status']['id_str']))

                    else:
                        #*************if tweet is a quoted****************

                        Types.append('QuotedTweet')
                        source_id.append(str(documentdata['quoted_status_id_str'])) # original tweet ID

                #***********************if tweet is a reply******************
                if documentdata['in_reply_to_status_id_str'] is not None:
                    Types.append('Reply')
                    if documentdata.has_key('in_reply_to_status_id_str'):
                        source_id.append((documentdata['in_reply_to_status_id_str']))# original tweet ID
                #***************************only tweet**************************
                elif not (documentdata.has_key('retweeted_status') or documentdata.has_key('quoted_status') or documentdata['is_quote_status']):
                    Types.append('Tweet')
                    source_id.append((documentdata['id_str']))

                #****************AuthorName***********************************
                user_dict = documentdata['user']
                if not isinstance(documentdata['user'], dict):
                    user_dict = documentdata['user'].__dict__

                author_screen_name.append(user_dict['screen_name'])
                author_id.append(str(user_dict['id_str']))



        df1=pd.DataFrame({'tweetid':id_str,'timestampms':timestamp_ms,
                          'userfollowercount':userfollowCount,'userfriendCount':userfriendCount,'createdat':created_at,'hashtagslist':hashtagslist,
                          'mentionslist':mentionslist1,'mentionslistid':mentionslistid,'Types':Types,'sourcetweetid':source_id,'tweetauthorscname':author_screen_name,
                          'authorid':author_id}).set_index('tweetid')
        # df1.to_csv('sample.csv',encoding='utf-8')
        return df1

    def filter(self,dataframe):
        result=[]

        listcols=['hashtagslist','mentionslistid','mentionslist']
        for i in listcols:
            result.append(self.filterhelper(i,dataframe))
        print len(result[0]),len(result[1]),len(result[2])
        df1=pd.DataFrame({'hashtagslist':result[0],'mentionslistid':result[1],'mentionslist':result[2]})
        dataframe['hashtagslist']=df1['hashtagslist'].values
        dataframe['mentionslistid']=df1['mentionslistid'].values
        dataframe['mentionslist']=df1['mentionslist'].values
        return dataframe
    def filterhelper(self,colname,dataframe):
        newCol=[]
        for i in dataframe.loc[:,colname]:
            if i!=[]:
                strg=json.dumps(i)
                newCol.append(strg.replace(',', '|').replace('[', '').replace(']', '').replace(' ', '').replace('"', ''))
            else:
                newCol.append(None)
        return newCol


    def CSVWrite(self,dataframe):
        # print dataframe
        dataframe.to_csv('Updated.csv', encoding='utf-8',sep=',')
        print 'Complied'








if __name__=='__main__':
    Compile()