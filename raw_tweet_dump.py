import tweepy, json, sys, datetime, traceback
from tweepy.streaming import StreamListener
import ConfigParser

reload(sys)
sys.setdefaultencoding('utf-8')



class MyStreamListener(StreamListener):
    
    """Sets up connection with OAuth"""
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('settings.cfg')
        self.Consumer_Key=config.get('consumer','Consumer_Key')
        self.Consumer_Secret=config.get('consumer','Consumer_Secret')
        self.Access_Token=config.get('token','Access_Token')
        self.Access_Token_Secret=config.get('token','Access_Token_Secret')
        auth=tweepy.OAuthHandler(self.Consumer_Key, self.Consumer_Secret)
        auth.set_access_token(self.Access_Token, self.Access_Token_Secret)
        self.api=tweepy.API(auth)
    
    def on_status(self, status):
        """called when new status arrives"""
        try:            
            print 4
	            
        except:
            traceback.print_exc(file=sys.stdout)
            print datetime.datetime.now(),
            print sys.exc_info()[0]
           
        

    def on_data(self, data):
         """Called when raw data is received from connection."""
        all_data = json.loads(data)
        write_tweets(all_data)


    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        print datetime.datetime.now(),status_code
        if status_code == 420:
            print '420'
            return False

    def on_limit(self, status):
        """Called when a limitation notice arrives"""
        print 'Limit threshold exceeded', status

    def on_timeout(self, status):
        """Called when stream connection times out"""
        print 'Stream disconnected; continuing...'
        



def write_tweets(tweet):
    """Writes all tweets of a user to a file in json format"""
    bundle_id = tweet['id_str']
    print bundle_id
    f = open('StreamingFiles/' + bundle_id + '.json', 'w')
    json.dump(tweet, f, ensure_ascii=False, indent=4)
    f.close()
    return
        
        
        
class twitterHelper:    
   
    """Sets up connection with OAuth"""
    def __init__(self):
        self.myStreamListener = MyStreamListener()
        auth = tweepy.OAuthHandler(self.myStreamListener.Consumer_Key,self.myStreamListener.Consumer_Secret)
        auth.set_access_token(self.myStreamListener.Access_Token, self.myStreamListener.Access_Token_Secret)
        self.api = tweepy.API(auth)   

    def getStreamTweets(self):
        
        myStream = tweepy.Stream(auth=self.api.auth, listener=self.myStreamListener)
        """give user ids to get the tweets tweeted by them"""
        # @NatlParkService => 36771809  
        user_ids = ['36771809']
        myStream.filter(follow=user_ids, async=True)

if __name__ == '__main__':
    t=twitterHelper()       
    t.getStreamTweets()