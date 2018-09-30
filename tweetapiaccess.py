import tweepy
import json
import ConfigParser


class TwitterConnect():
	def __init__(self):
		config = ConfigParser.RawConfigParser()
		config.read('C:\Users\Jubanjan\PycharmProject\Tweets\settings.cfg')
		self.Consumer_Key=config.get('consumer','Consumer_Key')
		self.Consumer_Secret=config.get('consumer','Consumer_Secret')
		self.Access_Token=config.get('token','Access_Token')
		self.Access_Token_Secret=config.get('token','Access_Token_Secret')
		self.UserID=config.get('user','userid')
		self.tweetlimit=config.get('user','tweetlimit')
		self.Authenticate()
	

	def Authenticate(self):
		auth= tweepy.OAuthHandler(self.Consumer_Key,self.Consumer_Secret)
		auth.set_access_token(self.Access_Token,self.Access_Token_Secret)
		self.api=tweepy.API(auth)


	def getData(self):
		data=self.api.user_timeline(self.UserID,count=self.tweetlimit)
		for i in data:
			tweetid= str(i._json['id'])
			f = open('SearchRest/' + tweetid + '.json', 'w')
			json.dump(i._json, f, indent=4)
			f.close()
		print 'done'





if __name__=='__main__':
	api=TwitterConnect()
	api.getData()