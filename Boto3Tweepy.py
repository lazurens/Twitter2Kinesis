#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import boto3
import json

#Variables that contains the user credentials to access Twitter API
access_token = "-"
access_token_secret = "-"
consumer_key = "-"
consumer_secret = "-"

#Listener that pushes data to kinesis and prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        tweets = []
        jsonItem = json.dumps(data)
        tweets.append({'Data': jsonItem, 'PartitionKey': "filler"})
        kinesis.put_records(StreamName="twitter", Records=tweets)
        tweets = []
        print("item sent to kinesis stream..")
        print (data)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':
    client = boto3.client('kinesis', region_name="us-east-1",
                          aws_access_key_id='-',
                          aws_secret_access_key='-')

    kinesis = boto3.client('kinesis', region_name="us-east-1",
                           aws_access_key_id='-',
                           aws_secret_access_key='-')

    # response = client.create_stream(
    #     StreamName='twitter',
    #     ShardCount=1
    # )
    print("kinesis stream initiated ")
    #Handling Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    #ftweets = stream.filter(track=['python', 'javascript', 'ruby'])
    stream.filter(track=['protest'])



    # kinesis = boto3.client('kinesis', region_name="us-east-1",
    #                        aws_access_key_id='AKIAIQBSVCBK7NSDWCFQ',
    #                        aws_secret_access_key='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')
    #
    # response = client.create_stream(
    #     StreamName='twitter',
    #     ShardCount=1
    # )
