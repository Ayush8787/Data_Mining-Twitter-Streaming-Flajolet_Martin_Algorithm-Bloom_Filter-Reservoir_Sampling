import tweepy, sys, collections
import random

api_key = "9APZth1EclAD2EmuFYUoyIOZ4"
api_key_secret = "xpZB1kkK43okihnDy58TQxFR6UrihO83doKYqxo3qKsTZGie64"
access_token = "764686249739169792-h8puG4XQCdbbSqXXkwOJmcd7eXwftQn"
access_token_secret = "hA6mvWqn0kcSvgbClOqA4gXc3l69l0U0Je9EptPwpTQHK"

tt = sys.argv[1]
output = sys.argv[2]

auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


class MyStreamListener(tweepy.StreamListener):
    inc = 0
    gl = []
    counter_dic = {}
    trace_back = {}
    index = {}
    inc_tweet = 0

    def random_index_generator(self, x):
        return random.randint(0, x-1)

    def on_status(self, temp):
        tags = temp.entities.get("hashtags")
        if self.inc <= 100:
            if tags:
                self.inc_tweet += 1
                for i in tags:
                    tag = i.get("text")
                    self.inc = self.inc + 1
                    if tag not in self.counter_dic.keys():
                        self.counter_dic[tag] = 1
                        # self.index[self.inc] = tag
                        self.gl.append(tag)
                    else:
                        # self.index[self.inc] = tag
                        self.counter_dic[tag] += 1

                outfile = open(output, "a", encoding="utf-8")
                result = sorted(self.counter_dic.items(), key=lambda kv: (-kv[1], kv[0]))
                values = sorted(set(self.counter_dic.values()), reverse=True)
                temp_qw = []
                c = 0
                for i in values:
                    if c == 3:
                        break
                    temp_qw.append(i)
                    c += 1
                outfile.write("The number of tweets with tags from the beginning: {}\n".format(self.inc_tweet))
                for k in result:
                    if k[1] in temp_qw:
                        outfile.write(k[0] + " : " + str(k[1]) + "\n")
                outfile.write("\n")
                outfile.close()
        else:
            if tags:
                self.inc_tweet += 1
                pr = float(100.0 / self.inc)
                if pr > random.random():
                    ind = self.random_index_generator(len(self.gl))
                    tag_1 = self.gl[ind]
                    if self.counter_dic[tag_1] == 1:
                        self.counter_dic.pop(tag_1, None)
                        self.gl.remove(tag_1)
                        self.inc = self.inc - 1
                    else:
                        self.counter_dic[tag_1] -= 1
                        self.inc = self.inc - 1
                    for jj in tags:
                        tag_2 = jj.get("text")
                        self.inc = self.inc + 1
                        if tag_2 not in self.counter_dic.keys():
                            self.counter_dic[tag_2] = 1
                            self.gl.append(tag_2)
                            # self.index[self.inc] = tag_2
                        else:
                            self.counter_dic[tag_2] += 1
                            # self.index[self.inc] = tag_2
                        break
                outfile = open(output, "a", encoding="utf-8")
                result = sorted(self.counter_dic.items(), key=lambda kv: (-kv[1], kv[0]))
                values = sorted(set(self.counter_dic.values()), reverse=True)
                temp_qw=[]
                c = 0
                for i in values:
                    if c == 3:
                        break
                    temp_qw.append(i)
                    c += 1
                outfile.write("The number of tweets with tags from the beginning: {}\n".format(self.inc_tweet))
                for k in result:
                     if k[1] in temp_qw:
                         outfile.write(k[0] + " : " + str(k[1]) + "\n")
                outfile.write("\n")
                outfile.close()



myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
myStream.filter(track=["trump"], languages=['en'])
