import sys
import redis
from rdbtools import RdbCallback, JSONCallback, RdbParser
from rediscluster import RedisCluster

targethost = '127.0.0.1'

class MyCallback(RdbCallback):
    _redisclient = None

    def __init(self, string_escape=None):
        super(MyCallback, self).__init__(string_escape)

    def start_database(self, db_number):
        print('=========== start db %s ========' % db_number)
        startup_nodes = [{"host": targethost, "port": "6379"}]
        #self._redisclient = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, db=db_number)
        self._redisclient = redis.Redis(host=targethost, port=6379, db=db_number)

    def set(self, key, value, expiry, info):
        print('%s = %s' % (str(key), str(value)))
        try:
            self._redisclient.set(key, value, expiry)
        except Exception as ex:
            if value:
                print('with value: %s=%s %s'%(str(key), str(value), type(value)))
                self._redisclient.set(key, int(value), expiry)
            else:
                print('no value: %s=%s %s'%(str(key), str(value), type(value)))
                self._redisclient.set(key, str(""))


    def hset(self, key, field, value):
        print('%s.%s = %s' % (str(key), str(field), str(value)))
        self._redisclient.hset(key, field, value)

    def sadd(self, key, member):
        print('%s has {%s}' % (str(key), str(member)))
        self._redisclient.sadd(key, member) 

    def rpush(self, key, value) :
        print('%s has [%s]' % (str(key), str(value)))
        self._redisclient.rpush(key, value) 

    def zadd(self, key, score, member):
        print('%s has {%s : %s}' % (str(key), str(member), str(score)))
        self._redisclient.zadd(key, score, member) 

def main():
    global targethost
    targethost=sys.argv[1]
    for filename in sys.argv[2].split(','):
        parser = RdbParser(MyCallback(None))
        parser.parse(filename)


if __name__ == '__main__':
    main()
