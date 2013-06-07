from redis import StrictRedis
import redis
import couchdb
from pprint import pprint
import math
import mincemeat
import json
db = couchdb.Database("http://localhost:5984/lr-data")
r = StrictRedis(db=1)

def count_map(k, v):
    yield v


def count_reduce(k, vs):    
    with open("counts/" + k, "w+") as f:
        f.write(str(sum(vs)))
    return sum(vs)


def process_keys():
    for n in xrange(ord('a'), ord('x')):
        query = chr(n) + "*"
        for k in r.keys(query):
            try:
                for (key, value) in r.zrevrange(k, 0, -1, "WITHSCORES"):
                    yield k, key, value
            except redis.exceptions.ResponseError:
                pass


s = mincemeat.Server()

s.datasource = {k: (d, v) for k, d, v in process_keys()}
s.mapfn = count_map
s.reducefn = count_reduce
print("Start Workers")
s.run_server(password="password")

def tfidf_map(k, v):
    yield k, v

def tfidf_reduce(k, vs):
    from redis import StrictRedis
    import redis
    import couchdb    
    import json
    import math
    db = couchdb.Database("http://localhost:5984/lr-data")
    r = StrictRedis(db=0)
    doc_count = len(db)
    counts = None
    def freq(word, doc_id):
        return r.zscore(word, doc_id)

    def word_count(doc_id):        
        with open("counts/" + doc_id, "r+") as f:
            return float(f.read())

    def num_docs_containing(word):
        return r.zcard(word)

    def tf(word, doc_id):
        return (freq(word, doc_id) / float(word_count(doc_id)))

    def idf(word):
        return math.log(doc_count / float(num_docs_containing(word)))

    def tf_idf(word, doc_id):
        return (tf(word, doc_id) * idf(word))    
    key, doc_id = k
    if doc_id not in db:
        return
    doc = db[doc_id]
    multiplier = 1
    try:
        if key in doc['title'].lower():
            multiplier = 2
            print("key in title, double the power")
    except:
        pass
    rank = tf_idf(key, doc_id) * multiplier
    if rank is None :
        rank = 0
    r.zadd(key, rank, doc_id)
    return rank

# s = mincemeat.Server()

# s.datasource = {(k, d): v for k, d, v in process_keys()}
# s.mapfn = tfidf_map
# s.reducefn = tfidf_reduce
print("Start Workers")
# results = s.run_server(password="password")