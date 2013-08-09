from math import log
from celeryconfig import config
from redis import StrictRedis
from redis.exceptions import ResponseError
from pprint import pprint


def doc_count(client):
    doc_ids = set()
    key_counts = {}
    for k in client.keys("*"):
        try:
            count = 0
            docs = client.zrevrange(k, 0, -1)
            for d in docs:
                doc_ids.add(d)
                count += 1
            key_counts[k.decode("utf-8")] = count
        except ResponseError:
            pass
    return len(doc_ids), key_counts


def caluclate_idf(doc_count, terms_with_count):
    term_ids = {}
    for k, v in terms_with_count.items():
        term_ids[k] = log(doc_count/v, 2)
    return term_ids


def calculate_tf_ids(client, idfs):
    for k in client.keys("*"):
        try:
            docs = client.zrevrange(k, 0, -1, "WITHSCORES")
            for d in docs:
                tf_idf = log(d[1] + 1) * idfs[k.decode("utf-8")]
                print(client.zadd(k, tf_idf, d[0]))
        except ResponseError:
            pass

if __name__ == "__main__":
    r = StrictRedis(host=config['redis']['host'],
                    port=config['redis']['port'],
                    db=config['redis']['db'])
    total_count, counts = doc_count(r)
    idfs = caluclate_idf(total_count, counts)
    calculate_tf_ids(r, idfs)
