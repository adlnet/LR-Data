# -*- coding: utf-8 -*-
from tasks.save import *
from celeryconfig import config
import requests
import unittest
from pprint import pprint

class DisplayTests(unittest.TestCase):
    def test_khan(self):
        url = "http://sandbox.learningregistry.org/obtain?request_id=http://www.khanacademy.org/video/dependent-probability-example-1?playlist=Probability"
        data = requests.get(url).json()
        data = data['documents'][0]['document'][0]
        createRedisIndex(data, config)
    def test_stringy_json(self):
    	url = "http://node01.public.learningregistry.net/obtain?request_id=286dc2ef744842c7bd7083cb44b1e5ed&by_doc_ID=T"
    	data = requests.get(url).json()
        data = data['documents'][0]['document'][0]
        createRedisIndex(data, config)

if __name__ == "__main__":
    unittest.main()
