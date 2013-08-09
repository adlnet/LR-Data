import requests
import nltk
from nltk import PorterStemmer
import json
from pprint import pprint
from ftplib import FTP
import itertools
stemmer = PorterStemmer()
stop_words = []
ftp = FTP('ftp.cs.cornell.edu')
ftp.login()
ftp.cwd('/pub/smart/')
ftp.retrlines("RETR english.stop", stop_words.append)
ftp.quit()
resp = requests.get('http://node01.public.learningregistry.net/obtain?request_ID=http://www.phschool.com/iText/math/sample_chapter/Ch02/02-01/PH_Alg1_ch02-01_Obj1.html', stream=False)
raw_items = json.loads(resp.content)
items = (i for i in itertools.chain.from_iterable((x['document'] for x in raw_items['documents'])))
raw_keys = (i.lower() for i in itertools.chain.from_iterable(x['keys'] for x in items))
keys = (i for i in itertools.chain.from_iterable(nltk.tokenize.wordpunct_tokenize(x) for x in raw_keys) if i not in stop_words and len(i) > 1)
pprint(list(keys))
#print nltk.pos_tag(items)
