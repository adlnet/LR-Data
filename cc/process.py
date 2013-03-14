import json
import couchdb


with open('math.json') as f:
    math_data = json.load(f)

with open('english.json') as f:
    english_data = json.load(f)


db = couchdb.Database("http://localhost:5985/standards")

math_doc = db['math']

math_doc['children'] = math_data

english_doc = db['english']

english_doc['children'] = english_data

db.save(math_doc)
db.save(english_doc)
