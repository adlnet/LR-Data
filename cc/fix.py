from requests import get
from couchdb import Database
db = Database("http://localhost:5985/lr-data")

split_on = ", supported by"

page = 0

url = "http://12.109.40.31/search?terms=grade&page={0}"

data = get(url.format(page)).json()


while len(data) > 0:
    for item in data:
        if item['publisher'] is not None and split_on in item['publisher']:
            parts = [x.strip() for x in item['publisher'].split(split_on)]
            if parts[0] == parts[1]:
                print(parts)
                doc = db[item['_id']]
                item['publisher'] = parts[0]
                doc.update(item)
                print(db.save(doc))
    page += 1
    data = get(url.format(page)).json()
