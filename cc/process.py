from requests import get
from couchdb import Database
from lxml import etree
import json
from StringIO import StringIO
base_meta_url = "http://s3.amazonaws.com/asnstatic/data/manifest/{0}.json"
base_url = "http://asn.jesandco.org/ASNJurisdiction/{0}/feed"

namespaces = {"dc": "http://purl.org/dc/elements/1.1/"}

db = Database("http://localhost:5984/standards")

states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
}


states = {value: key for (key, value) in states.items()}


def add_doc(main_doc):
    if main_doc['_id'] in db:
        del db[main_doc['_id']]
    print(db.save(main_doc))


def process(state_num):
    resp = get(base_url.format(state_num))
    parser = etree.XMLParser(ns_clean=True, recover=True)
    tree = etree.parse(StringIO(resp.content), parser)
    title = tree.xpath("/rss/channel/title").pop().text
    print(title)
    print(states.get(title.strip()))
    main_doc = {
        "_id": states.get(title.strip()),
        "description": title,
        "title": title,
        "children": []
    }
    item_titles = tree.xpath("/rss/channel/item/link", namespaces=namespaces)
    for title in item_titles:
        identifier = title.text[title.text.rfind("/")+1:]
        data = get(base_meta_url.format(identifier)).json()
        main_doc['children'].extend(data)
    add_doc(main_doc)


# for state_num in xrange(111, 161):
#     process(state_num)

title = "Common"
main_doc = {
    "_id": title,
    "description": title,
    "title": title,
    "children": []
}
local_docs = ["math", "english"]
for doc in local_docs:
    with open(doc + ".json", "r+") as f:
        local_data = json.load(f)
    d = {
        "title": doc,
        "children": local_data
    }
    main_doc['children'].append(d)

add_doc(main_doc)
