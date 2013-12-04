from requests import get
from couchdb import Database
from pyquery import PyQuery as pq
import json
base_meta_url = "http://s3.amazonaws.com/asnstatic/data/manifest/{0}.json"
base_url = "http://asn.jesandco.org/resources/ASNJurisdiction/{0}"

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
keys_to_remove = [unicode(x) for x in ['leaf', 'dcterms_language', "text",
                  'dcterms_educationLevel', 'skos_exactMatch', "asn_localSubject",
                  'dcterms_description', 'dcterms_subject',
                  'asn_indexingStatus', 'asn_authorityStatus',
                  'asn_statementLabel', 'asn_statementNotation',
                  'asn_altStatementNotation', 'cls', 'asn_comment']]


def process_doc(doc):
    doc['count'] = 0
    if "asn_identifier" in doc:
        if 'uri' in doc['asn_identifier']:
            doc['id'] = doc['asn_identifier']['uri'].strip()
        else:
            doc['id'] = doc['asn_identifier'].strip()
    if 'id' in doc:
        url = doc['id']
        doc['id'] = url[url.rfind("/")+1:].lower()
    if "text" in doc:
        doc['title'] = doc['text']
    for key in keys_to_remove:
        if key.strip() in doc:
            del doc[key]
    if "children" in doc:
        for child in doc['children']:
            process_doc(child)


def add_doc(main_doc):
    if main_doc['_id'] in db:
        del db[main_doc['_id']]
    print(db.save(main_doc))


def process(state):
    d = pq(url=base_url.format(state))
    title = state
    main_doc = {
        "_id": title,
        "description": title,
        "title": title,
        "children": []
    }
    names = d("td.views-field-field-dcterms-subject-value")
    links = d("td.views-field-markup a")
    for entry in zip(names, links):
        name = entry[0].text.strip()
        url = entry[1].attrib['href']
        if url.endswith('json'):
            child_doc = {
                "description": name,
                "title": name,
                "children": get(url).json()
            }
            main_doc['children'].append(child_doc)
    process_doc(main_doc)
    add_doc(main_doc)

for state_num in states.keys():
    process(state_num)

title = "Common"
main_doc = {
    "_id": title,
    "description": "Multistate",
    "title": "Multistate",
    "children": []
}
local_docs = [("Common Core Mathmatics", "math.json"), 
              ("National Standards for Arts Education", "D10003BC_manifest.json"),
              ("National Council of Teachers of Mathematics", "D100000A_manifest.json"),
              ("National Center for History in the Schools", "D10003BD_manifest.json"),
              ("National Geography Education Standards", "D100026F_manifest.json"),
              ("National Science Education Standards", "D10001D0_manifest.json"),
              ("NCTE/IRA Standards for the English Language Arts", "D10003BB_manifest.json"),
              ("Common Core English Language Arts", "english.json")]
for doc in local_docs:
    with open(doc[1], "r+") as f:
        local_data = json.load(f)
    d = {
        "title": doc[0],
        "children": local_data
    }
    main_doc['children'].append(d)
process_doc(main_doc)
add_doc(main_doc)


for doc in db:
    print(doc)
    d = db[doc]    
    process_doc(d)
    print(d['count'])
    db.save(d)
