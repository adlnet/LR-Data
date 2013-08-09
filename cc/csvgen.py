from requests import get
from pyquery import PyQuery as pq
from urlparse import urljoin
import csv


def get_standards_page():   
    def page(url):  
        d = pq(url=url)
        links = d("td.views-field-title a") 
        for l in links:
            u = urljoin(url, l.attrib['href'])
            yield u
        next_link = d('li.pager-next a')
        for l in next_link:
            for r in page(urljoin(url, urljoin(url, l.attrib['href']))):
                yield r
    url = "http://asn.jesandco.org/resources/ASNJurisdiction"
    return page(url)


def get_json_links(page_url):
    d = pq(url=page_url)
    links = d("td.views-field-markup a")
    for l in links:
        url = l.attrib['href']
        if url.endswith('json'):
            yield url


def process_doc(url):   
    def process(data):
        for s in data:
            guid = None
            uri = None
            asn = s.get("asn_statementNotation")
            if "skos_exactMatch" in s:
                if isinstance(s['skos_exactMatch'], dict):
                    uri = s['skos_exactMatch']['uri']
                elif isinstance(s['skos_exactMatch'], list):
                    if len(s['skos_exactMatch']) >= 1:
                        uri =  s['skos_exactMatch'][0]['uri']
                    if len(s['skos_exactMatch']) >= 2:
                        guid = s['skos_exactMatch'][1]['uri']
            m = [s.get('id')]
            if asn is not None:
                m.append(asn)
            if uri is not None:
                m.append(uri)
            if guid is not None:
                m.append(guid)
            yield m
            if "children" in s:
                for c in process(s['children']):
                    yield c
    data = get(url).json()
    return process(data)


def main():
    with open('mapping.csv', 'w') as f:
        w = csv.writer(f)
        for standard in get_standards_page():
            for url in get_json_links(standard):
                for mapping in process_doc(url):
                    try:
                        w.writerow(mapping)
                    except:
                        print(mapping)

if __name__ == "__main__":
    main()