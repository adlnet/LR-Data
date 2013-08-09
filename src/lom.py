import requests
import ijson
from lxml import etree
lom_namespaces = {
"lom": "http://ltsc.ieee.org/xsd/LOM"
}
base_xpath = "//lom:lom/lom:general/lom:{0}/lom:string[@language='en-us' or @language='en-gb' or @language='en']"
slice_url = "http://node01.public.learningregistry.net/obtain?by_doc_ID=t&request_ID=e14839e8b5554da49983ad1b72af976d"
res = requests.get(slice_url, stream=True)
items = ijson.items(res.raw, "documents.item.document.item")


def parse_lom(data, config):
	dom = etree.fromstring(data['resource_data'])
	keys = []
	found_titles = dom.xpath(base_xpath.format('title'),
		namespaces=namespaces)
	keys.extend([i.text for i in found_titles])
	found_description = dom.xpath(base_xpath.format('description'),
		namespaces=namespaces)
	keys.extend([i.text for i in found_description])
	found_keyword = dom.xpath(base_xpath.format('keyword'),
		namespaces=namespaces)
	keys.extend([i.text for i in found_keyword])
	return keys


for i in items:
	print(parse_lom(i, None))