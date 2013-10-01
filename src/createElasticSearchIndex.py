#!/usr/bin/env python
import pyes
from celeryconfig import config

es = pyes.ES([("http", "localhost", "9200")])
es.create_index_if_missing("lr")
mapping = {
        "title": {
            "index": "analyzed",
            "store": "yes",
            "type": "string",
            },
        "description": {
            "index": "analyzed",
            "store": "yes",
            "type": "string",
            },
        "publisher": {
            "index": "analyzed",
            "store": "yes",
            "type": "string",
            },
        "keys": {
            "index": "analyzed",
            "store": "no",
            "type": "string",
            },
        "standards": {
            "index": "analyzed",
            "store": "no",
            "type": "string",
            },
        "accessMode": {
            "index": "analyzed",
            "store": "yes",
            "type": "string",
            },
        "mediaFeatures": {
            "index": "analyzed",
            "store": "yes",
            "type": "string",
            },
        "url": {
            "index": "not_analyzed",
            "store": "yes",
            "type": "string"
            },
        "hasScreenshot": {
            "index": "not_analyzed",
            "store": "boolean",
            "type": "string"
            },
        }
es.put_mapping("lr_doc" ,{'properties':mapping}, ["lr"])
