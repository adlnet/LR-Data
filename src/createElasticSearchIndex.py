#!/usr/bin/env python
import pyes
from celeryconfig import config
conf = config['elasticsearch']

es = pyes.ES("{0}:{1}".format(conf['host'],conf['port']))
es.create_index(conf['index'])
mapping = { 'resource_locator':{
					'bost':1.0,
					'index':'analyzed',
					'store':'no',
					'type':'string',
					"term_vector" : "with_positions_offsets"
				},
				'resource_data':{
					'bost':1.0,
					'index':'analyzed',
					'store':'no',
					'type':'string',
					"term_vector" : "with_positions_offsets"					
				},
				'doc_ID':{
					'bost':1.0,
					'index':'analyzed',
					'store':'yes',
					'type':'string',
					"term_vector" : "with_positions_offsets"					
				}				

		}
es.put_mapping(conf['index-type'],{'properties':mapping},conf['index'])