import json
from elasticsearch import Elasticsearch
from dataverk.connectors import BaseConnector

# Elasticsearch
class ElasticsearchConnector(BaseConnector):
    """Elasticsearch connection"""
    def __init__(self, host, port):
        self.es = Elasticsearch(host, port=port)
        self.index = 'metadata'

    def create_index(self):
        """Create index
        
        """
        self.es.indices.delete(index=self.index, ignore=[400, 404])

        body = """
        {
            "settings": {
                "index": {
                "number_of_shards": 1,
                "analysis": {
                    "analyzer": {
                    "trigram": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["standard", "shingle"]
                    },
                    "reverse": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["standard", "reverse"]
                    }, 
                    "autocomplete": {
                        "type":      "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "autocomplete_filter" 
                        ]
                    }
                    },
                    "filter": {
                    "shingle": {
                        "type": "shingle",
                        "min_shingle_size": 2,
                        "max_shingle_size": 3
                    },
                    "autocomplete_filter": { 
                            "type": "edge_ngram",
                            "min_gram": 1,
                            "max_gram": 20
                    }
                    }
                }
                }
            },
            "mappings": {
                "metadata": {
                "properties": {
                    "Tittel": {
                    "type": "text",
                    "fields": {
                        "trigram": {
                        "type": "text",
                        "analyzer": "trigram"
                        },"complete": {
                        "type": "text",
                        "analyzer": "autocomplete"
                        }
                    }
                    },
                    "DatasettEier": {
                    "type": "text"
                    }
                }
                }
            }
        }
        """

        self.es.indices.create(index=self.index, body = body)


    def write(self, id, doc):
        """Add or update ddocument
        
        """

        res = self.es.index(index=self.index, doc_type='metadata', id=id, body=doc)
        self.es.indices.refresh(index="metadata")
        return res

    def get(self, id):
        """Retrieve document by id from elastic index
        
        """

        try:
            self.log(f'Get document {id} from, elastic {self.index}')
            doc = self.es.get(index=self.index, id=id)
            return doc
        except:
            raise ValueError(f'Error retrieving document {id} from elastic index {self.index}')

    def search(self, query):
        """Search elastic index
        
        """

        try:
            self.log(f'Search elastic {self.index} with query {query}')
            hits = self.es.search(index=self.index, query=query)
            return hits
        except:
            raise ValueError(f'Error retrieving document {id} from elastic index {self.query}')
        
      


