from elasticsearch import Elasticsearch
from dataverk.connectors import BaseConnector
from dataverk.utils.settings_store import SettingsStore

# Elasticsearch
class ElasticsearchConnector(BaseConnector):
    """Elasticsearch connection"""

    def __init__(self, settings: SettingsStore, host='private'):

        host_uri = settings["index_connections"][host]

        if host_uri is None:
            raise ValueError('Connection settings are not avilable for the host: {host}')

        self.es = Elasticsearch([host_uri])
        self.index = settings["index_connections"]["index"]

    def _create_index(self):
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
                    "Readme": {
                    "type": "text"
                    }
                }
                }
            }
        }
        """

        self.es.indices.create(index=self.index, body = body)


    def write(self, id, doc):
        """Add or update ddocument"""

        res = self.es.index(index=self.index, doc_type=self.index, id=id, body=doc)
        self.es.indices.refresh(index=self.index)
        self.log(f'{self.__class__}: Document {id} of type {self.index} indexed to elastic index: {self.index}.')
        return res

    def get(self, id):
        """Retrieve document by id from elastic index"""

        try:
            self.log(f'Get document {id} from, elastic {self.index}')
            doc = self.es.get(index=self.index, id=id)
            return doc
        except:
            self.log(f'{self.__class__}: Unable to retrieve document: {id} by id from elastic index: {self.index}.')

    def search(self, query):
        """Search elastic index"""

        try:
            self.log(f'Search elastic {self.index} with query {query}')
            hits = self.es.search(index=self.index, query=query)
            return hits
        except:
            self.log(f'{self.__class__}: Unable to retrieve document with query: {query} from elastic index: {self.index}.')
        
      


