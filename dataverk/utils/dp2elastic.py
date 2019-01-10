import json, fire, re, os.path
from .notebookname import get_notebook_name 

def dp2elastic(dp = None, elasticdoc = None):
    dic = json.load(open('datapackage.json', 'r'))
    path = dic.get('path','')
    path = path.rstrip('\/')
    path = f'{path}/datapackage.json'
    js = {
                'name': dic.get('id', ''),
                'title': dic.get('title', ''),
                'updated': dic.get('updated', ''),
                'keywords': dic.get('keywords', []),
                'accessRights': dic.get('accessRights', ''),
                'description': dic.get('description', ''),
                'publisher': dic.get('publisher', ''),
                'geo': dic.get('geo', []),
                'provenance': dic.get('provenance', ''),
                'uri': path
    }
    fname_out = 'elasticdoc.json'
    with open(fname_out,'w') as f: 
        f.write(json.dumps(js))
    print(f"{dp} converted to {fname_out}")


if __name__ == '__main__': fire.Fire(dp2elastic)