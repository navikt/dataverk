import json, fire, re, os.path


def dp2elastic(dp=None, elasticdoc=None):
    dic = json.load(open('datapackage.json', 'r'))
    path = dic.get('path','')
    path = path.rstrip('\/')
    path = f'{path}/datapackage.json'
    js = {
                'id': dic.get('id', ''),
                'title': dic.get('name', ''),
                'updated': dic.get('updated', ''),
                'keywords': dic.get('keywords', []),
                'auth':  dic.get('auth',[]),
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
    print(f"{fname_out} generated from datapackage.json")


if __name__ == '__main__': fire.Fire(dp2elastic)