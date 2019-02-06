# adapted from fast.ai 

import json, fire, re, os.path
from .notebookname import get_notebook_name 

def is_export(cell):
    if cell['cell_type'] != 'code': return False
    src = cell['source']
    if len(src) == 0 or len(src[0]) < 7: return False
    #import pdb; pdb.set_trace()
    return re.match(r'^\s*#\s*export\s*$', src[0], re.IGNORECASE) is not None

def notebook2script(fname_in = None, fname_out = None):
    if fname_in is None: fname_in = get_notebook_name()
    main_dic = json.load(open(fname_in,'r'))
    cells = main_dic['cells']
    code_cells = [c for c in cells if is_export(c)]
    module = '''
        ##########################################################
        ### THIS FILE WAS AUTOGENERATED FROM JUPYTER ETL NOTEBOOK  ###
        ##########################################################\n\n'''
    for cell in code_cells: module += ''.join(cell['source'][1:]) + '\n\n'
    fname = os.path.splitext(fname_in)[0]
    if fname_out is None: fname_out = f'{fname}.py'
    # remove trailing spaces
    module = re.sub(r' +$', '', module, flags=re.MULTILINE)
    with open(fname_out,'w') as f: 
        f.write(module[:-2])
    print(f"{fname_in} converted to {fname_out}")

if __name__ == '__main__': fire.Fire(notebook2script)