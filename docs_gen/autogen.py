# -*- coding: utf-8 -*-
'''
General documentation architecture:
Home
Index
Credits: This doc generator is largely based on the Keras doc generator
'''

import re
import inspect
import os
import shutil
import sys
import errno
from subprocess import call

sys.path.append(os.path.join(os.getcwd(), os.pardir))

import dataverk
from dataverk import api
from dataverk.datapackage import Datapackage
from dataverk import utils
from dataverk.connectors import JSONStatConnector, StorageConnector, FileStorageConnector, GoogleStorageConnector, \
    SQLiteConnector, ElasticsearchConnector, BaseConnector, OracleConnector
from dataverk.context import EnvStore, settings_classes, settings

root_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

EXCLUDE = {
}

# For each class to document, it is possible to:
# 1) Document only the class: [classA, classB, ...]
# 2) Document all its methods: [classA, (classB, "*")]
# 3) Choose which methods to document (methods listed as strings):
# [classA, (classB, ["method1", "method2", ...]), ...]
# 4) Choose which methods to document (methods listed as qualified names):
# [classA, (classB, [module.classB.method1, module.classB.method2, ...]), ...]
PAGES = [
    {
      'page': 'datapackage.md',
        'classes': [(Datapackage, '*')]
    },
    {
        'page': 'api.md',
        'functions': [api.publish_datapackage,
                      api.write_datapackage,
                      api.is_sql_file,
                      api.read_sql,
                      api.to_sql,
                      api.notebook2script,
                      api.write_notebook]

    },
    {
        'page': 'connectors/connectors.md',
        'classes': [
            (BaseConnector, '*'),
            (JSONStatConnector, '*'),
            (StorageConnector, '*'),
            (FileStorageConnector, '*'),
            (OracleConnector, '*'),
            (GoogleStorageConnector, '*'),
            (ElasticsearchConnector, '*'),
            (SQLiteConnector, '*'),
        ],
    },
    {
        'page': 'utils.md',
        'functions': [utils.get_fylke_from_region],
        'classes': [utils.AuthMixin,
                    utils.LoggerMixin]
    },
    {
        'page': 'context.md',
        'functions': [settings.singleton_settings_store_factory, settings.settings_store_factory],
        'classes': [(EnvStore, '*'),
                    (settings_classes.SettingsStore, '*'),
                    (settings_classes.SettingsBuilder, '*')]
    }
]

ROOT = 'https://github.com/navikt/dataverk.io'


def get_function_signature(function, method=True):
    wrapped = getattr(function, '_original_function', None)
    if wrapped is None:
        signature = inspect.getfullargspec(function)
    else:
        signature = inspect.getfullargspec(wrapped)
    defaults = signature.defaults
    if method:
        args = signature.args[1:]
    else:
        args = signature.args
    if defaults:
        kwargs = zip(args[-len(defaults):], defaults)
        args = args[:-len(defaults)]
    else:
        kwargs = []
    st = '%s.%s(' % (clean_module_name(function.__module__), function.__name__)

    for a in args:
        st += str(a) + ', '
    for a, v in kwargs:
        if isinstance(v, str):
            v = '\'' + v + '\''
        st += str(a) + '=' + str(v) + ', '
    if kwargs or args:
        signature = st[:-2] + ')'
    else:
        signature = st + ')'

    if not method:
        # Prepend the module name.
        signature = clean_module_name(function.__module__) + '.' + signature
    return post_process_signature(signature)


def get_class_signature(cls):
    try:
        class_signature = get_function_signature(cls.__init__)
        class_signature = class_signature.replace('__init__', cls.__name__)
    except (TypeError, AttributeError):
        # in case the class inherits from object and does not
        # define __init__
        class_signature = clean_module_name(cls.__module__) + '.' + cls.__name__ + '()'
    return post_process_signature(class_signature)


def post_process_signature(signature):
    parts = re.split(r'\.(?!\d)', signature)
    if len(parts) >= 4:
        if parts[1] == 'utils':
            signature = 'dataverk.utils.' + '.'.join(parts[3:])
        if parts[1] == 'datasets':
            signature = 'dataverk.datasets.' + '.'.join(parts[3:])
    return signature


def clean_module_name(name):
    assert name[:9] == 'dataverk.', 'Invalid module name: %s' % name
    return name


def class_to_docs_link(cls):
    module_name = clean_module_name(cls.__module__)
    module_name = module_name[6:]
    link = ROOT + module_name.replace('.', '/') + '#' + cls.__name__.lower()
    return link


def class_to_source_link(cls):
    module_name = clean_module_name(cls.__module__)
    path = module_name.replace('.', '/')
    path += '.py'
    line = inspect.getsourcelines(cls)[-1]
    link = ('https://github.com/navikt/dataverk/blob/master/' + path + '#L' + str(line))
    return '[[source]](' + link + ')'


def class_to_notebook_link(cls):
    module_name = clean_module_name(cls.__module__)
    path = module_name.replace('.', '/')
    path += '.ipynb'
    line = inspect.getsourcelines(cls)[-1]
    link = ('https://github.com/navikt/dataverk/blob/master/' + path + '#L' + str(line))
    return '[[source]](' + link + ')'


def class_to_source_file(cls, ext):
    module_name = clean_module_name(cls.__module__)
    path = module_name.replace('.', '/')
    path = f'{root_path}/{path}.{ext}'
    return path


def code_snippet(snippet):
    result = '```python\n'
    result += snippet + '\n'
    result += '```\n'
    return result


def count_leading_spaces(s):
    ws = re.search(r'\S', s)
    if ws:
        return ws.start()
    else:
        return 0


def process_list_block(docstring, starting_point, leading_spaces, marker):
    ending_point = docstring.find('\n\n', starting_point)
    block = docstring[starting_point:None if ending_point == -1 else ending_point - 1]
    # Place marker for later reinjection.
    docstring = docstring.replace(block, marker)
    lines = block.split('\n')
    # Remove the computed number of leading white spaces from each line.
    lines = [re.sub('^' + ' ' * leading_spaces, '', line) for line in lines]
    # Usually lines have at least 4 additional leading spaces.
    # These have to be removed, but first the list roots have to be detected.
    top_level_regex = r'^    ([^\s\\\(]+):(.*)'
    top_level_replacement = r'- __\1__:\2'
    lines = [re.sub(top_level_regex, top_level_replacement, line) for line in lines]
    # All the other lines get simply the 4 leading space (if present) removed
    lines = [re.sub(r'^    ', '', line) for line in lines]
    # Fix text lines after lists
    indent = 0
    text_block = False
    for i in range(len(lines)):
        line = lines[i]
        spaces = re.search(r'\S', line)
        if spaces:
            # If it is a list element
            if line[spaces.start()] == '-':
                indent = spaces.start() + 1
                if text_block:
                    text_block = False
                    lines[i] = '\n' + line
            elif spaces.start() < indent:
                text_block = True
                indent = spaces.start()
                lines[i] = '\n' + line
        else:
            text_block = False
            indent = 0
    block = '\n'.join(lines)
    return docstring, block


def process_docstring(docstring):
    # First, extract code blocks and process them.
    code_blocks = []
    if '```' in docstring:
        tmp = docstring[:]
        while '```' in tmp:
            tmp = tmp[tmp.find('```'):]
            index = tmp[3:].find('```') + 6
            snippet = tmp[:index]
            # Place marker in docstring for later reinjection.
            docstring = docstring.replace(
                snippet, '$CODE_BLOCK_%d' % len(code_blocks))
            snippet_lines = snippet.split('\n')
            # Remove leading spaces.
            num_leading_spaces = snippet_lines[-1].find('`')
            snippet_lines = ([snippet_lines[0]] +
                             [line[num_leading_spaces:]
                              for line in snippet_lines[1:]])
            # Most code snippets have 3 or 4 more leading spaces
            # on inner lines, but not all. Remove them.
            inner_lines = snippet_lines[1:-1]
            leading_spaces = None
            for line in inner_lines:
                if not line or line[0] == '\n':
                    continue
                spaces = count_leading_spaces(line)
                if leading_spaces is None:
                    leading_spaces = spaces
                if spaces < leading_spaces:
                    leading_spaces = spaces
            if leading_spaces:
                snippet_lines = ([snippet_lines[0]] +
                                 [line[leading_spaces:]
                                  for line in snippet_lines[1:-1]] +
                                 [snippet_lines[-1]])
            snippet = '\n'.join(snippet_lines)
            code_blocks.append(snippet)
            tmp = tmp[index:]

    # Format docstring lists.
    section_regex = r'\n( +)# (.*)\n'
    section_idx = re.search(section_regex, docstring)
    shift = 0
    sections = {}
    while section_idx and section_idx.group(2):
        anchor = section_idx.group(2)
        leading_spaces = len(section_idx.group(1))
        shift += section_idx.end()
        marker = '$' + anchor.replace(' ', '_') + '$'
        docstring, content = process_list_block(docstring,
                                                shift,
                                                leading_spaces,
                                                marker)
        sections[marker] = content
        section_idx = re.search(section_regex, docstring[shift:])

    # Format docstring section titles.
    docstring = re.sub(r'\n(\s+)# (.*)\n',
                       r'\n\1__\2__\n\n',
                       docstring)

    # Strip all remaining leading spaces.
    lines = docstring.split('\n')
    docstring = '\n'.join([line.lstrip(' ') for line in lines])

    # Reinject list blocks.
    for marker, content in sections.items():
        docstring = docstring.replace(marker, content)

    # Reinject code blocks.
    for i, code_block in enumerate(code_blocks):
        docstring = docstring.replace(
            '$CODE_BLOCK_%d' % i, code_block)

    # Handle :param and :return
    tags = (":param", ":return")
    for tag in tags:
        docstring = docstring.replace(tag, "<br>"+tag)

    return docstring


print('Cleaning up existing sources directory.')
if os.path.exists('sources'):
    shutil.rmtree('sources')

print('Populating sources directory with templates.')
for subdir, dirs, fnames in os.walk('templates'):
    for fname in fnames:
        new_subdir = subdir.replace('templates', 'sources')
        if not os.path.exists(new_subdir):
            os.makedirs(new_subdir)
        if fname[-3:] == '.md':
            fpath = os.path.join(subdir, fname)
            new_fpath = fpath.replace('templates', 'sources')
            shutil.copy(fpath, new_fpath)


def read_file(path):
    with open(path) as f:
        return f.read()


def write_file(path, content):
    # https://stackoverflow.com/questions/12517451/automatically-creating-directories-with-file-output
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(path, "w") as f:
        f.write(content)


def collect_class_methods(cls, methods):
    if isinstance(methods, (list, tuple)):
        return [getattr(cls, m) if isinstance(m, str) else m for m in methods]
    methods = []
    for _, method in inspect.getmembers(cls, predicate=inspect.isroutine):
        if method.__name__[0] == '_' or method.__name__ in EXCLUDE:
            continue
        methods.append(method)
    return methods


def render_function(function, method=True):
    subblocks = []
    signature = get_function_signature(function, method=method)
    if method:
        signature = signature.replace(clean_module_name(function.__module__) + '.', '')
    else:
        signature = signature.replace(clean_module_name(function.__module__) + '.', '', 1)
    subblocks.append('### ' + function.__name__ + '\n')
    subblocks.append(code_snippet(signature))
    docstring = function.__doc__
    if docstring:
        subblocks.append(process_docstring(docstring))
    return '\n\n'.join(subblocks)


def get_notebook(cls):
    # Inject notebook markdown
    jupyter_notebook = class_to_source_file(cls, 'ipynb')

    if os.path.isfile(jupyter_notebook):
        arg = "jupyter nbconvert --to markdown " + jupyter_notebook
        try:
            retcode = call(arg, shell=True)
            if retcode < 0:
                print(sys.stderr, "Jupyter notebook conversion was terminated by signal", -retcode)
                return None
            else:
                print(sys.stderr, "Jupyter notebook conversion", retcode)
        except OSError as e:
            print(sys.stderr, "Jupyter notebook conversion failed:", e)
            return None

        jupyter_markdown_file = class_to_source_file(cls, 'md')

        if os.path.isfile(jupyter_markdown_file):
            return read_file(jupyter_markdown_file)


def read_page_data(page_data, type):
    assert type in ['classes', 'functions']
    data = page_data.get(type, [])
    for module in page_data.get('all_module_{}'.format(type), []):
        module_data = []
        for name in dir(module):
            if name[0] == '_' or name in EXCLUDE:
                continue
            module_member = getattr(module, name)
            if (inspect.isclass(module_member) and type == 'classes' or
                    inspect.isfunction(module_member) and type == 'functions'):
                instance = module_member
                if module.__name__ in instance.__module__:
                    if instance not in module_data:
                        module_data.append(instance)
        module_data.sort(key=lambda x: id(x))
        data += module_data
    return data


if __name__ == '__main__':
    readme = read_file('../README.md')
    index = read_file('templates/index.md')
    installation = read_file('templates/getting-started/installation.md')
    contributers = read_file('templates/contributing.md')
    index = index.replace('{{autogenerated}}', readme[readme.find('##'):])
    write_file('sources/index.md', index)
    write_file('sources/getting-started/installation.md', installation)
    write_file('sources/contributing.md', contributers)

    print('Generating docs %s.' % dataverk.__version__)
    for page_data in PAGES:
        classes = read_page_data(page_data, 'classes')

        blocks = []
        for element in classes:
            if not isinstance(element, (list, tuple)):
                element = (element, [])
            cls = element[0]
            subblocks = []
            signature = get_class_signature(cls)
            subblocks.append('<span style="float:right;">' +
                             class_to_source_link(cls) + '</span>')
            if element[1]:
                subblocks.append('## ' + cls.__name__ + ' class\n')
            else:
                subblocks.append('### ' + cls.__name__ + '\n')
            subblocks.append(code_snippet(signature))
            docstring = cls.__doc__
            if docstring:
                subblocks.append(process_docstring(docstring))

            # inject notebook after docstring
            notebook = get_notebook(cls)
            if notebook is not None:
                subblocks.append('\n---')
                subblocks.append('<span style="float:right;">' +
                                 class_to_notebook_link(cls) + '</span>')
                subblocks.append('#### Examples')
                subblocks.append('\n')
                subblocks.append(notebook)

            methods = collect_class_methods(cls, element[1])
            if methods:
                subblocks.append('\n---')
                subblocks.append('## ' + cls.__name__ + ' methods\n')
                subblocks.append('\n---\n'.join(
                    [render_function(method, method=True) for method in methods]))
            blocks.append('\n'.join(subblocks))

        functions = read_page_data(page_data, 'functions')

        for function in functions:
            blocks.append(render_function(function, method=False))

        if not blocks:
            raise RuntimeError('Found no content for page ' +
                               page_data['page'])

        mkdown = '\n----\n\n'.join(blocks)

        # save module page.
        # Either insert content into existing page,
        # or create page otherwise
        page_name = page_data['page']
        path = os.path.join('sources', page_name)
        if os.path.exists(path):
            template = read_file(path)
            assert '{{autogenerated}}' in template, ('Template found for ' + path +
                                                     ' but missing {{autogenerated}} tag.')
            mkdown = template.replace('{{autogenerated}}', mkdown)
            print('...inserting autogenerated content into template:', path)
        else:
            print('...creating new page with autogenerated content:', path)
        subdir = os.path.dirname(path)
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        with open(path, 'w') as f:
            f.write(mkdown)

    # shutil.copyfile('../CONTRIBUTING.md', 'sources/contributing.md')