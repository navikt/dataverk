from pathlib import Path
from string import Template

exclude = ["__pycache__", "__init__", "abc"]


template = """
.. _$link_name:

$headline
========

.. module:: dataverk.connectors.$module_name

.. autoclass:: $classname
    :members:
"""

index_template = """
.. _connectors:

Connectors
=================

.. toctree::

    $connectors


"""

def parse_class_name(module_string):
    words = module_string.split()
    class_name_index = words.index("class") + 1
    return words[class_name_index]


def module_contains_class(module_string):
    words = module_string.split()
    try:
        words.index("class")
    except ValueError:
        return False
    else:
        return True


def generate_con_docs():
    file_dir: Path = Path(__file__).parent
    connectors_dir: Path = file_dir.parent.parent.parent.joinpath("src").joinpath("dataverk").joinpath("connectors")
    for con in connectors_dir.iterdir():
        if con.is_file():
            file_name = str(con.parts[-1]).split(".")[0]
            if file_name not in exclude:
                with file_dir.joinpath(file_name + ".rst").open("w") as con_doc_file:
                    with con.open("r") as module_reader:
                        moduel_str = module_reader.read()
                        if module_contains_class(moduel_str):
                            class_name = parse_class_name(moduel_str)
                        else:
                            class_name = file_name
                    temp = Template(template)
                    final_string = temp.safe_substitute(link_name=file_name, headline=file_name.capitalize(),
                                                        module_name=file_name, classname=class_name)
                    con_doc_file.write(final_string)


def update_connectors_index():
    file_dir: Path = Path(__file__).parent
    index_file = file_dir.joinpath("connectors.rst")

    connectors = ""
    for connector in file_dir.iterdir():
        file_name = str(connector.parts[-1]).split(".")[0]
        connectors += file_name + "\n"

    with index_file.open("w") as writer:
        temp = Template(index_template)
        final_string = temp.safe_substitute(connectors=connectors)
        writer.write(final_string)


if __name__ == "__main__":
    update_connectors_index()

