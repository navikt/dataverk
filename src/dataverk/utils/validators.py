import re


def validate_bucket_name(name):
    ''' Validates that the bucket name consists of lower case letters and numbers, words separated with '-', and
        that it does not start or end with '-'.

    :param name: str: bucket name to be validated
    '''

    valid_name_pattern = "(^[a-z0-9])([a-z0-9\-])+([a-z0-9])$"
    if not re.match(pattern=valid_name_pattern, string=name):
        raise NameError(f"Illegal bucket name ({name}): "
                        "Must be lower case letters or numbers, words separated with '-', "
                        "and cannot start or end with '-')")
