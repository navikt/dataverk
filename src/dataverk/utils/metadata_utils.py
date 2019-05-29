def get_schema(df, path, dataset_name, format, dsv_separator):
    fields = []

    for name, dtype in zip(df.columns, df.dtypes):
        # TODO : Bool and others
        if str(dtype) == 'object':
            dtype = 'string'
        else:
            dtype = 'number'

        fields.append({'name': name, 'description': '', 'type': dtype})

    if format == 'csv':
        mediatype = 'text/csv'
    elif format == 'json':
        mediatype = 'application/json'
    else:
        mediatype = 'text/csv'

    return {
        'name': dataset_name,
        'path': f'{path}/resources/{dataset_name}.{format}',
        'format': format,
        'separator': dsv_separator,
        'mediatype': mediatype,
        'schema': {'fields': fields}
    }


