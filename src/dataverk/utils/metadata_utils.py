def get_schema(df, dataset_name, format, separator):
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
        'df': df,
        'name': dataset_name,
        'path': f'resources/{dataset_name}.{format}',
        'format': format,
        'separator': separator,
        'mediatype': mediatype,
        'schema': {'fields': fields}
    }


