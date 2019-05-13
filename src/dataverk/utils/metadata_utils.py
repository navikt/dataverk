def get_csv_schema(df, filename):
    fields = []

    for name, dtype in zip(df.columns, df.dtypes):
        # TODO : Bool and others? Move to utility method
        if str(dtype) == 'object':
            dtype = 'string'
        else:
            dtype = 'number'

        fields.append({'name': name, 'description': '', 'type': dtype})

    return {
        'name': filename,
        'path': 'resources/' + filename + '.csv',
        'format': 'csv',
        'mediatype': 'text/csv',
        'schema': {'fields': fields}
    }
