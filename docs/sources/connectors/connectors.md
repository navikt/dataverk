<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/connectors/base.py#L11)</span>
## BaseConnector class

```python
dataverk.connectors.base.BaseConnector(encrypted=True)
```

Common connection methods



---
## BaseConnector methods

### get_pandas_df


```python
get_pandas_df(query)
```


Get Pandas

Method inherited from BaseConnector


---
### get_user


```python
dataverk.utils.get_user()
```


Get currently logged in user

Inherited from AuthMixin class 


---
### is_authorized


```python
dataverk.utils.is_authorized(user=None)
```


Check if is user is authorized to access data source

Inherited from AuthMixin class 


---
### log


```python
log(message)
```


Logging util

Method inherited from BaseConnector


----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/connectors/jsonstat.py#L8)</span>
## JSONStatConnector class

```python
dataverk.connectors.jsonstat.JSONStatConnector()
```

JSONStat based connections



---
## JSONStatConnector methods

### get_pandas_df


```python
get_pandas_df(query)
```


Get Pandas dataframe


---
### get_user


```python
dataverk.utils.get_user()
```


Get currently logged in user

Inherited from AuthMixin class 


---
### is_authorized


```python
dataverk.utils.is_authorized(user=None)
```


Check if is user is authorized to access data source

Inherited from AuthMixin class 


---
### log


```python
log(message)
```


Logging util

Method inherited from BaseConnector


----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/connectors/storage.py#L9)</span>
## StorageConnector class

```python
dataverk.connectors.storage.StorageConnector(settings, storage='gcs', encrypted=True)
```

Storage connection



---
## StorageConnector methods

### get_blob_metadata


```python
get_blob_metadata(blob_name, format='markdown')
```


Get metadata from blob storage


---
### get_pandas_df


```python
get_pandas_df(query)
```


Get Pandas

Method inherited from BaseConnector


---
### get_user


```python
dataverk.utils.get_user()
```


Get currently logged in user

Inherited from AuthMixin class 


---
### is_authorized


```python
dataverk.utils.is_authorized(user=None)
```


Check if is user is authorized to access data source

Inherited from AuthMixin class 


---
### log


```python
log(message)
```


Logging util

Method inherited from BaseConnector


---
### read


```python
read(blob_name, encrypted=None)
```


Download from blob storage


---
### write


```python
write(source_string, destination_blob_name, fmt='json', metadata={})
```


Upload to blob storage


----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/connectors/file_storage.py#L9)</span>
## FileStorageConnector class

```python
dataverk.connectors.file_storage.FileStorageConnector(settings, encrypted=True, bucket=None)
```

File Storage connector

---
## FileStorageConnector methods

### delete


```python
delete(file_name)
```

---
### download


```python
download(blob_name, destination_file_name)
```

---
### get_blob_metadata


```python
get_blob_metadata(blob_name, format='markdown')
```


Get a blob's metadata.


---
### get_pandas_df


```python
get_pandas_df(query)
```


Get Pandas

Method inherited from BaseConnector


---
### get_user


```python
dataverk.utils.get_user()
```


Get currently logged in user

Inherited from AuthMixin class 


---
### is_authorized


```python
dataverk.utils.is_authorized(user=None)
```


Check if is user is authorized to access data source

Inherited from AuthMixin class 


---
### log


```python
log(message)
```


Logging util

Method inherited from BaseConnector


---
### read


```python
read(file_name)
```


Downloads a blob from the bucket.
---
### write


```python
write(source_string, destination_blob_name, fmt, metadata=None)
```

----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/connectors/oracle.py#L11)</span>
## OracleConnector class

```python
dataverk.connectors.oracle.OracleConnector(settings, source=None)
```

Common oracle connector methods

Troubleshooting:

Note: Oracle instant client libraries required to be installed in order to use cx_Oracle

Mac:

´´´
unzip instant client zip file from oracle to ~/opt/oracle/instantclient_12_2
ln -s ~/opt/oracle/instantclient_12_2/libclntsh.dylib.12.1 /usr/local/lib/
´´´



---
## OracleConnector methods

### get_pandas_df


```python
get_pandas_df(sql, arraysize=100000)
```

---
### get_user


```python
dataverk.utils.get_user()
```


Get currently logged in user

Inherited from AuthMixin class 


---
### is_authorized


```python
dataverk.utils.is_authorized(user=None)
```


Check if is user is authorized to access data source

Inherited from AuthMixin class 


---
### log


```python
log(message)
```


Logging util

Method inherited from BaseConnector


---
### persist_pandas_df


```python
persist_pandas_df(table, schema=None, df=None, chunksize=10000)
```

----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/connectors/google_storage.py#L10)</span>
## GoogleStorageConnector class

```python
dataverk.connectors.google_storage.GoogleStorageConnector(settings, encrypted=True)
```

Google Storage connector

---
## GoogleStorageConnector methods

### delete_blob


```python
delete_blob(blob_name)
```


Deletes a blob from the bucket.
---
### download_blob


```python
download_blob(blob_name, destination_file_name)
```


Downloads a blob from the bucket.
---
### get_blob_metadata


```python
get_blob_metadata(blob_name, format='markdown')
```


Prints out a blob's metadata.
---
### get_pandas_df


```python
get_pandas_df(query)
```


Get Pandas

Method inherited from BaseConnector


---
### get_user


```python
dataverk.utils.get_user()
```


Get currently logged in user

Inherited from AuthMixin class 


---
### is_authorized


```python
dataverk.utils.is_authorized(user=None)
```


Check if is user is authorized to access data source

Inherited from AuthMixin class 


---
### log


```python
log(message)
```


Logging util

Method inherited from BaseConnector


---
### read


```python
read(blob_name)
```


Downloads a blob from the bucket.
---
### upload_blob


```python
upload_blob(source_file_name, destination_blob_name)
```


Uploads a file to the bucket.
---
### write


```python
write(source_string, destination_blob_name, fmt, metadata={})
```


Write string to a bucket.
----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/connectors/elasticsearch.py#L8)</span>
## ElasticsearchConnector class

```python
dataverk.connectors.elasticsearch.ElasticsearchConnector(settings, host='elastic_private')
```

Elasticsearch connection

---
## ElasticsearchConnector methods

### get


```python
get(id)
```


Retrieve document by id from elastic index
---
### get_pandas_df


```python
get_pandas_df(query)
```


Get Pandas

Method inherited from BaseConnector


---
### get_user


```python
dataverk.utils.get_user()
```


Get currently logged in user

Inherited from AuthMixin class 


---
### is_authorized


```python
dataverk.utils.is_authorized(user=None)
```


Check if is user is authorized to access data source

Inherited from AuthMixin class 


---
### log


```python
log(message)
```


Logging util

Method inherited from BaseConnector


---
### search


```python
search(query)
```


Search elastic index
---
### write


```python
write(id, doc)
```


Add or update document
----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/connectors/sqlite.py#L5)</span>
## SQLiteConnector class

```python
dataverk.connectors.sqlite.SQLiteConnector(source=':memory:')
```


---
## SQLiteConnector methods

### get_pandas_df


```python
get_pandas_df(query)
```


Get Pandas dataframe
---
### get_user


```python
dataverk.utils.get_user()
```


Get currently logged in user

Inherited from AuthMixin class 


---
### is_authorized


```python
dataverk.utils.is_authorized(user=None)
```


Check if is user is authorized to access data source

Inherited from AuthMixin class 


---
### log


```python
log(message)
```


Logging util

Method inherited from BaseConnector


---
### persist_pandas_df


```python
persist_pandas_df(table, df)
```


Persist Pandas dataframe