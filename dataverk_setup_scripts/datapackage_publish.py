



# [TODO] ElasticSearch Factory



def publish(self, destination=('nais', 'gcs')):



    self.resource_files = resource_discoverer.search_for_files(start_path=Path(search_start_path),
                                                                   file_names=('settings.json', '.env'), levels=4)


    try:
        env_store = EnvStore(Path(self.resource_files[".env"]))
    except KeyError:
        env_store = None

    settings = settings.create_settings_store(settings_file_path=Path(self.resource_files["settings.json"]),
                                                   env_store=env_store)


    metadata_store = {} # [TODO] get datapackage.json



    place_to_publish = mappingobj["place_to_publish"]


    publish_to_place(place_to_publish)



    if 'nais' in destination:
        publish_data.publish_s3_nais(dir_path=self.dir_path,
                                     datapackage_key_prefix=self._datapackage_key_prefix
                                     (self.datapackage_metadata["datapackage_name"]),
                                     settings=self.settings)

        try:
            es = ElasticsearchConnector(self.settings, host="elastic_private")
            id = self.datapackage_metadata["datapackage_name"]
            js = json.dumps(self.datapackage_metadata)
            es.write(id, js)
        except:
            print("Exception: write to elastic index failed")
            pass

    if self.is_public and 'gcs' in destination:
        publish_data.publish_google_cloud(dir_path=self.dir_path,
                                          datapackage_key_prefix=self._datapackage_key_prefix
                                          (self.datapackage_metadata["datapackage_name"]),
                                          settings=self.settings)

        try:
            es = ElasticsearchConnector(self.settings, host="elastic_public")
            id = self.datapackage_metadata["datapackage_name"]
            js = json.dumps(self.datapackage_metadata)
            es.write(id, js)
        except:
            print("Exception: write to public elastic index failed")
            pass


def _datapackage_key_prefix(datapackage_name):
    return datapackage_name + '/'


def publish_to_place(data):

    for bucket in butckets:
        conn = connFac(data)
        elestic = elasticFac(data)
        generic_publish(conn, elestic)
