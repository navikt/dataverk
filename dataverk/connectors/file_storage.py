import os
import json
from dataverk.connectors.abc.base import BaseConnector
from dataverk.utils.file_functions import write_file, read_file
from dataverk.context.settings_classes import SettingsStore


class FileStorageConnector(BaseConnector):
    """File Storage connector"""
    
    def __init__(self, settings: SettingsStore, encrypted=True, bucket=None):

        super(self.__class__, self).__init__(encrypted=encrypted)

        self.bucket = bucket
        self.settings = settings

        try: 
            self.path = self.settings["file_storage_path"]
        except Exception as ex:
            print(ex)

    def write(self, source_string, destination_blob_name, fmt, metadata=None):

        if not isinstance(source_string, str):
            source_string = source_string.decode("utf-8") 

        """Write blob file."""
        if self.bucket is not None:
            path = f'{self.path}/{self.bucket}'
        else:
            path = self.path
             
        try:
            write_file(f'{path}/{destination_blob_name}.{fmt}', source_string)
            self.log(f'{self.__class__}: String (format: {fmt}) written to file: {path}/{destination_blob_name}.{fmt}') 
        except:
            raise ValueError(f'Error writing {destination_blob_name} to file: {path}/{destination_blob_name}.{fmt}')

        if metadata is not None:
            try: 
                write_file(f'{path}/{destination_blob_name}.meta', json.dumps(metadata))
                self.log('f{self.__class__}: Metadata written to file: {path}/{destination_blob_name}.meta') 
            except:
                raise ValueError(f'Error writing {destination_blob_name} metadata to file: {path}/{destination_blob_name}.meta')

        return f'{path}/{destination_blob_name}.{fmt}'


    def read(self, file_name):
        """Downloads a blob from the bucket."""
        if self.bucket is not None:
            path = f'{self.path}/{self.bucket}'
        else:
            path = self.path

        return read_file(f'{self.path}/{file_name}')


    def delete(self, file_name):
        try:
            os.remove(file_name)
            self.log(f'{self.__class__}: File{file_name} in bucket {self.bucket} deleted')
        except:
            self.log(f'{self.__class__}: Error deleting file{file_name} in bucket {self.bucket}')


    def download(self, blob_name, destination_file_name):
        raise NotImplementedError
       

    def get_blob_metadata(self, blob_name, format='markdown'):
        """Get a blob's metadata.
        
        """
        if self.bucket is not None:
            path = f'{self.path}/{self.bucket}'
        else:
            path = self.path

        blob_id = os.path.splitext(blob_name)[0]

        blob = read_file(f'{path}/{blob_id}.meta')

        blob = json.loads(blob)

        if format == 'markdown':
            items = []

            if blob['dataset'] is not None:
                items.append(f'### Dataset:\n\n')
                for key, value in blob['dataset'].items():
                    if key != 'metadata': 
                        items.append(f'{key}: _{value}_')
                        items.append(f'\n\n')
            
            if blob['dataset']['metadata'] is not None:
                items.append(f'### Metadata:\n\n')
                for key, value in blob['dataset']['metadata'].items():  
                    items.append(f'{key}: _{value}_')
                    items.append(f'\n\n')

                        
            return ''.join(items)


        return blob



