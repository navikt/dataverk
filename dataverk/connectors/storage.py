import json
from cryptography.fernet import Fernet
from dataverk.connectors.google_storage import GoogleStorageConnector
from dataverk.connectors.file_storage import FileStorageConnector
from dataverk.connectors.base import BaseConnector
from dataverk.utils.settings_store import SettingsStore


class StorageConnector(BaseConnector):
    """Storage connection
    
    """
    
    def __init__(self, settings: SettingsStore, storage='gcs', encrypted = True):

        super(StorageConnector, self).__init__(encrypted=encrypted)

        self.log(f'Initializing storage connector: {storage}. Encrypted: {encrypted}')
        self.storage = storage
        self.settings = settings
        # TODO make array instead??

        if storage == 'gcs':
            self.conn = GoogleStorageConnector(encrypted=encrypted,settings=self.settings)
        elif storage == 'file':
            self.conn = FileStorageConnector(encrypted=encrypted, settings=settings)
        else:
            raise ValueError("A storage provider must be specified in config file")

    def write(self, source_string, destination_blob_name, fmt='json', metadata={}):
        """Upload to blob storage
        
        """

        if source_string is None or destination_blob_name is None:
            raise ValueError("Source string and destination blob name must be provided")

        if (not isinstance(source_string,str)) and (fmt not in ['xlsx']) :
            source_string = json.dumps(source_string)


        # TODO compress dataset??
        if self.conn is not None:

            if self.encrypted: 
                key = self.settings["encryption_key"]
                f = Fernet(key)
                encypted_source_string = f.encrypt(source_string.encode('utf-8'))
                self.log(f'Writing encrypted string to {destination_blob_name}')
                return self.conn.write(encypted_source_string, destination_blob_name, fmt, metadata)

            self.log(f'Writing unencrypted string to {destination_blob_name}')
            return self.conn.write(source_string, destination_blob_name, fmt, metadata)


    def read(self, blob_name, encrypted = None):
        """Download from blob storage
        
        """ 

        if encrypted is None:
            encrypted = self.conn.encrypted

        if blob_name is None:
            raise ValueError("Error: Blob is None. Blob name must be provided")

        if self.conn is not None:
            blob = self.conn.read(blob_name)

            if encrypted: 
                self.log(f'Reading and decrypting blob {blob_name} from storage {self.storage}. Encrypted {encrypted}')

                if isinstance(blob, str):
                    blob = blob.encode("utf-8") 

                try:
                    key = self.settings["encryption_key"]
            
                    f = Fernet(key)
                    plain_text = f.decrypt(blob)
                    return plain_text
                except:
                    self.log(f'Error decrypting blob {blob_name} from storage {self.storage}. Encrypted {encrypted}')
                    pass
            else:
                self.log(f'Reading unecrypted blob {blob_name} from storage {self.storage}')

            return blob


    def get_blob_metadata(self, blob_name, format='markdown'):
        """Get metadata from blob storage
        
        """

        if blob_name is None:
            raise ValueError("Blob name to read from must be provided")

        if self.conn is not None:
            md = self.conn.get_blob_metadata(blob_name, format)

            return md


    