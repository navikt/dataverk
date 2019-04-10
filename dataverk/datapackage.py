import copy
from collections import Mapping

import pandas as pd
import json
import datetime
from dataverk.connectors import OracleConnector, SQLiteConnector
from dataverk.utils import resource_discoverer, notebook2script, get_notebook_name
from dataverk.context import settings
from dataverk.utils.validators import validate_bucket_name, validate_datapackage_name
from pathlib import Path
from dataverk.context import EnvStore
from collections.abc import MutableMapping, MutableSequence


class Datapackage:
    """
    Understands packaging of data resources and views on those resources for publication
    """

    def __init__(self, metadata: Mapping, package_license: str=None, readme: str=None):
        self._resources = {}
        self._license = package_license
        self._readme = readme
        self.views = []
        self._datapackage_metadata = self._create_datapackage(dict(metadata))

    @property
    def datapackage_metadata(self):
        return copy.deepcopy(self._datapackage_metadata)

    @property
    def resources(self):
        return self._resources

    def _create_datapackage(self, metadata):
        today = datetime.date.today().strftime('%Y-%m-%d')

        metadata['updated'] = today
        metadata['version'] = "0.0.1"
        metadata['license'] = self._license
        metadata['readme'] = self._readme
        metadata["views"] = []
        metadata["resources"] = []
        metadata["datasets"] = {}
        validate_bucket_name(metadata["bucket_name"])
        return metadata

    def add_resource(self, df: pd.DataFrame, dataset_name: str, dataset_description: str=""):
        """
        Adds a provided DataFrame as a resource in the Datapackage object with provided name and description.

        :param df: DataFrame to add as resource
        :param dataset_name: Name of the dataset
        :param dataset_description: Description of the dataset
        :return: None
        """

        self._verify_add_resource_input_types(df, dataset_name, dataset_description)
        self.resources[dataset_name] = df
        self._datapackage_metadata["datasets"][dataset_name] = dataset_description
        self._datapackage_metadata['resources'].append(self._get_csv_schema(df, dataset_name))

    def _verify_add_resource_input_types(self, df, dataset_name, dataset_description):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f'df must be of type pandas.Dataframe()')
        if not isinstance(dataset_name, str):
            raise TypeError(f'dataset_name must be of type string')
        if not isinstance(dataset_description, str):
            raise TypeError(f'dataset_description must be of type string')

    def _get_csv_schema(self, df, filename):
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

    def add_view(self, name: str, resources: MutableSequence, title: str="", description: str="", attribution: str="", spec_type: str="simple",
                 spec: MutableMapping=None, type: str="", group: str="", series: MutableSequence=list(), row_limit: int=500):
        """
        Adds a view to the Datapackage object. A view is a specification of a visualisation the datapackage provides.

        :param name: View name
        :param resources: resource the view is for
        :param title: Title to be presented in the visualisation
        :param description: Description of the view
        :param attribution:
        :param spec_type: Spec type eg. (matplotlib, vega-lite)
        :param spec:
        :param type:
        :param group:
        :param series:
        :param row_limit:
        :return: None
        """
        if spec is None:
            spec = {"type": type,
                    "group": group,
                    "series": series}

        view = {'name': name,
                'title': title,
                'description': description,
                'attribution': attribution,
                'resources': resources,
                'specType': spec_type,
                'spec': spec,
                'transform': {
                    "limit": row_limit
                }}

        self._datapackage_metadata["views"].append(view)

    def write_datapackage(self, path="."):
        """
        Writes the Datapackage object to output files, the datapackage files can then be published.

        :return: None
        """

        path = Path(path)
        with path.joinpath('datapackage.json').open('w', encoding="utf-8") as datapackage_file:
            json.dump(self._datapackage_metadata, datapackage_file, indent=2, sort_keys=True)

            data_path = path.joinpath('resources/')
            if not data_path.exists():
                data_path.mkdir()

            for filename, df in self.resources.items():
                df.to_csv(data_path.joinpath(filename + '.csv'), index=False, sep=',')

