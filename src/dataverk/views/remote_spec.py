from abc import ABC
from typing import Sequence
from dataverk.views.base import BaseView


class RemoteSpecView(ABC, BaseView):

    def __init__(self, name: str, resources: Sequence, title: str = "", description: str = "", attribution: str = "",
                 spec_type: str = "simple", spec: dict = None, type: str = "", group: str = "",
                 series: Sequence = list(), row_limit: int = 500, metadata: dict = None):
        super().__init__(name=name, resources=resources, title=title, description=description, attribution=attribution,
                         spec_type=spec_type, spec=spec, type=type, group=group,
                         series=series, row_limit=row_limit, metadata=metadata)

    def add_to_datapackage(self, dp):
        resource_path = self._add_resource(dp)
        dp.datapackage_metadata["views"].append({
            'name': self._name,
            'title': self._title,
            'description': self._description,
            'attribution': self._attribution,
            'resources': self._resources,
            'specType': self._spec_type,
            'spec': {
                "url": resource_path
            },
            'transform': {
                "limit": self._row_limit
            },
            'metadata': self._metadata
        })

    def _add_resource(self, dp):
        raise NotImplementedError()
