from typing import Sequence
from dataverk.views.remote_spec import RemoteSpecView


class DataTableSpecView(RemoteSpecView):

    def __init__(self, name: str, resources: Sequence, title: str = "", description: str = "", attribution: str = "",
                 spec_type: str = "simple", spec: dict = None, type: str = "", group: str = "",
                 series: Sequence = list(), row_limit: int = 500, metadata: dict = None):
        super().__init__(name=name, resources=resources, title=title, description=description, attribution=attribution,
                         spec_type=spec_type, spec=spec, type=type, group=group,
                         series=series, row_limit=row_limit, metadata=metadata)

        if not metadata:
            self._metadata = {}

    def _add_resource(self, dp) -> str:
        return dp.add_resource(self._spec,
                               resource_name=self._name,
                               resource_description=self._name,
                               resource_type="df",
                               spec={'dsv_separator': self._metadata.get('dsv_separator', ';')})
