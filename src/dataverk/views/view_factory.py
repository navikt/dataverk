from enum import Enum
from typing import Sequence
from dataverk.views.default import DefaultView
from dataverk.views.remote_spec import RemoteSpecView


class RemoteSpecTypes(Enum):
    VEGA: str = "vega"
    PLOTLY: str = "plotly"
    DATATABLE: str = "datatable"

    @classmethod
    def has_spec_type(cls, spec_type: str):
        return spec_type in cls._value2member_map_


def get_view_object(name: str, resources: Sequence, title: str = "", description: str = "", attribution: str = "",
                    spec_type: str = "simple", spec: dict = None, type: str = "", group: str = "",
                    series: Sequence = list(), row_limit: int = 500, metadata: dict = None):

    if RemoteSpecTypes.has_spec_type(spec_type):
        return RemoteSpecView(name=name, resources=resources, title=title, description=description, attribution=attribution,
                              spec_type=spec_type, spec=spec, type=type, group=group,
                              series=series, row_limit=row_limit, metadata=metadata)
    else:
        return DefaultView(name=name, resources=resources, title=title, description=description, attribution=attribution,
                           spec_type=spec_type, spec=spec, type=type, group=group,
                           series=series, row_limit=row_limit, metadata=metadata)
