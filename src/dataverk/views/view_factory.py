from enum import Enum
from typing import Sequence

from dataverk.views.datatable_spec import DataTableSpecView
from dataverk.views.default import DefaultView
from dataverk.views.json_spec import JsonSpecView


class RemoteSpecTypes(Enum):
    VEGA: str = "vega"
    PLOTLY: str = "plotly"
    DATATABLE: str = "datatable"


def get_view_object(name: str, resources: Sequence, title: str = "", description: str = "", attribution: str = "",
                    spec_type: str = "simple", spec: dict = None, type: str = "", group: str = "",
                    series: Sequence = list(), row_limit: int = 500, metadata: dict = None):

    if spec_type == RemoteSpecTypes.PLOTLY:
        return JsonSpecView(name=name, resources=resources, title=title, description=description, attribution=attribution,
                            spec_type=spec_type, spec=spec, type=type, group=group,
                            series=series, row_limit=row_limit, metadata=metadata)
    elif spec_type == RemoteSpecTypes.VEGA:
        return JsonSpecView(name=name, resources=resources, title=title, description=description, attribution=attribution,
                            spec_type=spec_type, spec=spec, type=type, group=group,
                            series=series, row_limit=row_limit, metadata=metadata)
    elif spec_type == RemoteSpecTypes.DATATABLE:
        return DataTableSpecView(name=name, resources=resources, title=title, description=description,
                                 attribution=attribution,
                                 spec_type=spec_type, spec=spec, type=type, group=group,
                                 series=series, row_limit=row_limit, metadata=metadata)
    else:
        return DefaultView(name=name, resources=resources, title=title, description=description, attribution=attribution,
                           spec_type=spec_type, spec=spec, type=type, group=group,
                           series=series, row_limit=row_limit, metadata=metadata)
