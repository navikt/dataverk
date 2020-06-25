from enum import Enum
from typing import Sequence
from dataverk.views.default import DefaultView
from dataverk.views.vega import VegaView


class ViewType(Enum):
    VEGA: str = "vega"


def get_view_object(name: str, resources: Sequence, title: str = "", description: str = "", attribution: str = "",
                    spec_type: str = "simple", spec: dict = None, type: str = "", group: str = "",
                    series: Sequence = list(), row_limit: int = 500, metadata: dict = None):

    if spec_type == ViewType.VEGA.value:
        return VegaView(name=name, resources=resources, description=description, attribution=attribution,
                        spec_type=spec_type, spec=spec, type=type, group=group,
                        series=series, row_limit=row_limit, metadata=metadata)
    else:
        return DefaultView(name=name, resources=resources, description=description, attribution=attribution,
                           spec_type=spec_type, spec=spec, type=type, group=group,
                           series=series, row_limit=row_limit, metadata=metadata)
