from typing import Sequence


class BaseView:

    def __init__(self, name: str, resources: Sequence, title: str = "", description: str = "", attribution: str = "",
                 spec_type: str = "simple", spec: dict = None, type: str = "", group: str = "",
                 series: Sequence = list(), row_limit: int = 500, metadata: dict = None):
        self._resources = resources
        self._title = title
        self._description = description
        self._attribution = attribution
        self._spec_type = spec_type
        self._type = type
        self._group = group
        self._series = series
        self._row_limit = row_limit
        self._metadata = metadata

        if name is None or len(name) is 0:
            self._name = title
        else:
            self._name = name

        if spec is None:
            self._spec = {
                "type": type,
                "group": group,
                "series": series
            }
        else:
            self._spec = spec

    def add_to_datapackage(self, dp):
        raise NotImplementedError()
