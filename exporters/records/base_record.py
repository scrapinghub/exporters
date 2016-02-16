import json


class BaseRecord(dict):
    """
    This class represents the basic item that the exporters pipeline works with.

    fields:
        - group_key: grouping info. It describes which keys combination has been used to group the item
        - group_membership: grouping info. It describes the group membership of the item.
        - formatted: Item serialized to string using the configured formatter.
    """
    group_key = []
    group_membership = ()
    format = 'json'
    header = False
    bottom = False

    def __init__(self, *args, **kwargs):
        super(BaseRecord, self).__init__(*args, **kwargs)
        self._formatted = None

    @property
    def formatted(self):
        return json.dumps(self) if self._formatted is None else self._formatted

    @formatted.setter
    def formatted(self, fmt):
        self._formatted = fmt