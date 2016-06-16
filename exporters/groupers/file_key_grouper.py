from exporters.groupers.base_grouper import BaseGrouper
from exporters.utils import str_list


class FileKeyGrouper(BaseGrouper):
    """
    Groups items depending on their keys. It adds the group membership information to items.

        - keys (list)
            A list of keys to group by
    """
    supported_options = {
        'keys': {'type': str_list}
    }

    def __init__(self, *args, **kwargs):
        super(FileKeyGrouper, self).__init__(*args, **kwargs)
        self.keys = self.read_option('keys', [])

    def _get_nested_value(self, item, key):
        if '.' in key:
            first_key, rest = key.split('.', 1)
            return self._get_nested_value(item.get(first_key, {}), rest)
        else:
            membership = item.get(key, 'unknown')
            if membership is None:
                membership = 'unknown'
            return membership

    def group_batch(self, batch):
        for item in batch:
            item.group_key = self.keys
            membership = []
            for key in self.keys:
                membership.append(self._get_nested_value(item, key))
            item.group_membership = tuple(membership)
            yield item
