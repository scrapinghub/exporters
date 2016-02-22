import unittest

from exporters.export_formatter.xml_export_formatter import XMLExportFormatter
from exporters.write_buffer import GroupingInfo


class XMLFileHandlerTest(unittest.TestCase):

    def setUp(self):
        self.handler = XMLExportFormatter({})
        self.handler.set_grouping_info(GroupingInfo())
        self.handler.grouping_info._init_group_info_key('somekey')

    def tearDown(self):
        self.handler.close()

    def test_create_new_buffer_path_for_key(self):
        self.handler.start_exporting('somekey')
        self.handler.finish_exporting('somekey')
        filename = self.handler.get_group_path('somekey')
        self.handler._compress_file(filename)
        expected = '<root>\n</root>'
        with open(filename) as f:
            content = f.read()
        self.assertEqual(content, expected)