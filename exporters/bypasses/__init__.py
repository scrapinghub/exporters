from exporters.bypasses.s3_to_azure_blob_bypass import S3AzureBlobBypass
from exporters.bypasses.s3_to_azure_file_bypass import S3AzureFileBypass
from exporters.bypasses.s3_to_s3_bypass import S3Bypass
from exporters.bypasses.stream_bypass import StreamBypass

"""
This bypass classes will be tried in the specified order, and the first one
that meets the requisites will be used.
"""
default_bypass_classes = [
    S3Bypass,
    S3AzureBlobBypass,
    S3AzureFileBypass,
    StreamBypass,  # Keep at the end
]
