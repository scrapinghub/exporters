from exporters.bypasses.s3_to_azure_blob_bypass import AzureBlobS3Bypass
from exporters.bypasses.s3_to_azure_file_bypass import AzureFileS3Bypass
from exporters.bypasses.s3_to_s3_bypass import S3Bypass

default_bypass_classes = [S3Bypass, AzureBlobS3Bypass, AzureFileS3Bypass]
