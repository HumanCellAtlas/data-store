#!/usr/bin/env python

import argparse
import os

from cloud_uploader import GSUploader, S3Uploader, Uploader


def upload(uploader: Uploader):
    uploader.reset()

    # upload the "good" source files
    uploader.checksum_and_upload_file(
        "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30",
        "test_good_source_data/0",
        {
            "hca-dss-content-type": "text/plain",
        }
    )
    uploader.checksum_and_upload_file(
        "9cdc9050cecf59381fed55a2433140b69596fc861bee55abeafd1f9150f3e2da",
        "test_good_source_data/1",
        {
            "hca-dss-content-type": "text/plain",
        }
    )
    uploader.upload_file(
        "9cdc9050cecf59381fed55a2433140b69596fc861bee55abeafd1f9150f3e2da",
        "test_good_source_data/incorrect_case_checksum",
        {
            "hca-dss-content-type": "text/plain",
            "hca-dss-crc32c": "114DEE2C",
            "hca-dss-s3_etag": "7F54939B30AE7B6D45D473A4C82A41B0",
            "hca-dss-sha1": "15684690E8132044F378B4D4AF8A7331C8DA17B1",
            "hca-dss-sha256": "9CDC9050CECF59381FED55A2433140B69596FC861BEE55ABEAFD1F9150F3E2DA",
        }
    )

    if isinstance(uploader, S3Uploader):
        # s3 has an extra test for merging tags and metadata...
        uploader.upload_file(
            "9cdc9050cecf59381fed55a2433140b69596fc861bee55abeafd1f9150f3e2da",
            "test_good_source_data/metadata_in_tags",
            {},
            {
                "hca-dss-content-type": "text/plain",
                "hca-dss-crc32c": "114dee2c",
                "hca-dss-s3_etag": "7f54939b30ae7b6d45d473a4c82a41b0",
                "hca-dss-sha1": "15684690e8132044f378b4d4af8a7331c8da17b1",
                "hca-dss-sha256": "9cdc9050cecf59381fed55a2433140b69596fc861bee55abeafd1f9150f3e2da",
            }
        )

    # upload the "bad" source files
    uploader.upload_file(
        "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30",
        "test_bad_source_data/incorrect_checksum",
        {
            "hca-dss-content-type": "text/plain",
            "hca-dss-crc32c": "07b9e16e",
            "hca-dss-s3_etag": "55fc854ddc3c6bd573b83ef96387f146",
            "hca-dss-sha1": "fb4ba0588b8b6c4918902b8b815229aa8a61e483",
            "hca-dss-sha256": "756814053e847c10f3eb003417bc523d30cfc7749b96f63bd31c3c42b5c471bf",
        }
    )

    # upload the /blobs.
    uploader.upload_file(
        "9cdc9050cecf59381fed55a2433140b69596fc861bee55abeafd1f9150f3e2da",
        "blobs/9cdc9050cecf59381fed55a2433140b69596fc861bee55abeafd1f9150f3e2da.15684690e8132044f378b4d4af8a7331c8da17b1.7f54939b30ae7b6d45d473a4c82a41b0.114dee2c"  # noqa
    )
    uploader.upload_file(
        "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30",
        "blobs/cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30.2b8b815229aa8a61e483fb4ba0588b8b6c491890.3b83ef96387f14655fc854ddc3c6bd57.e16e07b9"  # noqa
    )

    # upload the /files.
    uploader.upload_file(
        "ce55fd51-7833-469b-be0b-5da88ebebfcd.2017-06-16T193604.240704Z",
        "files/ce55fd51-7833-469b-be0b-5da88ebebfcd.2017-06-16T193604.240704Z"
    )
    uploader.upload_file(
        "ce55fd51-7833-469b-be0b-5da88ebebfcd.2017-06-18T075702.020366Z",
        "files/ce55fd51-7833-469b-be0b-5da88ebebfcd.2017-06-18T075702.020366Z"
    )

    # upload the /bundles.
    uploader.upload_file(
        "011c7340-9b3c-4d62-bf49-090d79daf198.2017-06-20T214506.766634Z",
        "bundles/011c7340-9b3c-4d62-bf49-090d79daf198.2017-06-20T214506.766634Z"
    )

    # upload the files used for testList
    for ix in range(10):
        uploader.upload_file(
            "empty",
            "testList/prefix.{:03d}".format(ix)
        )
    uploader.upload_file(
        "empty",
        "testList/delimiter"
    )
    uploader.upload_file(
        "empty",
        "testList/delimiter/test"
    )

    for fname in ["assay.json", "cell.json", "manifest.json", "project.json", "sample.json"]:
        uploader.checksum_and_upload_file(
            f"test_api/{fname}",
            f"fixtures/test_api/bundle/{fname}",
            {
                "hca-dss-content-type": "application/javascript",
            },
        )

    # Create a bundle based on data-bundle-examples/smartseq2/paired_ends.
    # The files are accessed from the data-bundle-examples subrepository to avoid
    # duplicating them in our test infrastructure.
    # Then some non-indexed files are added for a more complete and realistic bundle test.
    for fname in ["assay.json", "cell.json", "manifest.json", "project.json", "sample.json"]:
        bundle_path = os.path.join(data_bundle_examples_dir, "smartseq2", "paired_ends")
        uploader.checksum_and_upload_file(
            f"{bundle_path}/{fname}",
            f"fixtures/smartseq2/paired_ends/{fname}",
            {
                "hca-dss-content-type": "application/json",
            },
        )
    for fname in ["text_data_file1.txt", "text_data_file2.txt"]:
        uploader.checksum_and_upload_file(
            f"indexing/{fname}",
            f"fixtures/smartseq2/paired_ends/{fname}",
            {
                "hca-dss-content-type": "text/plain",
            },
        )

if __name__ == '__main__':
    # find the 'datafiles' subdirectory.
    root_dir = os.path.dirname(__file__)
    datafiles_dir = os.path.join(root_dir, "datafiles")
    data_bundle_examples_dir = os.path.abspath(os.path.join(root_dir, "..", "..", "data-bundle-examples"))

    parser = argparse.ArgumentParser(description="Set up test fixtures in cloud storage buckets")
    parser.add_argument("--s3-bucket", type=str)
    parser.add_argument("--gs-bucket", type=str)

    args = parser.parse_args()

    uploaders = []
    if args.s3_bucket is not None:
        uploaders.append(S3Uploader(datafiles_dir, args.s3_bucket))
    if args.gs_bucket is not None:
        uploaders.append(GSUploader(datafiles_dir, args.gs_bucket))

    for uploader in uploaders:
        upload(uploader)
