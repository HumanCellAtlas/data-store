import json
import os
import subprocess
import typing


class Uploader(object):
    def __init__(self, local_root: str) -> None:
        self.local_root = local_root

    def reset(self) -> None:
        raise NotImplementedError()

    def upload_file(
            self,
            local_path: str,
            remote_path: str,
            metadata_keys: typing.Dict[str, str]=None,
            *args,
            **kwargs) -> None:
        raise NotImplementedError()


class S3Uploader(Uploader):
    def __init__(self, local_root: str, bucket: str) -> None:
        super(S3Uploader, self).__init__(local_root)
        self.bucket = bucket

    def reset(self) -> None:
        args = [
            "aws",
            "s3",
            "rm",
            "s3://{}".format(self.bucket),
            "--recursive"
        ]
        subprocess.check_call(args)

    def upload_file(
            self,
            local_path: str,
            remote_path: str,
            metadata_keys: typing.Dict[str, str]=None,
            tags: typing.Dict[str, str]=None,
            *args,
            **kwargs) -> None:
        if metadata_keys is None:
            metadata_keys = dict()
        if tags is None:
            tags = dict()
        subprocess_args = [
            "aws", "s3", "cp",
            os.path.join(self.local_root, local_path),
            "s3://{}/{}".format(self.bucket, remote_path),
            "--metadata",
            json.dumps(metadata_keys)
        ]
        subprocess.check_call(subprocess_args)

        tagset = dict(TagSet=[])  # type: typing.Dict[str, typing.List[dict]]
        for tag_key, tag_value in tags.items():
            tagset['TagSet'].append(
                dict(
                    Key=tag_key,
                    Value=tag_value))
        subprocess_args = [
            "aws",
            "s3api",
            "put-object-tagging",
            "--bucket", self.bucket,
            "--key", remote_path,
            "--tagging", json.dumps(tagset)]
        subprocess.check_call(subprocess_args)


class GSUploader(Uploader):
    def __init__(self, local_root: str, bucket: str) -> None:
        super(GSUploader, self).__init__(local_root)
        self.bucket = bucket

    def reset(self) -> None:
        args = [
            "gsutil",
            "-m",
            "rm",
            "-r",
            "gs://{}/**".format(self.bucket),
        ]

        # This is not a check_call because of https://github.com/GoogleCloudPlatform/gsutil/issues/417
        subprocess.call(args)

    def upload_file(
            self,
            local_path: str,
            remote_path: str,
            metadata_keys: typing.Dict[str, str]=None,
            *args,
            **kwargs) -> None:
        if metadata_keys is None:
            metadata_keys = dict()
        subprocess_args = [
            "gsutil",
        ]
        for metadata_key, metadata_value in metadata_keys.items():
            subprocess_args.extend([
                "-h",
                "x-goog-meta-{}:{}".format(metadata_key, metadata_value)
            ])

        subprocess_args.extend([
            "cp",
            os.path.join(self.local_root, local_path),
            "gs://{}/{}".format(self.bucket, remote_path),
        ])
        subprocess.check_call(subprocess_args)
