import json
from typing import Dict, Any

class Storage:
    def __init__(self, backend: str, bucket: str, **kwargs):
        self.backend = backend
        self.bucket = bucket
        if backend == "gcs":
            from google.cloud import storage as gcs
            self.client = gcs.Client()
            bucket_name = bucket.replace("gs://", "")
            self.bucket_ref = self.client.bucket(bucket_name)
        elif backend == "s3":
            import boto3
            self.client = boto3.client("s3",
                endpoint_url=kwargs.get("endpoint_url"),
                aws_access_key_id=kwargs.get("aws_access_key_id"),
                aws_secret_access_key=kwargs.get("aws_secret_access_key"),
                region_name=kwargs.get("region_name"))
        else:
            raise ValueError("Unsupported backend")

    def _path(self, patch: str, region: str, match_id: str, kind: str) -> str:
        ## Have to get our folders named correctly for various word endings
        if kind == "match":
            folder = "matches"
        elif kind == "timeline":
            folder = "timelines"
        else:
            folder = f"{kind}s"
        return f"raw/{patch}/{region}/{folder}/{match_id}.json"


    def write_json(self, patch: str, region: str, match_id: str, kind: str, data: Dict[str, Any]):
        key = self._path(patch, region, match_id, kind)
        payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if self.backend == "gcs":
            blob = self.bucket_ref.blob(key)
            blob.upload_from_string(payload, content_type="application/json")
        else:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=payload, ContentType="application/json") # pyright: ignore[reportAttributeAccessIssue]
        return key
