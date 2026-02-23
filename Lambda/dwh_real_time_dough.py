"""
Lambda to read env parameters and list all objects in the S3 bucket BUCKET_DATA_TEAM_OLB_EXTRACTION.

Env parameters:
- BUCKET_DATA_TEAM_OLB_EXTRACTION
- TABLE_OLB_DICTIONARY_DOUGH
- TABLE_ACCOUNT_DAILY_BALANCE_DOUGH
- TABLE_INCOME_DAILY_EXPENDITURE
- TABLE_MEMBER_DAILY_BALANCE_DOUGH
- TABLE_CATEGORY_DAILY_SPENDING
- TABLE_BUDGET_MONTHLY_UTILIZATION
"""

import os
import json
from typing import Dict, Any, List, Optional

import boto3
from botocore.exceptions import ClientError


def get_env() -> Dict[str, Optional[str]]:
	"""Read expected env vars and return them in a dict."""
	keys = [
		"BUCKET_DATA_TEAM_OLB_EXTRACTION",
		"TABLE_OLB_DICTIONARY_DOUGH",
		"TABLE_ACCOUNT_DAILY_BALANCE_DOUGH",
		"TABLE_INCOME_DAILY_EXPENDITURE",
		"TABLE_MEMBER_DAILY_BALANCE_DOUGH",
		"TABLE_CATEGORY_DAILY_SPENDING",
		"TABLE_BUDGET_MONTHLY_UTILIZATION",
	]
	return {k: os.getenv(k) for k in keys}


def list_all_s3_objects(bucket: str, prefix: str = "") -> List[Dict[str, Any]]:
	"""
	List all objects in an S3 bucket (optionally under a prefix) using pagination.

	Returns a list of dicts with Key, Size, ETag, LastModified, StorageClass.
	"""
	s3 = boto3.client("s3")
	paginator = s3.get_paginator("list_objects_v2")
	items: List[Dict[str, Any]] = []
	try:
		for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
			for obj in page.get("Contents", []):
				items.append(
					{
						"Key": obj.get("Key"),
						"Size": obj.get("Size"),
						"ETag": obj.get("ETag"),
						"LastModified": obj.get("LastModified").isoformat() if obj.get("LastModified") else None,
						"StorageClass": obj.get("StorageClass"),
					}
				)
	except ClientError as e:
		# Surface error details for observability
		raise RuntimeError(f"Failed to list S3 objects in bucket '{bucket}' with prefix '{prefix}': {e}")
	return items


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
	"""
	Lambda handler: reads env, lists objects in BUCKET_DATA_TEAM_OLB_EXTRACTION, optional 'prefix' from event.
	"""
	env = get_env()
	bucket = env.get("BUCKET_DATA_TEAM_OLB_EXTRACTION")

	if not bucket:
		return {
			"statusCode": 400,
			"body": json.dumps({"error": "Missing env BUCKET_DATA_TEAM_OLB_EXTRACTION"}),
		}

	prefix = ""
	if isinstance(event, dict):
		prefix = (
			event.get("prefix")
			or event.get("queryStringParameters", {}).get("prefix", "")
			if event.get("queryStringParameters") is not None
			else event.get("prefix", "")
		)

	objects = list_all_s3_objects(bucket=bucket, prefix=prefix)
	return {
		"statusCode": 200,
		"body": json.dumps(
			{
				"bucket": bucket,
				"prefix": prefix,
				"count": len(objects),
				"objects": objects,
				"env": env,
			}
		),
	}


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
	"""Alias for AWS Lambda configured as module.handler."""
	return lambda_handler(event, context)

