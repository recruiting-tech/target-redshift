"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t

from singer_sdk.testing import get_target_test_class
from singer_sdk.testing.suites import TestSuite
from singer_sdk.testing.target_tests import (
    TargetArrayData,
    TargetCamelcaseComplexSchema,
    TargetCamelcaseTest,
    TargetCliPrintsTest,
    TargetDuplicateRecords,
    TargetInvalidSchemaTest,
    TargetNoPrimaryKeys,
    TargetOptionalAttributes,
    TargetRecordBeforeSchemaTest,
    TargetRecordMissingKeyProperty,
    TargetRecordMissingOptionalFields,
    TargetSchemaNoProperties,
    TargetSchemaUpdates,
    TargetSpecialCharsInAttributes,
)

from target_redshift.target import TargetRedshift

redshift_config: dict[str, t.Any] = {
    "aws_redshift_copy_role_arn": "arn:aws:iam::573569180693:role/AllowRedshiftToAccessS3Bucket",
    "flattening_enabled": True,
    "flattening_max_depth": 5,
    "dbname": "reporting",
    "s3_bucket": "ticketswap-redshift-reporting",
    "s3_key_prefix": "meltano-staging/",
    "temp_dir": ".meltano/temp",
    "user": "tobias",
    "enable_iam_authentication": True,
    "cluster_identifier": "reporting-cluster",
    "host": "localhost",
    "port": "5001",
    "default_target_schema": "target_redshift_test",
    "add_record_metadata": True,
    "ssl_mode": "prefer",
    "ssl_enable": True,
}

target_tests = TestSuite(
    kind="target",
    tests=[
        TargetArrayData,
        TargetCamelcaseComplexSchema,
        TargetCamelcaseTest,
        TargetCliPrintsTest,
        TargetDuplicateRecords,
        TargetInvalidSchemaTest,
        TargetNoPrimaryKeys,
        TargetOptionalAttributes,
        TargetRecordBeforeSchemaTest,
        TargetRecordMissingKeyProperty,
        TargetSchemaNoProperties,
        TargetSchemaUpdates,
        TargetSpecialCharsInAttributes,
        TargetRecordMissingOptionalFields,
    ],
)

# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetRedshift,
    config=redshift_config,
    custom_suites=[target_tests],
    suite_config=None,
    include_target_tests=False,
)


class TestTargetRedshift(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""
