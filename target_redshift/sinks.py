"""Redshift target sink class, which handles writing streams."""

from __future__ import annotations

import csv
import datetime
import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

import boto3
import simplejson as json
import smart_open
import sqlalchemy
from botocore.exceptions import ClientError
from singer_sdk.helpers._compat import (
    date_fromisoformat,
    datetime_fromisoformat,
    time_fromisoformat,
)
from singer_sdk.helpers._typing import (
    DatetimeErrorTreatmentEnum,
    get_datelike_property_type,
    handle_invalid_timestamp_in_record,
)
from singer_sdk.sinks import SQLSink
from singer_sdk.typing import _jsonschema_type_check

from target_redshift.connector import RedshiftConnector

if TYPE_CHECKING:
    from redshift_connector import Cursor


class RedshiftSink(SQLSink):
    """Redshift target sink class."""

    connector_class = RedshiftConnector
    MAX_SIZE_DEFAULT = 50000

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize SQL Sink. See super class for more details."""
        super().__init__(*args, **kwargs)
        self.temp_table_name = self.generate_temp_table_name()

        region = self.config.get("s3_region")
        if region is None:
            self.s3_client = boto3.client("s3")
        else:
            self.s3_client = boto3.client("s3", region_name=region)

    @property
    def schema_name(self) -> str | None:
        """Return the schema name or `None` if using names with no schema part.

        Returns:
            The target schema name.
        """
        # Look for a default_target_scheme in the configuraion fle
        default_target_schema: str = self.config.get("default_target_schema", os.getenv("MELTANO_EXTRACT__LOAD_SCHEMA"))
        parts = self.stream_name.split("-")

        # 1) When default_target_scheme is in the configuration use it
        # 2) if the streams are in <schema>-<table> format use the
        #    stream <schema>
        # 3) Return None if you don't find anything
        if default_target_schema:
            return default_target_schema
        return self.conform_name(parts[-2], "schema") if len(parts) in {2, 3} else None

    @property
    def conformed_schema(self) -> dict:
        """Return the stream schema, conformed to SQL naming standards.

        Returns:
            The target stream schema.
        """
        return self.conform_schema(self.schema)

    def setup(self) -> None:
        """Set up Sink.

        This method is called on Sink creation, and creates the required Schema and
        Table entities in the target database.
        """
        if self.key_properties is None or self.key_properties == []:
            self.append_only = True
        else:
            self.append_only = False
        with self.connector.connect_cursor() as cursor:
            if self.schema_name:
                self.connector.prepare_schema(self.schema_name, cursor=cursor)
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.conformed_schema,
                primary_keys=self.key_properties,
                cursor=cursor,
                as_temp_table=False,
            )
            self.connector.grant_privileges(self.schema_name, cursor=cursor)

    def generate_temp_table_name(self) -> str:
        """Uuid temp table name."""
        # sqlalchemy.exc.IdentifierError: Identifier  # noqa: ERA001
        # 'temp_test_optional_attributes_388470e9_fbd0_47b7_a52f_d32a2ee3f5f6'
        # exceeds maximum length of 63 characters
        # Is hit if we have a long table name, there is no limit on Temporary tables
        # in postgres, used a guid just in case we are using the same session
        return f"{str(uuid.uuid4()).replace('-', '_')}"

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.

        Args:
            context: Stream partition or context dictionary.
        """
        # If duplicates are merged, these can be tracked via
        # :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.
        with self.connector.connect_cursor() as cursor:
            # Get target table
            table: sqlalchemy.Table = self.connector.get_table(full_table_name=self.full_table_name)
            # Create a temp table (Creates from the table above)
            temp_table: sqlalchemy.Table = self.connector.copy_table_structure(
                full_table_name=self.temp_table_name,
                from_table=table,
                as_temp_table=True,
                cursor=cursor,
            )
            # Insert into temp table
            self.bulk_insert_records(
                table=temp_table,
                records=context["records"],
                cursor=cursor,
            )
            self.logger.info(f'merging {len(context["records"])} records into {table}')  # noqa: G004
            # Merge data from temp table to main table
            self.upsert(
                from_table=temp_table,
                to_table=table,
                join_keys=self.key_properties,
                cursor=cursor,
            )
            # clean_resources
        self.clean_resources()

    def s3_uri(self) -> str:
        """Return the s3 uri.

        Returns:
            The target s3 uri.
        """
        return f's3://{self.config["s3_bucket"]}/{self.s3_key()}'

    def s3_key(self) -> str:
        """Creates and returns an S3 key for the message.

        Returns:
            The target s3 key.
        """
        filename = Path(f"{self.stream_name}-{self.temp_table_name}.csv")

        if self.config.get("s3_key_prefix"):
            key = Path(self.config["s3_key_prefix"]) / filename
            return str(key)

        prefix = self.config.get("s3_key_prefix_naming_convention", "")

        batched_at = self._get_context({}).get("batch_start_time") or datetime.datetime.now(tz=datetime.timezone.utc)

        tokens = {
            "{stream}": self.stream_name,
            "{timestamp}": batched_at.strftime("%Y%m%dT%H%M%S"),
            "{date}": batched_at.strftime("%Y%m%d"),
        }

        for k, v in tokens.items():
            if k in prefix:
                prefix = prefix.replace(k, v)

        key = Path(prefix) / filename
        return str(key)

    def bulk_insert_records(  # type: ignore[override]
        self,
        table: sqlalchemy.Table,
        records: Iterable[dict[str, Any]],
        cursor: Cursor,
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            table: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.
            cursor: the redshift connector cursor.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        self.write_to_s3(records)
        self.copy_to_redshift(table, cursor)
        return True

    def upsert(
        self,
        from_table: sqlalchemy.Table,
        to_table: sqlalchemy.Table,
        join_keys: list[str],
        cursor: Cursor,
    ) -> None:
        """Merge upsert data from one table to another.

        Args:
            from_table: The source table.
            to_table: The destination table.
            schema: Singer Schema message.
            join_keys: The merge upsert keys, or `None` to append.
            cursor: The database cursor.

        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.

        """
        join_predicates = []
        to_table_key: sqlalchemy.Column
        for key in join_keys:
            from_table_key: sqlalchemy.Column = from_table.columns[key]
            to_table_key = to_table.columns[key]
            join_predicates.append(from_table_key == to_table_key)

        join_condition = sqlalchemy.and_(*join_predicates)
        if len(join_keys) > 0:
            sql = f"""
                MERGE INTO {self.connector.quote(str(to_table))}
                USING {self.connector.quote(str(from_table))}
                ON {join_condition}
                REMOVE DUPLICATES
                """
        else:
            sql = f"""
                INSERT INTO {self.connector.quote(str(to_table))}
                SELECT * FROM {self.connector.quote(str(from_table))}
                """  # noqa: S608
        cursor.execute(sql)

    def format_records_as_csv(self, records: Iterable[dict[str, Any]]) -> list[dict]:
        """Write records to a local csv file.

        Parameters
        ----------
        records : List[dict]
            the input records.

        Returns:
        -------
        None

        Raises:
        ------
        ValueError
            _description_
        """
        if "properties" not in self.schema:
            msg = "Stream's schema has no properties defined."
            raise ValueError(msg)
        object_keys = [
            key
            for key, value in self.conformed_schema["properties"].items()
            if _jsonschema_type_check(value, ("object", "array"))
        ]
        return [
            {
                key: (json.dumps(value).replace("None", "") if key in object_keys else value)
                for key, value in self.conform_record(record).items()
            }
            for record in records
        ]

    def write_to_s3(self, records: Iterable[dict[str, Any]]) -> None:
        """Write the csv file to s3."""
        records = self.format_records_as_csv(records)
        keys: list[str] = list(self.conformed_schema["properties"].keys())

        msg = f"writing {len(records)} records to {self.s3_uri()}"
        self.logger.info(msg)

        with smart_open.open(self.s3_uri(), "w") as fp:
            writer = csv.DictWriter(
                fp,
                fieldnames=keys,
                extrasaction="ignore",
                dialect="excel",
            )
            writer.writerows(records)

    def copy_to_redshift(self, table: sqlalchemy.Table, cursor: Cursor) -> None:
        """Copy the s3 csv file to redshift."""
        copy_credentials = f"IAM_ROLE '{self.config['aws_redshift_copy_role_arn']}'"

        # Step 3: Generate copy options - Override defaults from config.json if defined
        copy_options = self.config.get(
            "copy_options",
            """
            EMPTYASNULL BLANKSASNULL TRIMBLANKS TRUNCATECOLUMNS
            DATEFORMAT 'auto' TIMEFORMAT 'auto'
            COMPUPDATE OFF STATUPDATE OFF
        """,
        )
        columns = ", ".join([f'"{column}"' for column in self.conformed_schema["properties"]])
        # Step 4: Load into the stage table
        copy_sql = f"""
            COPY {self.connector.quote(str(table))} ({columns})
            FROM '{self.s3_uri()}'
            {copy_credentials}
            {copy_options}
            CSV
        """
        cursor.execute(copy_sql)

    def parse_timestamps_in_record(
        self,
        record: dict,
        schema: dict,
        treatment: DatetimeErrorTreatmentEnum,
    ) -> None:
        """Parse strings to datetime.datetime values, repairing or erroring on failure.

        Attempts to parse every field that is of type date/datetime/time. If its value
        is out of range, repair logic will be driven by the `treatment` input arg:
        MAX, NULL, or ERROR.

        Args:
            record: Individual record in the stream.
            schema: TODO
            treatment: TODO
        """
        for key, value in record.items():
            if key not in schema["properties"]:
                if value is not None:
                    self.logger.warning("No schema for record field '%s'", key)
                continue
            datelike_type = get_datelike_property_type(schema["properties"][key])
            if datelike_type:
                date_val = value
                try:
                    if value is not None:
                        if datelike_type == "time":
                            date_val = time_fromisoformat(date_val)
                        elif datelike_type == "date":
                            date_val = date_fromisoformat(date_val)
                        else:
                            date_val = datetime_fromisoformat(date_val)
                except ValueError as ex:
                    date_val = handle_invalid_timestamp_in_record(
                        record,
                        [key],
                        date_val,
                        datelike_type,
                        ex,
                        treatment,
                        self.logger,
                    )
                record[key] = date_val

    def clean_resources(self) -> None:
        """Remove local and s3 resources."""
        if self.config["remove_s3_files"]:
            try:
                _ = self.s3_client.delete_object(Bucket=self.config["s3_bucket"], Key=self.s3_key())
            except ClientError:
                self.logger.exception()
