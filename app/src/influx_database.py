# Built-in packages
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any, Union

# Third-party packages
import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from loguru import logger

# Custom packages
from docker_helper import DockerHelper
from tags import TagGroup, InstrumentTags

INFLUX_DB_DOCKER_IMAGE = "influxdb:latest"

class Attribute(str, Enum):
    Measurements = "measurements"
    Fields = "fieldKeys"
    Tags = "tagKeys"
class InfluxDatabaseInfo:
    class EnvironmentVariables:
        Bucket = "DOCKER_INFLUXDB_INIT_BUCKET"
        Token = "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
        Org = "DOCKER_INFLUXDB_INIT_ORG"
    
    INFLUXDB_CONTAINER_NAME = "influxdb"
    LOCALHOST_URL = "http://localhost:8086"
    
    def __init__(self, 
                 bucket: Optional[str] = None, 
                 token: Optional[str] = None, 
                 org: Optional[str] = None, 
                 url: Optional[str] = None):
        container_name = self.INFLUXDB_CONTAINER_NAME
        is_running = DockerHelper.is_container_running(container_name)
        if not is_running:
            logger.info(f"Starting container: {container_name}")
            DockerHelper.start_service(service_name=container_name)
        else:
            logger.info(f"Container {container_name} is already running")

        # Variables are pulled from running container as it is assumed that only one instance should be needed
        if bucket is None:
            self.active_bucket = DockerHelper.get_container_env_var("influxdb", self.EnvironmentVariables.Bucket)
        else:
            self.active_bucket = bucket

        if org is None:
            self.org = DockerHelper.get_container_env_var("influxdb", self.EnvironmentVariables.Org)

        if token is None:
            self.token = DockerHelper.get_container_env_var("influxdb", self.EnvironmentVariables.Token)

        self.url = url if url is not None else self.LOCALHOST_URL


@dataclass
class InfluxQuery:
    _active_database = None
    _query_range: Optional[str] = None
    _query_tags: List[str] = field(default_factory=list)
    _query_fields: List[str] = field(default_factory=list)
    
    def range(self, start: Optional[str] = None, stop: Optional[str] = None):
        if start is None and stop is None:
            range_part = "range(start: 0)"
        else:
            range_part = "range("
            if start:
                range_part += f"start: {start}"
            if stop:
                if start:
                    range_part += f", stop: {stop}"
                else:
                    range_part += f"stop: {stop}"
            range_part += ")"
        self._query_range = range_part
        return self
    
    def add_tag(self, key: str, value: str):
        self._query_tags.append(f'filter(fn: (r) => r["{key}"] == "{value}")')
        return self
    
    def add_tag_group(self, tag_group: TagGroup):
        tag_dict = tag_group.to_dict()
        for key, value in tag_dict.items():
            self._query_tags.append(f'filter(fn: (r) => r["{key}"] == "{value}")')
        return self
    
    def add_field(self, value: str):
        self._query_fields.append(f'filter(fn: (r) => r["_field"] == "{value}")')
        return self
    
    @classmethod
    def set_active_database(cls, database):
        cls._active_database = database
    
    def build(self, database=None) -> str:
        if database is None:
            database = self._active_database
            assert database is not None, "No database specified and no active database set"
        
        query = f'from(bucket: "{database.active_bucket}")'
        if self._query_range is not None:
            query += f' |> {self._query_range}'
        if self._query_tags:
            for tag_filter in self._query_tags:
                query += f' |> {tag_filter}'
        if self._query_fields:
            for field_filter in self._query_fields:
                query += f' |> {field_filter}'
        return query.lower()


class InfluxDatabase(InfluxDatabaseInfo):
    DEFAULT_TIMESTAMP_KEY = "date"
    
    def __init__(self, 
                 bucket: Optional[str] = None, 
                 token: Optional[str] = None, 
                 org: Optional[str] = None, 
                 url: Optional[str] = None):
        super().__init__(bucket, token, org, url)
        self.client = self._get_client(self.url, self.token, self.org)
    
    def read_records(self, 
                     query: Optional[str] = None,
                     return_dataframe: bool = True) -> Union[List[Dict[str, Any]], pd.DataFrame]:
        """Read records from InfluxDB using Flux query"""
        if query is None:
            raise ValueError("Query cannot be None")
        
        formatted_query = query.replace(' |> ', '\n |> ')
        logger.info(f"Executing query:\n{formatted_query}")
        query_api = self.client.query_api()
        result = query_api.query(query=query)

        records = []
        for table in result:
            for record in table.records:
                records.append(record.values)

        logger.info(f"Query returned {len(records)} records")
        if return_dataframe and len(records) > 0:
            logger.info("Converting records to Pandas dataframe")
            records_df = self.convert_to_pandas(records)
            return records_df
        
        return records
    
    def convert_to_pandas(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        extracted_data = []
        for record in data:
            extracted_data.append({
                '_time': record.get('_time'),
                '_field': record.get('_field'),
                '_value': record.get('_value')
            })
        df_long = pd.DataFrame(extracted_data)
        df_wide = df_long.pivot(index='_time', columns='_field', values='_value').reset_index()
        return df_wide
    
    def set_active_bucket(self, 
                          bucket: str) -> None:
        """Set the active bucket for database operations"""
        buckets_api = self.client.buckets_api()
        bucket_exists = buckets_api.find_bucket_by_name(bucket)
        
        if bucket_exists is None:
            raise ValueError(f"Bucket '{bucket}' does not exist")
        
        self.active_bucket = bucket
    
    def write_pandas(self, 
                     dataframe: pd.DataFrame, 
                     fields: Optional[List[str]] = None,
                     tags: Optional[Union[Dict[str, str], TagGroup]] = None,
                     timestamp_key: str = DEFAULT_TIMESTAMP_KEY) -> int:
        """Write pandas DataFrame to InfluxDB"""
        assert timestamp_key in dataframe.columns, f"Timestamp key '{timestamp_key}' not found in dataframe columns"
        field_keys = [col for col in dataframe.columns if col != timestamp_key]
        
        # Convert TagGroup to dict if needed
        tags_dict = tags.to_dict() if hasattr(tags, 'to_dict') else tags
        
        ingestion_info = {
            "rows": len(dataframe),
            "timestamp_key": timestamp_key,
            "tags": tags_dict,
            "fields": field_keys
        }
        logger.info(f"Ingesting data: {json.dumps(ingestion_info, indent=2)}")
        
        records_written = 0
        for index, row in dataframe.iterrows():
            timestamp = datetime.fromtimestamp(row[timestamp_key] / 1000)
            field_values = {key: row[key] for key in field_keys}
            self._write_record("stock_data", tags=tags_dict, fields=field_values, timestamp=timestamp)
            records_written += 1
        
        logger.info(f"Successfully wrote {records_written} records to InfluxDB")
        return records_written
    
    def get_connection_status(self) -> bool:
        """Check if InfluxDB is accessible"""
        logger.info("Checking InfluxDB connection status")
        is_db_connected = False
        
        try:
            ping_result = self.client.ping()
            is_db_connected = ping_result is not None
        except Exception:
            logger.exception("InfluxDB connection check failed")
        
        logger.info(f"InfluxDB connection status: {'Connected' if is_db_connected else 'Disconnected'}")
        
        return is_db_connected
    
    def get_attributes(self, bucket: str, attribute_type: Attribute) -> List[str]:
        """Get database attributes (measurements, fields, or tags) from the specified bucket"""
        query = f'import "influxdata/influxdb/schema"\nschema.{attribute_type.value}(bucket: "{bucket}")'
        query_api = self.client.query_api()
        result = query_api.query(query=query)
        
        attributes = []
        for table in result:
            for record in table.records:
                attributes.append(record.get_value())
        
        return attributes
    
    def get_tag_values(self, bucket: str, tag_name: str) -> List[str]:
        """Get all values for a specific tag from the specified bucket"""
        assert self._check_tag_exist(bucket, tag_name), f"Tag '{tag_name}' does not exist in bucket '{bucket}'"
        
        query = f'import "influxdata/influxdb/schema"\nschema.tagValues(bucket: "{bucket}", tag: "{tag_name}")'
        query_api = self.client.query_api()
        result = query_api.query(query=query)
        
        tag_values = []
        for table in result:
            for record in table.records:
                tag_values.append(record.get_value())
        
        return tag_values
    
    def _check_tag_exist(self, bucket: str, tag_name: str) -> bool:
        """Check if a tag exists in the specified bucket"""
        tags = self.get_attributes(bucket, Attribute.Tags)
        return tag_name in tags
    
    def _get_client(self, 
                    url: str, 
                    token: str, 
                    org: str) -> InfluxDBClient:
        """Create InfluxDB client"""
        db_client = InfluxDBClient(url=url, token=token, org=org)
        if db_client is not None:
            logger.info(f"InfluxDB connection success: {url}")
        else:
            logger.error(f"InfluxDB connection failure: {url}")
        return db_client
    
    def _write_record(self, 
                      measurement: str, 
                      tags: Optional[Dict[str, str]] = None, 
                      fields: Optional[Dict[str, Any]] = None,
                      timestamp: Optional[Union[str, datetime]] = None) -> None:
        """Write a record to InfluxDB"""
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        point = Point(measurement)

        if tags:
            for key, value in tags.items():
                point.tag(key, value)

        if fields:
            for key, value in fields.items():
                point.field(key, value)
        
        if timestamp is None:
            point.time(datetime.now(timezone.utc))
        else:
            point.time(timestamp.astimezone(timezone.utc))

        active_bucket = self.active_bucket
        write_api.write(bucket=active_bucket, record=point)


if __name__ == "__main__":
    db = InfluxDatabase()
    m = db.get_attributes(db.active_bucket, Attribute.Measurements)
    f = db.get_attributes(db.active_bucket, Attribute.Fields)
    t = db.get_attributes(db.active_bucket, Attribute.Tags)
    query1 = InfluxQuery().range().add_tag("symbol", "spy").build(db)
    tags = InstrumentTags(symbol="spy")
    query2 = InfluxQuery().range().add_tag_group(tags).build(db)  
    data_df = db.read_records(query1)
    print(data_df)
    print("done")