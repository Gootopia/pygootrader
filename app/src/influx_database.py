from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from typing import Dict, List, Optional, Any
from docker_helper import DockerHelper


class InfluxDatabaseInfo:
    class EnvironmentVariables:
        Bucket = "DOCKER_INFLUXDB_INIT_BUCKET"
        Token = "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
        Org = "DOCKER_INFLUXDB_INIT_ORG"
    influxdb_container_name = "influxdb"
    localhost_url = "http://localhost:8086"
    
    def __init__(self, bucket: Optional[str]=None, token: Optional[str] = None, org: Optional[str] = None, url: Optional[str] = None):
        if DockerHelper.is_container_running(self.influxdb_container_name) == False:
            raise RuntimeError(f"Container {self.influxdb_container_name} is not running")

        # Variables are pulled from running container as it is assumed that only one instance should be needed
        if bucket is None:
            self.active_bucket = DockerHelper.get_container_env_var("influxdb", self.EnvironmentVariables.Bucket)
        else:
            self.active_bucket = bucket

        if org is None:
            self.org = DockerHelper.get_container_env_var("influxdb", self.EnvironmentVariables.Org)

        if token is None:
            self.token = DockerHelper.get_container_env_var("influxdb", self.EnvironmentVariables.Token)

        self.url = url if url is not None else self.localhost_url


class InfluxDatabase:
    def __init__(self, dbinfo: InfluxDatabaseInfo):
        self.dbinfo = dbinfo
        self.client = self.get_client(dbinfo.url, dbinfo.token, dbinfo.org)
        
    def get_client(self, url: str, token: str, org: str) -> InfluxDBClient:
        """Create InfluxDB client"""
        return InfluxDBClient(url=url, token=token, org=org)
    
    def write_record(self, measurement: str, tags: Optional[Dict[str, str]] = None, fields: Optional[Dict[str, Any]] = None) -> None:
        """Write a record to InfluxDB"""
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        point = Point(measurement)

        if tags:
            for key, value in tags.items():
                point.tag(key, value)

        if fields:
            for key, value in fields.items():
                point.field(key, value)

        active_bucket = self.active_bucket
        write_api.write(bucket=active_bucket, record=point)
    
    def read_records(self, query: Optional[str] = None) -> List[Dict[str, Any]]:
        """Read records from InfluxDB using Flux query"""
        if query is None:
            raise ValueError("Query cannot be None")
        
        query_api = self.client.query_api()
        result = query_api.query(query=query)

        records = []
        for table in result:
            for record in table.records:
                records.append(record.values)

        return records
    
    @property
    def active_bucket(self) -> str:
        """Get the active bucket"""
        return self.dbinfo.active_bucket
    
    @active_bucket.setter
    def active_bucket(self, bucket: str) -> None:
        """Set the active bucket for database operations"""
        buckets_api = self.client.buckets_api()
        bucket_exists = buckets_api.find_bucket_by_name(bucket)
        
        if bucket_exists is None:
            raise ValueError(f"Bucket '{bucket}' does not exist")
        
        self.dbinfo.active_bucket = bucket


if __name__ == "__main__":
    db_info = InfluxDatabaseInfo()
    db = InfluxDatabase(db_info)  
    print("done")