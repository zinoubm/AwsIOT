import sys
import datetime
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import json

# from awsglue.context import GlueContext
# from pyspark.context import SparkContext
import logging


class Utils:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.glue_client = boto3.client("glue")
        self.event_client = boto3.client("cloudtrail")

        self.args = getResolvedOptions(sys.argv, ["WORKFLOW_NAME", "WORKFLOW_RUN_ID"])
        self.event_id = self.glue_client.get_workflow_run_properties(
            Name=self.args["WORKFLOW_NAME"], RunId=self.args["WORKFLOW_RUN_ID"]
        )["RunProperties"]["aws:eventIds"][1:-1]

        self.events = self.event_client.lookup_events(
            LookupAttributes=[
                {"AttributeKey": "EventName", "AttributeValue": "NotifyEvent"}
            ],
            StartTime=(datetime.datetime.now() - datetime.timedelta(minutes=5)),
            EndTime=datetime.datetime.now(),
        )[
            "Events"
        ]  # events for last 5 minutes

        self.sc = SparkContext()
        glueContext = GlueContext(self.sc)
        self.spark = glueContext.spark_session

    def get_s3_object(self):
        for event in self.events:
            event_payload = json.loads(event["CloudTrailEvent"])["requestParameters"][
                "eventPayload"
            ]
            if event_payload["eventId"] == self.event_id:
                self.object_key = json.loads(event_payload["eventBody"])["detail"][
                    "object"
                ]["key"]
                self.bucket_name = json.loads(event_payload["eventBody"])["detail"][
                    "bucket"
                ]["name"]

        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.object_key)
        s3_path = f"s3://{self.bucket_name}/{self.object_key}"

        return self.spark.read.json(s3_path, multiLine=True)

    def log_args(self):
        # sc = SparkContext()
        # sc.setLogLevel("DEBUG")
        # glueContext = GlueContext(sc)
        # logger = glueContext.get_logger()

        print("args", self.args)
        print("event_id", self.event_id)
        print("events", self.events)


utils = Utils()
# utils.log_args()
df = utils.get_s3_object()
df.show(n=5)
