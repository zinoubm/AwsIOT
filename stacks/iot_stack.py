from aws_cdk import Stack
import aws_cdk.aws_kinesisfirehose_alpha as firehose
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_kinesisfirehose_destinations_alpha as destinations
from constructs import Construct


class IotStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(self, "Bucket")
        s3_destination = destinations.S3Bucket(
            bucket, compression=destinations.Compression.SNAPPY
        )
        firehose.DeliveryStream(self, "Delivery Stream", destinations=[s3_destination])
