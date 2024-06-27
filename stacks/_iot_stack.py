import aws_cdk as cdk
from aws_cdk import Stack
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kinesisfirehose_alpha as firehose
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_lakeformation as lakeformation
import aws_cdk.aws_glue as glue
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_kinesisfirehose_destinations_alpha as destinations
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as events_targets
import aws_cdk.aws_logs as logs
from constructs import Construct


class IotStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # iam
        s3_policy_document = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                    ],
                    resources=["*"],
                )
            ]
        )

        sqs_policy_document = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "sqs:*",
                    ],
                    resources=["*"],
                )
            ]
        )

        glue_role = iam.Role(
            self,
            "GlueCrawlerRole",
            role_name="glue_iam_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            inline_policies={
                "s3_policy": s3_policy_document,
                "sqs_policy": sqs_policy_document,
            },
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        # s3
        raw_data_bucket = s3.Bucket(
            self,
            "Raw Data",
            # bucket_name="raw-data-bucket",
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            event_bridge_enabled=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=cdk.Duration.days(7),
                        )
                    ]
                )
            ],
        )

        s3.Bucket(
            self,
            "Processed Data",
            # bucket_name="processed-data-bucket",
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        scripts_bucket = s3.Bucket(
            self,
            "Scripts Bucket",
            # bucket_name="glue-scripts-bucket",
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # kinesis
        s3_destination = destinations.S3Bucket(
            raw_data_bucket, compression=destinations.Compression.GZIP
        )

        firehose.DeliveryStream(
            self,
            "Delivery Stream",
            delivery_stream_name="Iot-stream",
            destinations=[s3_destination],
        )

        # sqs
        glue_queue = sqs.Queue(self, "Glue Queue")
        raw_data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(glue_queue),
        )

        # glue
        glue_database = glue.CfnDatabase(
            self,
            "Glue Database",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="temprature-database",
                description="Database to store IOT termprature records.",
            ),
        )

        lakeformation.CfnPermissions(
            self,
            "Lakeformation Permission",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=glue_role.role_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=glue_database.catalog_id, name="temprature-database"
                )
            ),
            permissions=["ALL"],
        )

        glue_crawler = glue.CfnCrawler(
            self,
            "Glue Crawler",
            name="glue_crawler",
            role=glue_role.role_arn,
            database_name="price-database",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{raw_data_bucket.bucket_name}/",
                        event_queue_arn=glue_queue.queue_arn,
                    )
                ]
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_EVENT_MODE"
            ),
        )
