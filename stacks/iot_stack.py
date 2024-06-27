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

        # kinesis
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

        s3_destination = destinations.S3Bucket(bucket=raw_data_bucket)

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

        s3deploy.BucketDeployment(
            self,
            "Deployment",
            sources=[s3deploy.Source.asset("./scripts/")],
            destination_bucket=scripts_bucket,
        )

        glue_role = iam.Role(
            self,
            "Glue Role",
            role_name="glue-role",
            description="Role for Glue services to access S3",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            inline_policies={
                "glue_policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:*",
                                "glue:*",
                                "iam:*",
                                "logs:*",
                                "cloudwatch:*",
                                "sqs:*",
                                "ec2:*",
                                "cloudtrail:*",
                            ],
                            resources=["*"],
                        )
                    ]
                )
            },
        )

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
            name="glue-crawler",
            role=glue_role.role_arn,
            database_name="temprature-database",
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

        glue_job = glue.CfnJob(
            self,
            "Glue Job",
            name="glue-job",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/etl_script.py",
            ),
            role=glue_role.role_arn,
            glue_version="3.0",
            timeout=3,
        )

        glue_workflow = glue.CfnWorkflow(
            self,
            "Glue Workflow",
            name="glue-workflow",
            description="Workflow to process the IOT termprature data.",
        )

        glue.CfnTrigger(
            self,
            "Glue Crawler Trigger",
            name="glue-crawler-trigger",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    crawler_name="glue-crawler",
                    notification_property=glue.CfnTrigger.NotificationPropertyProperty(
                        notify_delay_after=3
                    ),
                    timeout=3,
                )
            ],
            type="EVENT",
            workflow_name=glue_workflow.name,
        )

        glue.CfnTrigger(
            self,
            "Glue Job Trigger",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=glue_job.name,
                    notification_property=glue.CfnTrigger.NotificationPropertyProperty(
                        notify_delay_after=3
                    ),
                    timeout=3,
                )
            ],
            type="CONDITIONAL",
            start_on_creation=True,
            workflow_name=glue_workflow.name,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        crawler_name="glue-crawler",
                        logical_operator="EQUALS",
                        crawl_state="SUCCEEDED",
                    )
                ]
            ),
        )

        # event bridge
        events_rule_role = iam.Role(
            self,
            "EventBridge Role",
            role_name="event-bridge-role",
            description="Role for EventBridge to trigger Glue workflows.",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
            inline_policies={
                "eventbridge_policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["events:*", "glue:*"],
                            resources=["*"],
                        )
                    ]
                )
            },
        )

        events.CfnRule(
            self,
            "Iot Event Role",
            # event_pattern=events.EventPattern(
            #     source=["aws.s3"],
            # ),
            targets=[
                events.CfnRule.TargetProperty(
                    arn=f"arn:aws:glue:us-east-2:{cdk.Aws.ACCOUNT_ID}:workflow/glue-workflow",
                    # arn=glue_workflow.arn,
                    role_arn=events_rule_role.role_arn,
                    id=cdk.Aws.ACCOUNT_ID,
                )
            ],
            event_pattern={
                "source": ["aws.s3"],
                "detail-type": ["Object Created"],
                "detail": {"bucket": {"name": [f"{raw_data_bucket.bucket_name}"]}},
            },
        )
