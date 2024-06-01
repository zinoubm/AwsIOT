#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.iot_stack import IotStack


app = cdk.App()
IotStack(app, "IotStack",
    # Uncomment the next line if you know exactly what Account and Region you
    # want to deploy the stack to. */

    #env=cdk.Environment(account='123456789012', region='us-east-1'),
    )

app.synth()
