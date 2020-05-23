#!/opt/conda/bin/python
import boto3
import config as c

emr = boto3.client('emr',
                     region_name=c.REGION_NAME,
                     aws_access_key_id=c.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=c.AWS_SECRET_ACCESS_KEY
)

# get cluster id:
job_flow_id = input('Enter Cluster id j-: ')

# Terminate cluster
response = emr.terminate_job_flows(
    JobFlowIds=[
        job_flow_id
    ]
)
print("Termination initiated for Cluster", job_flow_id)