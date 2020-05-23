#!/opt/conda/bin/python
import boto3
import config as c

emr = boto3.client('emr',
                     region_name=c.REGION_NAME,
                     aws_access_key_id=c.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=c.AWS_SECRET_ACCESS_KEY
)

# Add steps here:
ListofSteps=[
#       {
#            'Name': 'Create_SL_Datalake',
#            'ActionOnFailure': 'TERMINATE_CLUSTER',
#            'HadoopJarStep': {
#                'Jar': 'command-runner.jar',
#                'Args': [
#                    'spark-submit', '/home/hadoop/etl.py'
#                ]
#            }
#        },
#        {
#            'Name': 'New Step Name',
#            'ActionOnFailure': 'CONTINUE',
#            'HadoopJarStep': {
#                'Jar': 'command-runner.jar',
#                'Args': [
#                    'argForNewStep'
#                ]
#            }
#        } 
    ]

# get cluster id:
job_flow_id = input('Enter Cluster id j-: ')
print("Job flow ID:", job_flow_id)

# Add aditional steps
step_response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=ListofSteps)
step_ids = step_response['StepIds']

print("Added Step IDs:", step_ids)