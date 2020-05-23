#!/opt/conda/bin/python
import boto3
import config as c

emr = boto3.client('emr',
                     region_name=c.REGION_NAME,
                     aws_access_key_id=c.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=c.AWS_SECRET_ACCESS_KEY
)


print('CREATING CLUSTER:')
response = emr.run_job_flow(
    Name=c.CLUSTER_NAME,
    LogUri='s3://aws-logs-'+ c.ACCOUNT_NUM + '-' + c.REGION_NAME + '/elasticmapreduce/',
    ReleaseLabel='emr-5.29.0',
    Instances={
        'MasterInstanceType': c.MASTERINSTANCETYPE,
        'SlaveInstanceType': c.SLAVEINSTANCETYPE,
        'InstanceCount': c.INSTANCECOUNT,
        'Ec2KeyName': c.EC2KEYNAME,
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'HadoopVersion': '2.8.5',
        
        # For using EMR notebooks within same after lanuch
        # Fill these out in config.py
        # If not, these 3 lines below can be commented out for job execution
        'Ec2SubnetId': c.EC2SUBNETID,
        'EmrManagedMasterSecurityGroup': c.EMRMANAGEDMASTERSECURITYGROUP,
        'EmrManagedSlaveSecurityGroup': c.EMRMANAGEDSLAVESECURITYGROUP
    },
    Steps=[
        {
            'Name': 'Setup Debugging',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'state-pusher-script'
                ]
            }
        },
        {
            'Name': 'copy spark script files',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'aws', 's3', 'cp', c.SPARK_SCRIPTS, '/home/hadoop/', '--recursive'
                ]
            }
        },
        {
            'Name': 'Create_SL_Datalake',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit', '/home/hadoop/etl.py'
                ]
            }
        }
    ],
    Applications=[
        {
            'Name': 'Hadoop'
        },
        {
            'Name': 'Hive'
        },
        {
            'Name': 'Spark'
        }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
)
print('EMR cluster start initiated.')