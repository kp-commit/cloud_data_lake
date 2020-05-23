# AWS Info
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
REGION_NAME='us-west-2'
ACCOUNT_NUM=''


#Instance configs, get these from your AWS account
CLUSTER_NAME='Sparkify DataLake'
EC2KEYNAME='keypairname'
MASTERINSTANCETYPE='m5.xlarge'
SLAVEINSTANCETYPE='m5.xlarge'
INSTANCECOUNT=3

# Security Groups (get from AWS accoutn and fill this out 
# these lines in lanuch_cluster.py to use with notebook)
# If not, these 3 lines below can be commented out for job execution
EC2SUBNETID='subnet-xyz0123'
EMRMANAGEDMASTERSECURITYGROUP='sg-xyz0123'
EMRMANAGEDSLAVESECURITYGROUP='sg-xyz0123'


# Buckets
INPUT_BUCKET='s3://udacity-dend/'
OUTPUT_BUCKET='s3://output_bucketname/analytics/'
SPARK_SCRIPTS='s3://scriptfiles_bucketname'
