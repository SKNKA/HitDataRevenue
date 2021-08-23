import boto3
import datetime
import botocore

input_Filename="data.sql"

today = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')

client = boto3.client('emr', region_name='us-east-2')
S3_BUCKET = 'bigdata-assessment'
S3_KEY = 'pyspark/main.py'
S3_DATA = 'data/'+today+'/'+input_Filename
S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)
S3_DATA_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_DATA)

# # upload file to an S3 bucket
s3 = boto3.resource('s3')
s3.meta.client.upload_file("HitDataAws.py", S3_BUCKET, S3_KEY)

s3 = boto3.resource('s3')
s3.meta.client.upload_file(input_Filename, S3_BUCKET, S3_DATA)

response = client.run_job_flow(
    Name="BigDataSparkEMRCluster",
    ReleaseLabel='emr-5.33.0',
    Instances={
        'MasterInstanceType': 'm5.xlarge',
        'SlaveInstanceType': 'm5.xlarge',
        'InstanceCount': 3,
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    Applications=[
        {
            'Name': 'Spark'
        }
    ],
    Steps=[
        {
            'Name': 'Setup Debugging',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['state-pusher-script']
            }
        },
        {
            'Name': 'setup - copy files',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['aws', 's3', 'cp', S3_URI, '/home/hadoop/']
            }
        },
        {
            'Name': 'Run Spark',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit' , '/home/hadoop/main.py',S3_DATA_URI]
            }
        }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
)

print(response['JobFlowId'])

waiter = client.get_waiter('cluster_terminated')

waiter.wait(
    ClusterId=response['JobFlowId'],
    WaiterConfig={
        'Delay': 123,
        'MaxAttempts': 123
    }
)

today_date = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')

s3 = boto3.resource('s3')

s3_client = boto3.client('s3')
response = s3_client.list_objects(
    Bucket = 'bigdata-assessment',
    Prefix = 'output/'+today_date+'/'
)

name = response["Contents"][1]["Key"].split('/')[-1]

BUCKET_NAME = 'bigdata-assessment' # replace with your bucket name
KEY ='output/'+today_date+'/'+name # replace with your object key

try:
    bucket = s3.Bucket(BUCKET_NAME).download_file(KEY, today_date+'_SearchKeywordPerformance.tab')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise


