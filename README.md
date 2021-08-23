# HitDataRevenue

![Program Flow](/images/Program_flow.png?raw=true "Optional Title")

## Steps to Execute scripts:
1.	Load the necessary input data files into appropriate local directory.
2.	Execute the AWSBotoScript.py by providing data file name in “input_Filename” parameter, this execution print cluster name (e.g. AWS Cluster Name: - j-1R5QZTBK8HUZV) in IDE. Detail process steps performed by boto3 are mentioned below.
	-  2.1 Input Data File will be moved to mentioned S3 bucket
	2.2 An EMR Spark application will be created to execute script in spark application.
	2.3 Once the application is created Pyspark executable will be moved to respective folder
	2.4 Spark job submit will be submitted by passing input file.
	2.5 Once job is completed it will place file in s3 buckets output folder.
	2.6 Waiter will be called to check the status of the cluster and will be proceed further once the cluster is terminated.
	2.7 Output file will be downloaded to local folder from S3 bucket output folder.


## Local environment setup for connecting to AWS from IntelliJ IDE:
1. Click on “run” menu and click on “edit configurations”
2. Add new configuration and provide AWS Access related Environment Variables


![Program Flow](/images/Intellij_setup.png?raw=true "Optional Title")
