CUD repo.

Description: Contains all the CUD related code. MEaning all the jobs which are going to be executed to transform the data from CUP If the notebook has widget arguments (aka params coming from the job) then all these params should be documented as comments on the first cell.

The widget params are instantiated on the 1st cell. 

Add any documentation resources where ever is possible.

## Do not hardcode anything

Your notebooks should run in all workspaces regardless. So do not hardcode anything there and use the global init variables as in the sample below or configuration in aws SSM.

### find the S3 bucket

To connect to the right bucket ex: d-ec1-... when running on dev OR qa-ec1-... when running the notebook on QA, you can use the S3PREFIX environmental variable. This is set in all clusters in all workspaces with the appropriate value (d for dev, qa for QA and p for prod). Here is a code snapshot how to retrive this value:

    import os
    env_prefix = os.environ["S3PREFIX"]
    
### Use SSM to pass configuration params

You can use SSM (AWS system registry) to pass values to your notebook and this way you could have a different path when running on DEV coparing to the one on QA. For example:

    import boto3
    # Initialize the SSM client
    ssm_client = boto3.client("ssm", region_name="eu-central-1")
    response = ssm_client.get_parameter(
        Name='/cfn/a360/reports_output_path',
        WithDecryption=False
    )
    s3_output_path = response['Parameter']['Value']

In the example above /cfn/a360/reports_output_path can be set to S3:///d-ec1-test/output on dev but S3:///qa-ec1-test/uatresults/output in QA.
