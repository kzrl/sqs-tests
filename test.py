import boto3
import uuid

# receive JSON. Write to S3

# S3 bucket puts onto SQS. Yay

# MESS dequeues from SQS. Fetches JSON from S3 and happy days

# Method 2: Client.put_object()
client = boto3.client('s3')
client.put_object(Body=b'{"hello":"world"}', Bucket='kc-messtest', Key=str(uuid.uuid4()))


# aws sqs receive-message --queue-url https://us-west-2.queue.amazonaws.com/943240146135/mess
