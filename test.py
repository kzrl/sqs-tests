import boto3
import uuid
import random

# receive JSON. Write to S3

# S3 bucket puts onto SQS. Yay

# MESS dequeues from SQS. Fetches JSON from S3 and happy days

# Method 2: Client.put_object()
client = boto3.client('s3')


payloads = [b'{"hello": "world"}', b'{"moo": "wowow"}', b'{"woop": "mooop"}']

for i in range(10):
    client.put_object(Body=random.choice(payloads), Bucket='kc-messtest', Key=str(uuid.uuid4()))


# aws sqs receive-message --queue-url https://us-west-2.queue.amazonaws.com/943240146135/mess
