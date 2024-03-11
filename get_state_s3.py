import boto3
import json 

right_buckets = []
with open('example.txt') as f:
    right_buckets = [line.rstrip() for line in f]

session = boto3.session.Session()

s3_client = session.client(
    service_name='s3',
    aws_access_key_id='aws_access_key_id',
    aws_secret_access_key='aws_secret_access_key',
    endpoint_url='',
)

validCount = {}
for i in right_buckets:
    validCount[i] = 0

allCount = 0
for i in s3_client.list_buckets().get('Buckets', []):
    allCount += 1
    buc_name = i.get('Name', '')
    response = s3_client.list_objects(Bucket=buc_name)

    if buc_name in right_buckets:
        if len(response.get('Contents', [])):
            print(buc_name, "is valid")
            validCount[buc_name] += 1
        else:
            print(buc_name, "error bucket")
    else:
        file_size = len(response.get('Contents', [])) 
        if file_size:
            print(buc_name, "not empty left bucket -> file count:", file_size)
            for j in response.get('Contents', []):
                print('-> fileName=', j['Key'], 'size=', j['Size'])
        else:
            print(buc_name, "empty bucket -> FOR DELETE")
            # s3_client.delete_bucket(Bucket=buc_name)

print('need=', len(set(right_buckets)))
print('allCount=', allCount)
print('dict validCount=', json.dumps(validCount, separators=(". ", " = ")))
