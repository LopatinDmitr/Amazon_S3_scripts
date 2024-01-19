import boto3
import json 

right_buckets = []
with open('root_paths.txt') as f:
    right_buckets = [line.rstrip() for line in f]

session = boto3.session.Session()

s3_client = session.client(
    service_name='s3',
    aws_access_key_id='-',
    aws_secret_access_key='-',
    endpoint_url='https://s3mts.ru:443',
)

validCount = {}
for i in right_buckets:
    validCount[i] = 0

for i in s3_client.list_buckets().get('Buckets', []):
    
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
                print(s3_client.delete_object(Bucket=buc_name, Key=j['Key']))
            print(s3_client.delete_bucket(Bucket=buc_name))
            break

print('need=', len(set(right_buckets)))
print('validCount=', len(validCount))
