import boto3
import json 
import threading
# from multiprocessing import Process

def clean_bucket(client, b, files):
    deleted = 0
    for j in files:
        client.delete_object(Bucket=b, Key=j['Key'])
        # deleted += 1
        # if deleted % 100 == 0:
        #     print(b, deleted)
    try:
        print(client.delete_bucket(Bucket=b))
    except:
        print(b, "not empty bucket -> skiped")

right_buckets = []
with open('root_paths_1.txt') as f:
    right_buckets = [line.rstrip() for line in f]

session = boto3.session.Session()

s3_client = session.client(
    service_name='s3',
    aws_access_key_id='62yrdd5rsakl2xx5',
    aws_secret_access_key='lKO3cK4zYEsdhFZdZ4ng1T9MZ3dJSFf5WD2xQ1KD',
    endpoint_url='https://s3mts.ru:443',
)

# validCount = {}
# for i in right_buckets:
#     validCount[i] = 0
spec_count = 0
all_count = 0
rigth_count = 0
left_count = 0
for i in s3_client.list_buckets().get('Buckets', []):
    all_count += 1
    buc_name = i.get('Name', '')
    response = s3_client.list_objects(Bucket=buc_name)

    if buc_name in right_buckets:
        if len(response.get('Contents', [])):
            print(buc_name, "is valid")
            rigth_count += 1
            # validCount[buc_name] += 1
        else:
            print(buc_name, "error bucket")
    else:
        left_count += 1
        file_size = len(response.get('Contents', [])) 
        if file_size == 1000:
            spec_count += 1

        print(buc_name, "left bucket -> file count:", file_size)
        # if file_size:
            # print(buc_name, "not empty left bucket -> file count:", file_size)
            # clean_bucket(s3_client, response.get('Contents', []))
        # threading.Thread(target=clean_bucket, args=(s3_client, buc_name, response.get('Contents', []),)).start()
            # p = Process(target=clean_bucket, args=(s3_client, response.get('Contents', []),))
            # p.start()
            # p.join()

            # deleted = 0
            # for j in response.get('Contents', []):
            #     s3_client.delete_object(Bucket=buc_name, Key=j['Key'])
            #     deleted += 1
            #     print(deleted)
            # try:
            #     print(s3_client.delete_bucket(Bucket=buc_name))
            # except:
            #     print("not empty bucket -> skiped")

# print('need=', len(set(right_buckets)))
# print('validCount=', len(validCount))
# print('full validCount=', validCount)
        
print('all=', all_count)
print('1000items=', spec_count)
print('right=', rigth_count, ' left=', left_count)
        

