import re
import json
import urllib.parse
import boto3

'''
Folder structure:

covenants-deeds-images
    -raw
        -mn-ramsey-county
        -wi-milwaukee-county
    -ocr
        -txt
            -mn-ramsey-county
            -wi-milwaukee-county
        -json
            -mn-ramsey-county
            -wi-milwaukee-county
        -stats
            -mn-ramsey-county
            -wi-milwaukee-county
        -hits
            -mn-ramsey-county
            -wi-milwaukee-county
    -web
        -mn-ramsey-county
        -wi-milwaukee-county
'''

s3 = boto3.client('s3')

covenant_flags = [
    'african',
    ' alien',
    'armenian',
    ' aryan',
    'caucasian',
    'chinese',
    'citizen',
    'colored',
    'domestic servants',
    'ethiopian',
    'hebrew',
    'hindu',
    ' indian',
    'irish',
    'italian',
    'japanese',
    ' jew ',
    'jewish',
    ' malay',
    'mexican',
    'mongolian',
    'moorish',
    'mulatto',
    'mulato',
    'nationality',
    ' not white',
    'negro',
    'occupied by any',
    'persian',
    'person not of',
    'persons not of',
    ' polish',
    'racial',
    'semetic',
    'semitic',
    'simitic',
    'syrian',
    'turkish',
    'white race',
]

def load_json(bucket, key):
    content_object = s3.get_object(Bucket=bucket, Key=key)
    file_content = content_object['Body'].read().decode('utf-8')
    return json.loads(file_content)

def save_match_file(results, bucket, key_parts):
    out_key = f"ocr/hits/{key_parts['workflow']}/{key_parts['remainder']}.json"

    s3.put_object(
        Body=json.dumps(results),
        Bucket=bucket,
        Key=out_key,
        StorageClass='GLACIER_IR',
        ContentType='application/json'
    )
    return out_key

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    if 'Records' in event:
        # Get the object from a more standard put event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        public_uuid = None
    elif 'detail' in event:
        # Get object from step function with this as first step
        bucket = event['detail']['bucket']['name']
        key = event['detail']['object']['key']
        public_uuid = None
    else:
        # Coming from previous step function
        bucket = event['body']['bucket']
        key = event['body']['json']
        public_uuid = event['body']['uuid']

    try:
        ocr_result = load_json(bucket, key)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure it exists and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    key_parts = re.search('ocr/json/(?P<workflow>[A-z\-]+)/(?P<remainder>.+)\.(?P<extension>[a-z]+)', key).groupdict()
    lines = [block for block in ocr_result['Blocks'] if block['BlockType'] == 'LINE']

    results = {}
    for line_num, line in enumerate(lines):
        text_lower = line['Text'].lower()
        for term in covenant_flags:
            if term in text_lower:
                if term not in results:
                    results[term] = [line_num]
                else:
                    results[term].append(line_num)

    bool_hit = False
    match_file = None
    if results != {}:
        results['workflow'] = key_parts['workflow']
        results['lookup'] = key_parts['remainder']
        results['uuid'] = public_uuid
        bool_hit = True
        match_file = save_match_file(results, bucket, key_parts)

    return {
        "statusCode": 200,
        "body": {
            "message": "hello world",
            "bool_hit": bool_hit,
            "match_file": match_file,
            "uuid": public_uuid
            # "location": ip.text.replace("\n", "")
        }
    }
