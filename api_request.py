import boto3
import json
import requests
from typing import Dict, List


BUCKET = 'veda-project-mwaa'
s3 = boto3.client('s3')

def get_subject_from_isbn(bucket: str, s3) -> List[Dict]:


    # Get the file inside the S3 Bucket
    s3_response = s3.get_object(
        Bucket=bucket,
        Key='nyt_data/bestsellers_isbns_sample.json'
    )

    # Get the Body object in the S3 get_object() response
    s3_object_body = s3_response.get('Body')

    # Read the data in bytes format and convert it to string
    isbns_string = s3_object_body.read().decode()

    isbns_json = json.loads(isbns_string)
    print(isbns_json)

    subject_dict_list = [{"isbn13": "book_subjects"}]

    for dict in isbns_json:
        for num in dict.values():
            try:
                res_json = requests.get(f"https://openlibrary.org/isbn/{num}.json").json()
                res_json["subjects"]
                subjects = res_json["subjects"]
                subject_dict_list.append({num: subjects})
            except ValueError:
                continue
            except KeyError:
                subject_dict_list.append({num: None})

    return subject_dict_list

body = get_subject_from_isbn(BUCKET, s3)
object = s3.Object(BUCKET, 'nyt_data/subject_isbn.json')
result = object.put(Body=body)
