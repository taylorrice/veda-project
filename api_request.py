import boto3
import json
import requests
from typing import Dict, List


BUCKET = 'veda-project-mwaa'
s3 = boto3.resource('s3')

def get_subject_from_isbn(bucket: str, s3) -> List[Dict]:


    try:
        # Get the file inside the S3 Bucket
        s3_response = s3.Bucket(bucket).Object('nyt_data/bestsellers_isbns_sample.json')
        file_content = s3_response.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)


    subject_dict_list = [{"isbn13": "book_subjects"}]

    for dict in json_content:
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
object = s3.Bucket(BUCKET).Object('nyt_data/subject_isbn.json')
result = object.put(Body=body)
