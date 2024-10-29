import io
import json
import toml
import boto3
import pytest

from term_search import app

with open('samconfig.toml', 'r') as f:
    config = toml.load(f)
    s3_bucket = config['default']['deploy']['parameters']['s3_bucket']
    s3_region = config['default']['deploy']['parameters']['region']


s3 = boto3.client('s3')


def get_s3_match_json(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read())


def build_lambda_input(bucket, infile_json):

    return {
        "statusCode": 200,
        "body": {
            "message": "hello world",
            "bucket": bucket,
            "orig_img": "raw/mn-sherburne-county/batch3/R3Part2/Abstract 88291.jpg",
            "ocr_json": infile_json,
            # "txt": "ocr/txt/mn-sherburne-county/batch3/R3Part2/Abstract 88291.txt",
            # "stats": "ocr/stats/mn-sherburne-county/batch3/R3Part2/Abstract 88291__69727524d8d04bfc99ee0f0bf22584e0.json",
            "web_img": "web/mn-sherburne-county/batch3/R3Part2/69727524d8d04bfc99ee0f0bf22584e0.jpg",
            "uuid": "69727524d8d04bfc99ee0f0bf22584e0",
            # "handwriting_pct": 0.01
        }
    }


@pytest.fixture()
def death_cert_table_input_1():
    """ Generates API GW Event"""
    return build_lambda_input(s3_bucket, "ocr/json/mn-sherburne-county/RECEXPORT/Abstract 103872_SPLITPAGE_2.json")


@pytest.fixture()
def basic_input_output():
    """ Generates API GW Event"""
    return {
        "statusCode": 200,
        "body": {
            "message": "hello world",
            "bucket": "covenants-deed-images",
            "orig_img": "raw/nc-forsyth-county/as/00a013/000001_SPLITPAGE_2.tif",
            "ocr_json": "ocr/json/nc-forsyth-county/as/00a013/000001_SPLITPAGE_2.json",
            "web_img": "web/nc-forsyth-county/as/00a013/2130ea9b017c4a5d95946f87dcc07bf1.jpg",
            "uuid": "2130ea9b017c4a5d95946f87dcc07bf1"
        }
    }


def test_input_output_results(basic_input_output):
    ''' Does this run appropriately with output of mp-covenants-term-search-fuzzy Lambda?'''
    fixture = basic_input_output

    ret = app.lambda_handler(fixture, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert data["message"] == "hello world"

    assert "uuid" in data
    assert "orig_img" in data
    assert "web_img" in data
    assert "ocr_json" in data
    
    assert data["uuid"] == fixture['body']['uuid']
    assert data["orig_img"] == fixture['body']['orig_img']
    assert data["web_img"] == fixture['body']['web_img']
    assert data["ocr_json"] == fixture['body']['ocr_json']


def test_death_cert_table_input_1(death_cert_table_input_1):

    ret = app.lambda_handler(death_cert_table_input_1, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert "message" in ret["body"]
    assert data["message"] == "hello world"
    
    assert data["bool_hit"] == True
    assert data["match_file"] != None

    hit_json = get_s3_match_json(s3_bucket, data["match_file"])

    assert len(hit_json['date of death']) > 0
    assert len(hit_json['name of deceased']) > 0
