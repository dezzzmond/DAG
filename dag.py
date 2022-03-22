from dagster import job, op
 
import boto3
 
 
def get_latest_data_slice_name(object_list):
    # Your implementation depending on the concrete files naming convention
    ...
 
@op
def extract_latest_data_slice():
    # Here is a description of credentials needed to connect to S3-bucket via boto3 library:
    # https://developers.selectel.ru/docs/cloud-services/cloud-storage/s3/storage_s3_api/
    # Here is a description of S3-bucket creation and configuration:
    # https://kb.selectel.ru/docs/cloud/object-storage/quickstart/
    # ACHTUNG: Please substitute <...> with your own variables
    client = boto3.client(
        service_name="s3",
        region_name="ru-1",
        endpoint_url="https://s3.storage.selcloud.ru",
        aws_access_key_id="<AWS-ACCESS-KEY-ID>",
        aws_secret_access_key="<AWS-SECRET-ACCESS-KEY>"
    )
     
    object_list = client.list_objects(Bucket='<TARGET-BUCKET>')
     
    target_data_slice_name = get_latest_data_slice_name(object_list)
     
    client.download_file('<TARGET-BUCKET>', target_data_slice_name, '<TARGET-DATA-LOCAL-NAME>')
     
    with open('<TARGET-DATA-LOCAL-NAME>', 'r') as f:
        data_rows = f.readlines()
     
    return text_data_rows
 
@op
def get_bow_text_representations(text_data_rows):
    ...
    return vectors
 
@op
def get_word2vec_text_representations(text_data_rows):
    ...
    return vectors
 
@op
def get_glove_text_representations(text_data_rows):
    ...
    return vectors
 
@op
def load_latest_data_slice(vectors):
    # Here is a description of credentials needed to connect to S3-bucket via boto3 library:
    # https://developers.selectel.ru/docs/cloud-services/cloud-storage/s3/storage_s3_api/
    # Here is a description of S3-bucket creation and configuration:
    # https://kb.selectel.ru/docs/cloud/object-storage/quickstart/
    session = boto3.Session(
        region_name="ru-1",
        aws_access_key_id="<AWS-ACCESS-KEY-ID>",
        aws_secret_access_key="<AWS-SECRET-ACCESS-KEY>"
    )
 
    s3 = session.resource(
        service_name="s3",
        endpoint_url="https://s3.storage.selcloud.ru"
    )
     
    object = s3.Object('<TARGET-BUCKET>', '<TARGET-DATA-S3-NAME>')
     
    result = object.put(Body=txt_data)
 
    res = result.get('ResponseMetadata')
 
    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')
 
 
@job
def get_text_vector_representations():
    texts = extract_latest_data_slice()
     
    bow_vectors = get_bow_text_representations(text_data_rows)
    word2vec_vectors = get_word2vec_text_representations(text_data_rows)
    glove_vectors = get_glove_text_representations(text_data_rows)
     
    load_latest_data_slice(bow_vectors)
    load_latest_data_slice(word2vec_vectors)
    load_latest_data_slice(glove_vectors)
