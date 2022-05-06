import time
import boto3
import pickle
import random
import logging
import datetime as dt

from airflow.decorators import task, dag
from airflow.models.param import Param

from airflow.operators.python import get_current_context


def get_latest_data_slice_name(object_list):
    # Your implementation depending on the concrete files naming convention
    return object_list[-1]['Key']


@dag(
    dag_id='pipeline_example',
    tags=['pipeline_example'],
    schedule_interval=None,
    start_date=dt.datetime.combine(dt.datetime.today() - dt.timedelta(2), dt.datetime.min.time()),
    params={
        'service_name': Param(type='string'),
        'region_name': Param(type='string'),
        'endpoint_url': Param(type='string'),
        'aws_access_key_id': Param(type='string'),
        'aws_secret_access_key': Param(type='string')
    }
)
def pipeline_example():
    @task
    def extract_latest_data_slice():
        logging.info("BEGIN")

        context = get_current_context()

        service_name = context["params"]["service_name"]
        region_name = context["params"]["region_name"]
        endpoint_url = context["params"]["endpoint_url"]
        aws_access_key_id = context["params"]["aws_access_key_id"]
        aws_secret_access_key = context["params"]["aws_secret_access_key"]
        # Here is a description of credentials needed to connect to S3-bucket via boto3 library:
        # https://developers.selectel.ru/docs/cloud-services/cloud-storage/s3/storage_s3_api/
        # Here is a description of S3-bucket creation and configuration:
        # https://kb.selectel.ru/docs/cloud/object-storage/quickstart/
        # ACHTUNG: Please substitute <...> with your own variables
        logging.info("BOTO CLIENT CREATION")
        client = boto3.client(
            service_name=service_name,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        logging.info("OBJECTS LIST DOWNLOADING")

        object_list = client.list_objects(Bucket="efim-test-bucket")
        object_list = object_list['Contents']
        object_list = list(filter(lambda f_name: not f_name['Key'].endswith(".pkl"), object_list))

        target_data_slice_name = get_latest_data_slice_name(object_list)

        logging.info("LATEST FILE DOWNLOADING")
        client.download_file(
            "efim-test-bucket",
            target_data_slice_name,
            target_data_slice_name.split("/")[-1]
        )

        logging.info("LATEST FILE LOCAL COPY READING")
        with open(target_data_slice_name.split("/")[-1], "r") as f:
            text_data_rows = f.readlines()

        time.sleep(int(random.randint(5, 15)))

        logging.info("END")
        return text_data_rows

    @task
    def get_bow_text_representations(text_data_rows):
        logging.info("BEGIN")
        # ...

        time.sleep(int(random.randint(5, 15)))

        logging.info("END")

        return "get_bow_text_representations", text_data_rows

    @task
    def get_word2vec_text_representations(text_data_rows):
        logging.info("BEGIN")
        # ...

        time.sleep(int(random.randint(5, 15)))

        logging.info("END")

        return "get_word2vec_text_representations", text_data_rows

    @task
    def get_glove_text_representations(text_data_rows):
        logging.info("BEGIN")
        # ...

        time.sleep(int(random.randint(5, 15)))

        logging.info("END")

        return "get_glove_text_representations", text_data_rows

    @task
    def load_latest_data_slice(vectors):
        logging.info("BEGIN")

        context = get_current_context()

        service_name = context["params"]["service_name"]
        region_name = context["params"]["region_name"]
        endpoint_url = context["params"]["endpoint_url"]
        aws_access_key_id = context["params"]["aws_access_key_id"]
        aws_secret_access_key = context["params"]["aws_secret_access_key"]
        # Here is a description of credentials needed to connect to S3-bucket via boto3 library:
        # https://developers.selectel.ru/docs/cloud-services/cloud-storage/s3/storage_s3_api/
        # Here is a description of S3-bucket creation and configuration:
        # https://kb.selectel.ru/docs/cloud/object-storage/quickstart/
        client = boto3.client(
            service_name=service_name,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        with open("{}.pkl".format(vectors[0]), "wb") as f:
            pickle.dump(vectors[1], f)

        out_name = "get_text_vector_representations/result/{}/{}.pkl".format(dt.datetime.now(), vectors[0])

        client.upload_file("{}.pkl".format(vectors[0]), "efim-test-bucket", out_name)

        time.sleep(int(random.randint(5, 15)))

        logging.info("END")

    text_data_rows = extract_latest_data_slice()

    bow_vectors = get_bow_text_representations(text_data_rows)
    word2vec_vectors = get_word2vec_text_representations(text_data_rows)
    glove_vectors = get_glove_text_representations(text_data_rows)

    load_latest_data_slice(bow_vectors)
    load_latest_data_slice(word2vec_vectors)
    load_latest_data_slice(glove_vectors)


pipeline_example_dag = pipeline_example()
