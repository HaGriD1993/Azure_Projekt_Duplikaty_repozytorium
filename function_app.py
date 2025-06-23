import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import hashlib
import os

app = func.FunctionApp()

@app.function_name(name="HttpTrigger")
@app.route(route="delete-duplicates", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET", "POST"])
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        azure_connection_string = os.getenv("AzureWebJobsStorage")
        container_name = os.getenv("AzureStorageContainerName")

        if not azure_connection_string or not container_name:
            return func.HttpResponse(status_code=500)

        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        hashes = {}

        blobs = container_client.list_blobs()
        for blob in blobs:
            blob_client = container_client.get_blob_client(blob.name)
            blob_data = blob_client.download_blob().readall()
            file_hash = hashlib.sha256(blob_data).hexdigest()

            if file_hash in hashes:
                blob_client.delete_blob()
            else:
                hashes[file_hash] = blob.name

        return func.HttpResponse(status_code=200)

    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(status_code=500)