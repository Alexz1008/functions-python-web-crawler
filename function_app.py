import azure.functions as func
import hashlib
import json
import logging
import os
import re
import requests
import traceback
import validators
from datetime import datetime, timezone
from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

CHUNK_SIZE = 2000  # characters per chunk, tuned for AI Search


@app.route(route="search_site", methods=["POST"])
def search_site(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    url = req.params.get('url')
    if not url:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            url = req_body.get('url')

    if url:
        if validators.url(url):
            result = orchestrator_function(url)
            return func.HttpResponse(
                json.dumps(result, indent=2),
                status_code=200,
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                json.dumps({"error": "The URL was invalid."}),
                status_code=400,
                mimetype="application/json"
            )
    else:
        return func.HttpResponse(
            json.dumps({"error": "No URL was passed. Please input a URL."}),
            status_code=400,
            mimetype="application/json"
        )


def orchestrator_function(url):
    try:
        data = crawl_site(url)

        title = get_page_title(data)
        description = get_meta_tag(data)
        content = get_text_content(data)
        links = get_all_urls(data)

        # Split content into chunks for AI Search indexing.
        chunks = chunk_text(content, CHUNK_SIZE)

        documents = []
        for i, chunk in enumerate(chunks):
            doc_id = generate_doc_id(url, i)
            document = {
                "id": doc_id,
                "url": url,
                "title": title or "",
                "chunk_index": i,
                "total_chunks": len(chunks),
                "content": chunk,
                "metadata_description": description or "",
                "links": links,
                "crawled_at": datetime.now(timezone.utc).isoformat(),
            }
            documents.append(document)

        upload_to_blob_storage(url, documents)

        return {
            "url": url,
            "title": title,
            "total_chunks": len(chunks),
            "documents_uploaded": len(documents),
        }
    except Exception as error:
        logging.error(f"Error while crawling the site: {error}")
        logging.error(traceback.format_exc())
        return {"error": str(error)}


def crawl_site(url):
    response = requests.get(url, allow_redirects=True, timeout=15)
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml")


def get_page_title(data):
    try:
        return data.title.string.strip() if data.title and data.title.string else None
    except Exception as error:
        logging.error(f"Error retrieving the site title: {error}")
        return None


def get_text_content(data):
    try:
        body = data.find("body")
        if not body:
            return ""
        for tag in body(["script", "style", "noscript", "nav", "footer", "header"]):
            tag.decompose()
        lines = (line.strip() for line in body.get_text(separator="\n").splitlines())
        return "\n".join(line for line in lines if line)
    except Exception as error:
        logging.error(f"Error retrieving text content: {error}")
        return ""


def get_all_urls(data):
    try:
        urls = []
        for el in data.select("a[href]"):
            href = el['href']
            if href.startswith("https://") or href.startswith("http://"):
                urls.append(href)
        return urls
    except Exception as error:
        logging.error(f"Error retrieving URLs: {error}")
        return []


def get_meta_tag(data):
    try:
        meta_tag = data.find("meta", attrs={'name': 'description'})
        return meta_tag["content"] if meta_tag else None
    except Exception as error:
        logging.error(f"Error retrieving meta description: {error}")
        return None


def chunk_text(text, chunk_size):
    """Split text into chunks, breaking at paragraph boundaries when possible."""
    if not text:
        return [""]

    paragraphs = text.split("\n")
    chunks = []
    current_chunk = ""

    for paragraph in paragraphs:
        if len(current_chunk) + len(paragraph) + 1 > chunk_size and current_chunk:
            chunks.append(current_chunk.strip())
            current_chunk = paragraph
        else:
            current_chunk = current_chunk + "\n" + paragraph if current_chunk else paragraph

    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    return chunks if chunks else [""]


def generate_doc_id(url, chunk_index):
    """Create a deterministic, URL-safe document ID for AI Search."""
    raw = f"{url}::chunk::{chunk_index}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def upload_to_blob_storage(url, documents):
    try:
        account_url = os.environ["STORAGE_ACCOUNT_URL"]
        container_name = os.environ["STORAGE_CONTAINER_NAME"]
        client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")

        if client_id:
            credential = ManagedIdentityCredential(client_id=client_id)
        else:
            credential = DefaultAzureCredential()

        blob_service_client = BlobServiceClient(account_url, credential=credential)
        container_client = blob_service_client.get_container_client(container_name)

        if not container_client.exists():
            container_client.create_container()

        parsed = urlparse(url)
        folder = re.sub(r"[^a-zA-Z0-9_-]", "_", parsed.netloc + parsed.path)[:120]

        for doc in documents:
            blob_name = f"{folder}/{doc['id']}.json"
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(json.dumps(doc, indent=2), overwrite=True)

        logging.info(f"Uploaded {len(documents)} documents for {url}")
    except Exception as error:
        logging.error(f"Error uploading to blob storage: {error}")
        logging.error(traceback.format_exc())