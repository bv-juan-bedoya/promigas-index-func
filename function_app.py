import html
import os
import re
import azure.functions as func
import datetime
import json
import logging
import time
import hashlib
from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableServiceClient
from openai import AzureOpenAI
import requests
import PyPDF2
from PIL import Image
import fitz  # PyMuPDF
import base64
from mimetypes import guess_type
import unicodedata
from utils.keyvault import get_kv_variable
from utils.shp_access import get_access_token, get_site_id, get_drive_id, list_drive_folder

app = func.FunctionApp()

def get_document_hash(file_content: bytes) -> str:
    """Generate a hash for document content to track changes"""
    return hashlib.md5(file_content).hexdigest()

def get_processing_state(table_client, doc_name: str, doc_hash: str):
    """Get the current processing state of a document"""
    try:
        entity = table_client.get_entity(partition_key="documents", row_key=doc_name)
        if entity.get('doc_hash') == doc_hash and entity.get('status') == 'completed':
            return entity.get('last_processed_page', 0), True  # Fully processed
        return entity.get('last_processed_page', 0), False  # Partially processed or hash changed
    except:
        return 0, False  # New document

def update_processing_state(table_client, doc_name: str, doc_hash: str, last_page: int, total_pages: int, status: str):
    """Update the processing state of a document"""
    try:
        entity = {
            'PartitionKey': 'documents',
            'RowKey': doc_name,
            'doc_hash': doc_hash,
            'last_processed_page': last_page,
            'total_pages': total_pages,
            'status': status,
            'last_updated': datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        table_client.upsert_entity(entity)
    except Exception as e:
        logging.error(f"Error updating processing state: {e}")

def get_sharepoint_documents(tenant_id: str, client_id: str, client_secret: str, scope: str, 
                           dominio: str, site: str, folder_path: str = None):
    """Get documents from SharePoint using Microsoft Graph API"""
    try:
        logging.info("=== STARTING SHAREPOINT ACCESS WITH MICROSOFT GRAPH API ===")
        logging.info(f"Target domain: {dominio}")
        logging.info(f"Target site: {site}")
        logging.info(f"Target folder: {folder_path if folder_path else 'Root/All folders'}")
        
        # Step 1: Get access token
        logging.info("Step 1: Getting access token...")
        token = get_access_token(tenant_id, client_id, client_secret, scope)
        logging.info("✅ Access token obtained successfully")
        
        # Step 2: Get site info and ID
        logging.info("Step 2: Getting SharePoint site ID...")
        site_info = get_site_id(token, dominio, site)
        site_id = site_info["id"]
        logging.info(f"✅ Site ID obtained: {site_id}")
        
        # Step 3: Get drive ID
        logging.info("Step 3: Getting drive ID...")
        drive_id = get_drive_id(token, site_id)
        if not drive_id:
            logging.error("❌ No drive found in SharePoint site")
            return []
        logging.info(f"✅ Drive ID obtained: {drive_id}")
        
        # Step 4: List files in the specified folder
        logging.info("Step 4: Listing files in SharePoint...")
        if folder_path:
            total_files, files_list = list_drive_folder(token, drive_id, folder_path)
        else:
            # If no folder specified, list from root
            total_files, files_list = list_drive_folder(token, drive_id, "")
        
        logging.info(f"✅ Found {total_files} total files")
        
        # The shp_access.py returns file paths as strings, so we need to convert them to file objects
        # and filter for PDF files
        pdf_files = []
        
        # Get individual file details for PDF files
        for file_path in files_list:
            if file_path.lower().endswith('.pdf'):
                try:
                    # Get file details using Graph API
                    file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path}"
                    headers = {"Authorization": f"Bearer {token}"}
                    
                    file_response = requests.get(file_url, headers=headers)
                    file_response.raise_for_status()
                    file_info = file_response.json()
                    
                    pdf_files.append({
                        'name': file_info['name'],
                        'path': file_path,
                        'download_url': file_info.get('@microsoft.graph.downloadUrl', ''),
                        'size': file_info.get('size', 0),
                        'last_modified': file_info.get('lastModifiedDateTime', ''),
                        'web_url': file_info.get('webUrl', '')
                    })
                    
                except Exception as file_error:
                    logging.warning(f"⚠️ Could not get details for file {file_path}: {file_error}")
        
        logging.info(f"📄 Found {len(pdf_files)} PDF files")
        
        # Log each PDF file found
        for i, pdf_file in enumerate(pdf_files, 1):
            logging.info(f"PDF {i}: {pdf_file['name']} (Path: {pdf_file['path']})")
        
        # Step 5: Download PDF files and create document objects
        documents = []
        for pdf_file in pdf_files:
            try:
                if pdf_file['download_url']:
                    logging.info(f"Downloading: {pdf_file['name']}")
                    
                    # Download file content using the download URL
                    headers = {"Authorization": f"Bearer {token}"}
                    response = requests.get(pdf_file['download_url'], headers=headers)
                    response.raise_for_status()
                    
                    file_content = response.content
                    documents.append({
                        'name': pdf_file['name'],
                        'content': file_content,
                        'hash': get_document_hash(file_content),
                        'path': pdf_file['path'],
                        'size': pdf_file['size'],
                        'last_modified': pdf_file['last_modified'],
                        'web_url': pdf_file['web_url']
                    })
                    logging.info(f"✅ Successfully downloaded: {pdf_file['name']} ({pdf_file['size']} bytes)")
                else:
                    logging.warning(f"⚠️ No download URL for file: {pdf_file['name']}")
                    
            except Exception as file_error:
                logging.error(f"❌ Error downloading file {pdf_file['name']}: {file_error}")
        
        logging.info(f"=== SHAREPOINT ACCESS COMPLETED: {len(documents)} PDF documents retrieved ===")
        return documents
        
    except Exception as e:
        logging.error(f"❌ Error accessing SharePoint with Graph API: {e}")
        return []


def deep_unicode_clean(text: str) -> str:
    # Step 1: Unescape common escaped Unicode sequences like \u00e2\u0080\u00a2
    text = text.encode('utf-8').decode('unicode_escape')

    # Step 2: Re-decode mojibake by interpreting broken characters as latin1
    try:
        text = text.encode('latin1').decode('utf-8')
    except UnicodeDecodeError:
        pass  # If it fails, leave text as is

    # Step 3: Normalize to compose accents and ligatures correctly
    text = unicodedata.normalize('NFC', text)

    # Step 4: Clean any leftover artifacts (e.g. stray control characters, newlines)
    text = re.sub(r'[\u0000-\u001F]+', ' ', text)  # Remove control chars
    text = text.replace('\n', ' ').replace('\r', ' ').strip()

    return text


# Function to convert a local image to a data URL
def local_image_to_data_url(image_path):
    """
    Get the URL of a local image
    """
    mime_type, _ = guess_type(image_path)

    if mime_type is None:
        mime_type = "application/octet-stream"

    with open(image_path, "rb") as image_file:
        base64_encoded_data = base64.b64encode(image_file.read()).decode("utf-8")

    return f"data:{mime_type};base64,{base64_encoded_data}"

@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def time_trigg_func(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function started.')

    # Load environment variables
    storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    agent_endpoint = os.getenv("WORKFLOW_EXPLAIN_AGENT_ENDPOINT")
    agent_api_key = os.getenv("API_KEY")
    agent_deployment = os.getenv("WORKFLOW_EXPLAIN_AGENT_DEPLOYMENT")
    agent_api_version = os.getenv("WORKFLOW_EXPLAIN_AGENT_API_VERSION")
    search_index_name = os.getenv("AZURE_SEARCH_INDEX_NAME")
    search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
    search_api_key = os.getenv("AZURE_SEACRH_API_KEY")
    embedding_model_deployment = os.getenv("EMBEDDING_MODEL_DEPLOYMENT")
    embedding_model_endpoint = os.getenv("EMBEDDING_MODEL_ENDPOINT")
    embedding_model_api_key = os.getenv("EMBEDDING_MODEL_API_KEY")
    embedding_model_api_version = os.getenv("EMBEDDING_MODEL_API_VERSION")
    
    # SharePoint environment variables for Graph API
    sharepoint_scope = os.getenv("SHAREPOINT_SCOPE", "https://graph.microsoft.com/.default")
    sharepoint_dominio = os.getenv("SHAREPOINT_DOMINIO", "")
    sharepoint_site = os.getenv("SHAREPOINT_SITE", "")
    sharepoint_folder_name = os.getenv("SHAREPOINT_FOLDER_NAME", "")
    
    # SharePoint credentials from Key Vault (recommended) or environment variables (fallback)
    try:
        sharepoint_tenant_id = get_kv_variable("TENANT_ID")
        sharepoint_client_id = get_kv_variable("CLIENT_ID")
        sharepoint_client_secret = get_kv_variable("CLIENT_SECRET")
        logging.info("✅ Successfully retrieved SharePoint credentials from Key Vault")
    except Exception as kv_error:
        logging.warning(f"⚠️ Failed to retrieve credentials from Key Vault: {kv_error}")
        logging.info("🔄 Falling back to environment variables for SharePoint credentials")
        sharepoint_tenant_id = os.getenv("SHAREPOINT_TENANT_ID", "")
        sharepoint_client_id = os.getenv("SHAREPOINT_CLIENT_ID", "")
        sharepoint_client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET", "")

    # Table storage for state tracking
    table_storage_connection_string = os.getenv("TABLE_STORAGE_CONNECTION_STRING", "")
    state_table_name = os.getenv("STATE_TABLE_NAME", "documentprocessing")
    
    # Processing optimization settings
    max_pages_per_execution = int(os.getenv("MAX_PAGES_PER_EXECUTION", "20"))  # Increased for daily run
    image_quality_dpi = int(os.getenv("IMAGE_QUALITY_DPI", "200"))  # Reduced for speed while maintaining quality
    processing_timeout_minutes = int(os.getenv("PROCESSING_TIMEOUT_MINUTES", "25"))  # 5 min buffer for consumption plan

    logging.info(f"Environment variables loaded: Storage Account: {storage_account_name}, Container: {container_name}, Agent Endpoint: {agent_endpoint}, Search Endpoint: {search_endpoint}")

    # Initialize clients
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key)
    
    # Initialize Table Storage for state tracking
    table_service_client = TableServiceClient.from_connection_string(table_storage_connection_string) if table_storage_connection_string else None
    table_client = table_service_client.get_table_client(state_table_name) if table_service_client else None
    
    # Ensure table exists
    if table_client:
        try:
            table_client.create_table()
        except:
            pass  # Table already exists

    # Initialize Azure OpenAI client
    openai_client = AzureOpenAI(
        api_version=agent_api_version,
        azure_endpoint=agent_endpoint,
        api_key=agent_api_key,
    )

    # Initialize embedding client
    embedding_client = AzureOpenAI(
        api_version=embedding_model_api_version,
        azure_endpoint=embedding_model_endpoint,
        api_key=embedding_model_api_key,
    )

    # Read agent prompt once at startup
    agent_prompt = "Eres un asistente útil especializado en analizar imágenes. Por favor, realiza las siguientes tareas: 1. Si la imagen contiene solo texto, transcribe el texto exacto sin agregar explicaciones adicionales. 2. Si la imagen contiene gráficos, diagramas, tablas u otros elementos visuales, proporciona una descripción detallada y precisa de su contenido en español. 3. Asegúrate de no omitir ningún detalle importante ni agregar información que no esté presente en la imagen."
    
    # Processing tracking
    start_time = datetime.datetime.now()
    pages_processed = 0
    documents_processed = 0

    # Get documents from SharePoint using Microsoft Graph API or fallback to local folder
    if sharepoint_dominio and sharepoint_site and sharepoint_client_id and sharepoint_client_secret and sharepoint_tenant_id:
        logging.info("🚀 Fetching documents from SharePoint using Microsoft Graph API...")
        documents = get_sharepoint_documents(
            tenant_id=sharepoint_tenant_id,
            client_id=sharepoint_client_id, 
            client_secret=sharepoint_client_secret,
            scope=sharepoint_scope,
            dominio=sharepoint_dominio,
            site=sharepoint_site,
            folder_path=sharepoint_folder_name
        )
    else:
        missing_configs = []
        if not sharepoint_dominio: missing_configs.append("SHAREPOINT_DOMINIO")
        if not sharepoint_site: missing_configs.append("SHAREPOINT_SITE")
        if not sharepoint_client_id: missing_configs.append("CLIENT_ID")
        if not sharepoint_client_secret: missing_configs.append("CLIENT_SECRET") 
        if not sharepoint_tenant_id: missing_configs.append("TENANT_ID")
        
        logging.warning(f"⚠️ SharePoint not fully configured. Missing: {', '.join(missing_configs)}")
        logging.info("🔄 Using local folder instead...")
        documents = []
        documents_folder = "sample-documents"
        if os.path.exists(documents_folder):
            for doc_name in os.listdir(documents_folder):
                if doc_name.endswith('.pdf'):
                    doc_path = os.path.join(documents_folder, doc_name)
                    with open(doc_path, "rb") as f:
                        content = f.read()
                    documents.append({
                        'name': doc_name,
                        'content': content,
                        'hash': get_document_hash(content)
                    })
    
    logging.info(f"Found {len(documents)} PDF documents to process")
    
    try:
        for doc_info in documents:
            # Check execution time limit
            elapsed_time = datetime.datetime.now() - start_time
            if elapsed_time.total_seconds() / 60 >= processing_timeout_minutes:
                logging.info(f"Approaching timeout limit ({processing_timeout_minutes} minutes). Stopping execution.")
                break
                
            if pages_processed >= max_pages_per_execution:
                logging.info(f"Reached maximum pages limit ({max_pages_per_execution}). Stopping execution.")
                break
            
            doc_name = doc_info['name']
            doc_content = doc_info['content']
            doc_hash = doc_info['hash']
            
            logging.info(f"Processing document: {doc_name}")
            
            # Check processing state
            last_processed_page, is_completed = get_processing_state(table_client, doc_name, doc_hash) if table_client else (0, False)
            
            if is_completed:
                logging.info(f"Document {doc_name} already fully processed. Skipping.")
                continue
            
            # Get page count using PyMuPDF (fitz) - much faster and free
            try:
                pdf_doc = fitz.open(stream=doc_content, filetype="pdf")
                total_pages = len(pdf_doc)
                pdf_doc.close()  # Clean up immediately
                logging.info(f"Document page count analysis completed for: {doc_name}. Total pages: {total_pages}")
                
                # Update state with total pages
                if table_client:
                    update_processing_state(table_client, doc_name, doc_hash, last_processed_page, total_pages, 'processing')
                
            except Exception as e:
                logging.error(f"Error analyzing document {doc_name}: {e}")
                continue

            # Process pages starting from last processed page
            for page_idx in range(last_processed_page, total_pages):
                # Check limits again
                elapsed_time = datetime.datetime.now() - start_time
                if elapsed_time.total_seconds() / 60 >= processing_timeout_minutes:
                    logging.info(f"Timeout approaching. Stopping at page {page_idx + 1}")
                    break
                    
                if pages_processed >= max_pages_per_execution:
                    logging.info(f"Page limit reached. Stopping at page {page_idx + 1}")
                    break
                
                try:
                    logging.info(f"Processing page {page_idx + 1}/{total_pages} of document: {doc_name} (Session total: {pages_processed + 1})")

                    # Create PDF document from content for page extraction
                    pdf_doc = fitz.open(stream=doc_content, filetype="pdf")
                    
                    # Capture the page as an image with optimized quality
                    pix = pdf_doc[page_idx].get_pixmap(dpi=image_quality_dpi)
                    img_data = pix.tobytes("png")
                    
                    # Convert directly to data URL without saving to disk
                    img_base64 = base64.b64encode(img_data).decode("utf-8")
                    image_data_url = f"data:image/png;base64,{img_base64}"
                    
                    pdf_doc.close()  # Clean up immediately
                    
                    logging.info(f"Page {page_idx + 1} converted to data URL")

                    # Minimal delay to avoid overwhelming APIs
                    time.sleep(0.5)

                    # Send the image data URL to the agent for explanation
                    agent_response = openai_client.chat.completions.create(
                        messages=[
                            {"role": "system", "content": agent_prompt},
                            {"role": "user", "content": [
                                {"type": "text", "text": "Por favor, analiza esta imagen."},
                                {"type": "image_url", "image_url": {"url": image_data_url}}
                            ]}
                        ],
                        max_tokens=3000,  # Optimized for speed vs quality
                        temperature=0.0,
                        model=agent_deployment
                    )
                    
                    # Process explanation
                    raw_explanation = agent_response.choices[0].message.content
                    explanation = deep_unicode_clean(raw_explanation)

                    # Generate embedding
                    try:
                        embedding_response = embedding_client.embeddings.create(
                            input=[explanation],
                            model=embedding_model_deployment
                        )
                        embedding_vector = embedding_response.data[0].embedding
                    except Exception as e:
                        logging.error(f"Error generating embedding for page {page_idx + 1}: {e}")
                        embedding_vector = None

                    # Sanitize the document key
                    sanitized_doc_name = ''.join(
                        c if c.isalnum() or c in ['_', '-', '='] else '_' 
                        for c in unicodedata.normalize('NFKD', doc_name).encode('ascii', 'ignore').decode('ascii')
                    )
                    document_id = f"{sanitized_doc_name}_page{page_idx + 1}_image"

                    # Prepare payload for Azure Search
                    payload = {
                        "value": [{
                            "id": document_id,
                            "metadata_spo_item_path": f"sharepoint://{doc_name}",
                            "metadata_spo_item_created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            "metadata_spo_item_last_modified": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            "metadata_spo_item_title": f"Page {page_idx + 1} of {doc_name}",
                            "metadata_spo_item_content": explanation,
                            "embedding": embedding_vector if embedding_vector else []
                        }]
                    }

                    # Index in Azure Search
                    search_response = requests.post(
                        f"{search_endpoint}/indexes/{search_index_name}/docs/index?api-version=2021-04-30-Preview",
                        headers={"Content-Type": "application/json", "api-key": search_api_key},
                        json=payload,
                        timeout=30
                    )

                    if search_response.status_code == 200:
                        logging.info(f"Indexed page {page_idx + 1} successfully.")
                        pages_processed += 1
                        
                        # Update progress in state
                        if table_client:
                            status = 'completed' if page_idx + 1 == total_pages else 'processing'
                            update_processing_state(table_client, doc_name, doc_hash, page_idx + 1, total_pages, status)
                        
                    else:
                        logging.error(f"Failed to index page {page_idx + 1}. Status: {search_response.status_code}")
                        # Don't break on indexing errors, continue with next page

                except Exception as e:
                    logging.error(f"Error processing page {page_idx + 1} of document {doc_name}: {e}")
                    continue
            
            documents_processed += 1
            logging.info(f"Completed processing document {doc_name}")

    except Exception as e:
        logging.error(f"Error in main processing loop: {e}")

    elapsed_time = datetime.datetime.now() - start_time
    logging.info(f'>>>>>>>>>>>> Function completed. Processed {pages_processed} pages from {documents_processed} documents in {elapsed_time.total_seconds():.1f} seconds <<<<<<<<<<<<<<<<<<')