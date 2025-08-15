#!/usr/bin/env python3
"""
Standalone Document Processor
Converts SharePoint documents to searchable content using Azure OpenAI and Azure Search
"""

import html
import os
import re
import datetime
import json
import logging
import time
import hashlib
import uuid
import sys
import argparse
from dotenv import load_dotenv
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

def setup_logging():
    """Configure logging for standalone execution"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Configure logging with both file and console output
    log_filename = f"logs/document_processor_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set Azure SDK logging levels to reduce noise
    logging.getLogger('azure').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

def load_environment():
    """Load and validate environment variables"""
    # Load from .env file for local development
    load_dotenv()
    
    # Critical environment variables
    required_vars = [
        'AZURE_STORAGE_ACCOUNT_NAME',
        'AZURE_STORAGE_ACCOUNT_KEY',
        'WORKFLOW_EXPLAIN_AGENT_ENDPOINT',
        'API_KEY',
        'AZURE_SEARCH_AI_ENDPOINT',
        'AZURE_SEARCH_AI_API_KEY',
        'AZURE_SEARCH_AI_INDEX_NAME'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    logging.info("✅ Environment variables loaded and validated")
    return True

def parse_arguments():
    """Parse command line arguments for flexible execution"""
    parser = argparse.ArgumentParser(description="SharePoint Document Processor")
    
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Maximum pages to process per execution (default: 20)"
    )
    
    parser.add_argument(
        "--timeout",
        type=int,
        default=25,
        help="Processing timeout in minutes (default: 25)"
    )
    
    parser.add_argument(
        "--folder",
        type=str,
        help="Specific SharePoint folder to process (overrides env var)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without actually indexing to Azure Search"
    )
    
    parser.add_argument(
        "--reset-tracking",
        action="store_true",
        help="Reset file tracking and start from beginning"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    return parser.parse_args()

def load_last_files_json():
    """Load the last_files.json from local file system"""
    try:
        with open("last_files.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        logging.info(f"Successfully loaded local last_files.json with {len(data)} file entries")
        return data
    except FileNotFoundError:
        logging.info("last_files.json not found locally. Creating new tracking file.")
        # Create empty tracking file
        empty_data = {}
        save_last_files_json(empty_data)
        return empty_data
    except Exception as e:
        logging.error(f"Error loading last_files.json: {e}. Starting with empty tracking.")
        return {}

def save_last_files_json(last_files_data: dict):
    """Save the last_files.json to local file system"""
    try:
        with open("last_files.json", "w", encoding="utf-8") as f:
            json.dump(last_files_data, f, indent=2, ensure_ascii=False)
        logging.info(f"Updated local last_files.json with current progress")
    except Exception as e:
        logging.error(f"Error saving last_files.json: {e}")

def update_file_progress(file_name: str, current_page: int):
    """Update the progress for a specific file in last_files.json"""
    try:
        # Load current data
        last_files_data = load_last_files_json()
        
        # Update the specific file
        last_files_data[file_name] = current_page
        
        # Save back to local file
        save_last_files_json(last_files_data)
        logging.info(f"Updated progress for {file_name}: page {current_page}")
    except Exception as e:
        logging.error(f"Error updating file progress: {e}")

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
                    # Clean the file path to ensure proper URL construction
                    clean_file_path = file_path.strip().lstrip('/')
                    file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{clean_file_path}"
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

def initialize_azure_clients():
    """Initialize all Azure service clients"""
    clients = {}
    
    # Storage clients
    storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    
    clients['blob_service'] = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net",
        credential=storage_account_key
    )
    
    # Table Storage for state tracking
    table_storage_connection_string = os.getenv("TABLE_STORAGE_CONNECTION_STRING", "")
    state_table_name = os.getenv("STATE_TABLE_NAME", "documentprocessing")
    
    if table_storage_connection_string:
        clients['table_service'] = TableServiceClient.from_connection_string(table_storage_connection_string)
        clients['table_client'] = clients['table_service'].get_table_client(state_table_name)
        
        # Ensure table exists
        try:
            clients['table_client'].create_table()
        except:
            pass  # Table already exists
    else:
        clients['table_service'] = None
        clients['table_client'] = None
    
    # OpenAI clients
    clients['openai'] = AzureOpenAI(
        api_version=os.getenv("WORKFLOW_EXPLAIN_AGENT_API_VERSION"),
        azure_endpoint=os.getenv("WORKFLOW_EXPLAIN_AGENT_ENDPOINT"),
        api_key=os.getenv("API_KEY")
    )
    
    clients['embedding'] = AzureOpenAI(
        api_version=os.getenv("EMBEDDING_MODEL_API_VERSION"),
        azure_endpoint=os.getenv("EMBEDDING_MODEL_ENDPOINT"),
        api_key=os.getenv("EMBEDDING_MODEL_API_KEY")
    )
    
    return clients

def create_progress_tracker():
    """Create progress tracking for standalone execution"""
    return {
        'start_time': datetime.datetime.now(),
        'documents_processed': 0,
        'pages_processed': 0,
        'errors': 0,
        'last_status_update': datetime.datetime.now()
    }

def update_progress_display(tracker):
    """Display progress information"""
    elapsed = datetime.datetime.now() - tracker['start_time']
    logging.info(f"📊 Progress: {tracker['documents_processed']} docs, "
                f"{tracker['pages_processed']} pages, "
                f"{tracker['errors']} errors in {elapsed}")

def process_documents(args):
    """Main document processing function"""
    # Initialize Azure clients
    clients = initialize_azure_clients()
    
    # Load environment variables
    sharepoint_scope = os.getenv("SHAREPOINT_SCOPE", "https://graph.microsoft.com/.default")
    sharepoint_dominio = os.getenv("SHAREPOINT_DOMINIO", "")
    sharepoint_site = os.getenv("SHAREPOINT_SITE", "")
    sharepoint_folder_name = args.folder or os.getenv("SHAREPOINT_FOLDER_NAME", "")
    
    # SharePoint credentials from Key Vault (recommended) or environment variables (fallback)
    try:
        sharepoint_tenant_id = get_kv_variable("Tenantid-secret")
        sharepoint_client_id = get_kv_variable("ApplicationId-secret")
        sharepoint_client_secret = get_kv_variable("ValueClient-secret")
        logging.info("✅ Successfully retrieved SharePoint credentials from Key Vault")
    except Exception as kv_error:
        logging.warning(f"⚠️ Failed to retrieve credentials from Key Vault: {kv_error}")
        logging.info("🔄 Falling back to environment variables for SharePoint credentials")
        sharepoint_tenant_id = os.getenv("SHAREPOINT_TENANT_ID", "")
        sharepoint_client_id = os.getenv("SHAREPOINT_CLIENT_ID", "")
        sharepoint_client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET", "")

    # Processing settings
    max_pages_per_execution = args.max_pages
    processing_timeout_minutes = args.timeout
    image_quality_dpi = int(os.getenv("IMAGE_QUALITY_DPI", "200"))
    
    # Search configuration
    search_index_name = os.getenv("AZURE_SEARCH_AI_INDEX_NAME")
    search_endpoint = os.getenv("AZURE_SEARCH_AI_ENDPOINT")
    search_api_key = os.getenv("AZURE_SEARCH_AI_API_KEY")
    ocp_apim_subs_key = os.getenv("Ocp-Apim-Subscription-Key")
    search_api_version = os.getenv("AZURE_SEARCH_AI_API_VERSION")

    # API Management
    api_management_key = os.getenv("API_MANAGEMENT_KEY")

    # Model configurations
    agent_deployment = os.getenv("WORKFLOW_EXPLAIN_AGENT_DEPLOYMENT")
    embedding_model_deployment = os.getenv("EMBEDDING_MODEL_DEPLOYMENT")
    
    # Agent prompt
    agent_prompt = "Eres un asistente útil especializado en analizar imágenes. Por favor, realiza las siguientes tareas: 1. Si la imagen contiene solo texto, transcribe el texto exacto sin agregar explicaciones adicionales. 2. Si la imagen contiene gráficos, diagramas, tablas u otros elementos visuales, proporciona una descripción detallada y precisa de su contenido en español. 3. Asegúrate de no omitir ningún detalle importante ni agregar información que no esté presente en la imagen."
    
    # Progress tracking
    progress = create_progress_tracker()
    
    # Reset tracking if requested
    if args.reset_tracking:
        logging.info("🔄 Resetting file tracking...")
        save_last_files_json({})
    
    # Get documents from SharePoint
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
    
    # Load file processing tracking from local file
    last_files_data = load_last_files_json()
    logging.info(f"File tracking enabled. Loaded tracking data: {last_files_data}")
    
    try:
        for doc_info in documents:
            # Check execution time limit
            elapsed_time = datetime.datetime.now() - progress['start_time']
            if elapsed_time.total_seconds() / 60 >= processing_timeout_minutes:
                logging.info(f"Approaching timeout limit ({processing_timeout_minutes} minutes). Stopping execution.")
                break
                
            if progress['pages_processed'] >= max_pages_per_execution:
                logging.info(f"Reached maximum pages limit ({max_pages_per_execution}). Stopping execution.")
                break
            
            doc_name = doc_info['name']
            doc_content = doc_info['content']
            doc_hash = doc_info['hash']
            
            logging.info(f"Processing document: {doc_name}")
            
            # Check processing state
            last_processed_page, is_completed = get_processing_state(clients['table_client'], doc_name, doc_hash) if clients['table_client'] else (0, False)
            
            # Get page count using PyMuPDF (fitz)
            try:
                pdf_doc = fitz.open(stream=doc_content, filetype="pdf")
                total_pages = len(pdf_doc)
                pdf_doc.close()
                logging.info(f"Document page count analysis completed for: {doc_name}. Total pages: {total_pages}")
                
                # Check last_files.json tracking
                last_processed_from_json = last_files_data.get(doc_name, 0)
                logging.info(f"File {doc_name}: Last processed page from JSON: {last_processed_from_json}, Total pages: {total_pages}")
                
                # Use the maximum of both tracking methods for safety
                last_processed_page = max(last_processed_page, last_processed_from_json)
                
                if last_processed_page >= total_pages:
                    logging.info(f"Document {doc_name} already fully processed (page {last_processed_page}/{total_pages}). Skipping.")
                    continue
                
                # Update state with total pages
                if clients['table_client']:
                    update_processing_state(clients['table_client'], doc_name, doc_hash, last_processed_page, total_pages, 'processing')
                
            except Exception as e:
                logging.error(f"Error analyzing document {doc_name}: {e}")
                progress['errors'] += 1
                continue

            # Process pages starting from last processed page
            for page_idx in range(last_processed_page, total_pages):
                # Check limits again
                elapsed_time = datetime.datetime.now() - progress['start_time']
                if elapsed_time.total_seconds() / 60 >= processing_timeout_minutes:
                    logging.info(f"Timeout approaching. Stopping at page {page_idx + 1}")
                    break
                    
                if progress['pages_processed'] >= max_pages_per_execution:
                    logging.info(f"Page limit reached. Stopping at page {page_idx + 1}")
                    break
                
                try:
                    logging.info(f"Processing page {page_idx + 1}/{total_pages} of document: {doc_name} (Session total: {progress['pages_processed'] + 1})")

                    # Create PDF document from content for page extraction
                    pdf_doc = fitz.open(stream=doc_content, filetype="pdf")
                    
                    # Capture the page as an image with optimized quality
                    pix = pdf_doc[page_idx].get_pixmap(dpi=image_quality_dpi)
                    img_data = pix.tobytes("png")
                    
                    # Convert directly to data URL without saving to disk
                    img_base64 = base64.b64encode(img_data).decode("utf-8")
                    image_data_url = f"data:image/png;base64,{img_base64}"
                    
                    pdf_doc.close()
                    
                    logging.info(f"Page {page_idx + 1} converted to data URL")

                    # Minimal delay to avoid overwhelming APIs
                    time.sleep(0.5)

                    try:
                        # Send the image data URL to the agent for explanation
                        agent_response = clients['openai'].chat.completions.create(
                            messages=[
                                {"role": "system", "content": agent_prompt},
                                {"role": "user", "content": [
                                    {"type": "text", "text": "Por favor, analiza esta imagen."},
                                    {"type": "image_url", "image_url": {"url": image_data_url}}
                                ]}
                            ],
                            max_tokens=3000,
                            temperature=0.0,
                            model=agent_deployment
                        )
                        
                        # Process explanation
                        raw_explanation = agent_response.choices[0].message.content
                        explanation = deep_unicode_clean(raw_explanation)
                        
                        if args.verbose:
                            logging.info(f'Agent response (first 200 chars): {explanation[:200]}...')
                            
                    except Exception as e:
                        logging.error(f"Error occurred while getting agent response: {e}")
                        progress['errors'] += 1
                        continue

                    # Generate embedding
                    try:
                        embedding_response = clients['embedding'].embeddings.create(
                            input=[explanation],
                            model=embedding_model_deployment
                        )
                        embedding_vector = embedding_response.data[0].embedding
                    except Exception as e:
                        logging.error(f"Error generating embedding for page {page_idx + 1}: {e}")
                        embedding_vector = None
                        progress['errors'] += 1

                    # Generate UUID for document ID
                    document_id = str(uuid.uuid4())

                    # Extract title from filename using regex
                    title_pattern = r'^[A-Z]{3}-\d+(?: - \d+)? - (.*?)(?: - \d{2}[.-]\d{2}[.-]\d{4})?\.pdf$'
                    title_match = re.match(title_pattern, doc_name)
                    if title_match:
                        extracted_title = title_match.group(1).strip()
                        document_title = f"Page {page_idx + 1} of {extracted_title}"
                    else:
                        document_title = f"Page {page_idx + 1} of {doc_name}"

                    # Prepare payload for Azure Search
                    payload = {
                        "value": [{
                            "id": document_id,
                            "metadata_spo_item_name": doc_name,
                            "metadata_spo_item_path": doc_info.get('web_url', f"https://{sharepoint_dominio}.sharepoint.com/sites/{sharepoint_site}/Documentos%20Compartidos/{sharepoint_folder_name}/{doc_name}"),
                            "metadata_spo_item_created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            "metadata_spo_item_last_modified": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            "metadata_spo_item_title": document_title,
                            "metadata_spo_item_content": explanation,
                            "embedding": embedding_vector if embedding_vector else []
                        }]
                    }

                    # Index in Azure Search (skip if dry run)
                    if not args.dry_run:
                        max_retries = 3
                        retry_delay = 1
                        
                        for attempt in range(max_retries):
                            url = f"{search_endpoint}/searchindex/{search_index_name}/docs/search.index?api-version={search_api_version}"
                            logging.info(f'Making request to: {url}\nwith APIM Subscription key = {ocp_apim_subs_key}')
                            try:
                                search_response = requests.post(
                                    url=url,
                                    headers={
                                        "Content-Type": "application/json",
                                        "Ocp-Apim-Subscription-Key": ocp_apim_subs_key

                                    },
                                    json=payload,
                                    timeout=30
                                )

                                if search_response.status_code == 200:
                                    logging.info(f"✅ Indexed page {page_idx + 1} successfully.")
                                    progress['pages_processed'] += 1
                                    
                                    # Update progress in both state table and file tracking
                                    if clients['table_client']:
                                        status = 'completed' if page_idx + 1 == total_pages else 'processing'
                                        update_processing_state(clients['table_client'], doc_name, doc_hash, page_idx + 1, total_pages, status)
                                    
                                    # Update file progress in local file
                                    update_file_progress(doc_name, page_idx + 1)
                                    
                                    break  # Success, exit retry loop
                                    
                                else:
                                    logging.error(f"Failed to index page {page_idx + 1}. Status: {search_response.status_code}")
                                    if attempt < max_retries - 1:
                                        time.sleep(retry_delay)
                                        retry_delay *= 2
                                    else:
                                        progress['errors'] += 1
                                        
                            except Exception as e:
                                logging.error(f"Error during indexing attempt {attempt + 1}: {e}")
                                if attempt == max_retries - 1:
                                    progress['errors'] += 1
                    else:
                        logging.info(f"🔍 DRY RUN: Would index page {page_idx + 1} (skipped)")
                        progress['pages_processed'] += 1
                        update_file_progress(doc_name, page_idx + 1)

                except Exception as e:
                    logging.error(f"Error processing page {page_idx + 1} of document {doc_name}: {e}")
                    progress['errors'] += 1
                    continue
                
                # Progress update every 10 pages
                if progress['pages_processed'] % 10 == 0:
                    update_progress_display(progress)
            
            progress['documents_processed'] += 1
            logging.info(f"✅ Completed processing document {doc_name}")

    except Exception as e:
        logging.error(f"Error in main processing loop: {e}")
        progress['errors'] += 1

    # Final progress report
    elapsed_time = datetime.datetime.now() - progress['start_time']
    logging.info(f"📊 FINAL RESULTS:")
    logging.info(f"   Documents processed: {progress['documents_processed']}")
    logging.info(f"   Pages processed: {progress['pages_processed']}")
    logging.info(f"   Errors encountered: {progress['errors']}")
    logging.info(f"   Total execution time: {elapsed_time.total_seconds():.1f} seconds")
    
    return progress

def main():
    """Main execution function"""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Setup logging
        logger = setup_logging()
        
        # Set verbose logging if requested
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        logger.info("=== STANDALONE DOCUMENT PROCESSOR STARTED ===")
        logger.info(f"Configuration: max_pages={args.max_pages}, timeout={args.timeout}min, dry_run={args.dry_run}")
        
        # Load and validate environment
        load_environment()
        
        # Process documents
        progress = process_documents(args)
        
        # Success
        logger.info("=== PROCESSING COMPLETED SUCCESSFULLY ===")
        return 0
        
    except KeyboardInterrupt:
        logging.info("⚠️ Processing interrupted by user")
        return 1
    except Exception as e:
        logging.error(f"❌ Critical error in main execution: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
