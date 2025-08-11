import os
import logging
from collections import defaultdict
from dotenv import load_dotenv
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from utils.keyvault import get_kv_variable

def explore_folder_recursively(ctx, folder_path: str, level: int = 0, max_level: int = 3):
    """
    Recursively explore a SharePoint folder and return its structure
    """
    if level > max_level:
        return {"error": "Max depth reached"}
    
    indent = "  " * level
    folder_info = {
        "name": folder_path.split('/')[-1] if '/' in folder_path else folder_path,
        "path": folder_path,
        "subfolders": [],
        "files": [],
        "file_types": defaultdict(int),
        "total_files": 0,
        "total_folders": 0,
        "errors": []
    }
    
    try:
        # Get the folder
        folder = ctx.web.get_folder_by_server_relative_url(folder_path)
        ctx.load(folder)
        ctx.load(folder.folders)
        ctx.load(folder.files)
        ctx.execute_query()
        
        print(f"{indent}📁 {folder_info['name']}")
        
        # Process files in current folder
        for file_obj in folder.files:
            try:
                file_name = getattr(file_obj, 'name', 'Unknown')
                file_info = {
                    "name": file_name,
                    "server_relative_url": getattr(file_obj, 'server_relative_url', '')
                }
                folder_info["files"].append(file_info)
                
                # Count file types
                if '.' in file_name:
                    ext = '.' + file_name.split('.')[-1].lower()
                    folder_info["file_types"][ext] += 1
                else:
                    folder_info["file_types"]["no_extension"] += 1
                    
                folder_info["total_files"] += 1
                print(f"{indent}  📄 {file_name}")
                
            except Exception as file_error:
                error_msg = f"Error accessing file: {str(file_error)[:50]}..."
                folder_info["errors"].append(error_msg)
                print(f"{indent}  ❌ {error_msg}")
        
        # Process subfolders recursively
        for subfolder in folder.folders:
            try:
                subfolder_name = getattr(subfolder, 'name', 'Unknown')
                subfolder_path = getattr(subfolder, 'server_relative_url', '')
                
                if subfolder_name not in ['Forms', 'Item', '_vti_cnf']:  # Skip system folders
                    folder_info["total_folders"] += 1
                    subfolder_info = explore_folder_recursively(ctx, subfolder_path, level + 1, max_level)
                    folder_info["subfolders"].append(subfolder_info)
                    
                    # Aggregate counts from subfolders
                    folder_info["total_files"] += subfolder_info.get("total_files", 0)
                    folder_info["total_folders"] += subfolder_info.get("total_folders", 0)
                    
                    # Aggregate file types
                    for ext, count in subfolder_info.get("file_types", {}).items():
                        folder_info["file_types"][ext] += count
                    
            except Exception as subfolder_error:
                error_msg = f"Error accessing subfolder {subfolder_name}: {str(subfolder_error)[:50]}..."
                folder_info["errors"].append(error_msg)
                print(f"{indent}  ❌ {error_msg}")
        
        # Print summary for this folder
        if folder_info["total_files"] > 0 or folder_info["total_folders"] > 0:
            print(f"{indent}📊 Summary: {folder_info['total_folders']} folders, {folder_info['total_files']} files")
            if folder_info["file_types"]:
                print(f"{indent}📋 File types:")
                for ext, count in sorted(folder_info["file_types"].items()):
                    print(f"{indent}   {ext}: {count}")
        
    except Exception as folder_error:
        error_msg = f"Cannot access folder {folder_path}: {str(folder_error)[:100]}..."
        folder_info["errors"].append(error_msg)
        print(f"{indent}❌ {error_msg}")
    
    return folder_info

def explore_documentos_comprehensive(sharepoint_site_url: str, client_id: str, client_secret: str, tenant_id: str):
    """
    Comprehensive exploration of the Documentos library using the same pattern as function_app.py
    """
    try:
        # Authenticate using App Registration (same as function_app.py)
        print("🔐 Authenticating to SharePoint using App Registration...")
        credentials = ClientCredential(client_id, client_secret)
        ctx = ClientContext(sharepoint_site_url).with_credentials(credentials)
        
        # Test connection
        try:
            web = ctx.web
            ctx.load(web)
            ctx.execute_query()
            print(f"✅ Connected to: {web.title}")
        except Exception as auth_error:
            print(f"❌ SharePoint authentication failed: {auth_error}")
            return
        
        print("=" * 80)
        
        # Find document libraries (same logic as function_app.py)
        lists = ctx.web.lists
        ctx.load(lists)
        ctx.execute_query()
        
        # Find the main Documents library
        doc_library = None
        print("🔍 Searching for document libraries...")
        
        for lib in lists:
            if lib.base_type == 1:  # Document Library
                library_title = lib.title.lower()
                print(f"   📚 Found library: {lib.title} ({lib.item_count} items)")
                
                if ('document' in library_title or 'compartido' in library_title or 
                    'shared' in library_title or lib.title == 'Documents'):
                    print(f"   ✅ Selected main document library: {lib.title}")
                    doc_library = lib
                    break
        
        if not doc_library:
            print("❌ No main document library found")
            return
        
        print(f"\n📚 EXPLORING {doc_library.title.upper()} LIBRARY")
        print(f"📊 Total items in library: {doc_library.item_count}")
        print("=" * 80)
        
        # Use the same approach as function_app.py - get all items from the library
        print("🔍 Getting all items from the document library...")
        try:
            items = doc_library.items
            ctx.load(items)
            ctx.execute_query()
            
            print(f"✅ Successfully loaded {len(items)} items from the library")
            
            # If no items, try folder-based approach
            if len(items) == 0:
                print("⚠️  No items loaded - trying folder-based exploration...")
                
                try:
                    root_folder = doc_library.root_folder
                    ctx.load(root_folder)
                    ctx.load(root_folder.files)
                    ctx.load(root_folder.folders)
                    ctx.execute_query()
                    
                    print(f"✅ Root folder has {len(root_folder.files)} files and {len(root_folder.folders)} folders")
                    
                    # Process files in root folder
                    total_files = 0
                    total_folders = 0
                    file_types = defaultdict(int)
                    
                    print("\n📄 FILES IN ROOT FOLDER:")
                    for file_obj in root_folder.files:
                        file_name = getattr(file_obj, 'name', 'Unknown')
                        print(f"   • {file_name}")
                        total_files += 1
                        
                        if '.' in file_name:
                            ext = '.' + file_name.split('.')[-1].lower()
                            file_types[ext] += 1
                        else:
                            file_types['no_extension'] += 1
                    
                    print("\n📁 FOLDERS IN ROOT:")
                    for folder_obj in root_folder.folders:
                        folder_name = getattr(folder_obj, 'name', 'Unknown')
                        if folder_name not in ['Forms', 'Item', '_vti_cnf']:  # Skip system folders
                            print(f"   📁 {folder_name}")
                            total_folders += 1
                            
                            # Try to access this folder's contents
                            try:
                                ctx.load(folder_obj.files)
                                ctx.load(folder_obj.folders)
                                ctx.execute_query()
                                
                                print(f"      📄 Files: {len(folder_obj.files)}")
                                for file_obj in folder_obj.files:
                                    file_name = getattr(file_obj, 'name', 'Unknown')
                                    print(f"         • {file_name}")
                                    total_files += 1
                                    
                                    if '.' in file_name:
                                        ext = '.' + file_name.split('.')[-1].lower()
                                        file_types[ext] += 1
                                    else:
                                        file_types['no_extension'] += 1
                                    
                                    # Highlight PDF files
                                    if file_name.lower().endswith('.pdf'):
                                        print(f"         🎯 PDF FOUND: {file_name}")
                                
                                print(f"      📁 Subfolders: {len(folder_obj.folders)}")
                                for subfolder_obj in folder_obj.folders:
                                    subfolder_name = getattr(subfolder_obj, 'name', 'Unknown')
                                    if subfolder_name not in ['Forms', 'Item', '_vti_cnf']:
                                        print(f"         📁 {subfolder_name}")
                                        total_folders += 1
                                        
                            except Exception as folder_error:
                                print(f"      ❌ Cannot access folder contents: {str(folder_error)[:100]}...")
                    
                    # Print summary
                    print("\n" + "=" * 80)
                    print("📋 FINAL SUMMARY")
                    print("=" * 80)
                    print(f"Total folders found: {total_folders}")
                    print(f"Total files found: {total_files}")
                    
                    if file_types:
                        print(f"\n📊 FILE TYPES BREAKDOWN:")
                        for ext, count in sorted(file_types.items(), key=lambda x: x[1], reverse=True):
                            print(f"   {ext}: {count} files")
                    
                    return  # Exit successfully after folder exploration
                    
                except Exception as folder_error:
                    print(f"❌ Cannot access root folder: {folder_error}")
            
            # If we have items from the original query, process them
            if len(items) > 0:
                folders = {}
                files = {}
                file_types = defaultdict(int)
                total_files = 0
                total_folders = 0
                
                print("\n📋 ANALYZING LIBRARY CONTENT...")
                print("=" * 60)
            
            for item in items:
                try:
                    # Get basic item properties
                    item_name = item.properties.get('FileLeafRef', 'Unknown')
                    file_ref = item.properties.get('FileRef', '')
                    file_dir_ref = item.properties.get('FileDirRef', '')
                    file_system_object_type = getattr(item, 'file_system_object_type', None)
                    
                    if file_system_object_type == 1:  # Folder
                        total_folders += 1
                        folder_path = file_dir_ref if file_dir_ref else file_ref
                        if folder_path not in folders:
                            folders[folder_path] = []
                        print(f"📁 Folder: {item_name} (Path: {file_ref})")
                        
                    elif file_system_object_type == 0:  # File
                        total_files += 1
                        folder_path = file_dir_ref
                        
                        # Track file types
                        if '.' in item_name:
                            ext = '.' + item_name.split('.')[-1].lower()
                            file_types[ext] += 1
                        else:
                            file_types['no_extension'] += 1
                        
                        # Organize files by folder
                        if folder_path not in files:
                            files[folder_path] = []
                        files[folder_path].append(item_name)
                        
                        print(f"📄 File: {item_name} (Folder: {folder_path})")
                        
                        # Special highlight for PDF files in specific folders
                        if item_name.lower().endswith('.pdf'):
                            if 'continuidad' in folder_path.lower() or 'negocio' in folder_path.lower():
                                print(f"   🎯 TARGET PDF: {item_name} in target folder!")
                    
                except Exception as item_error:
                    print(f"❌ Error processing item: {str(item_error)[:100]}...")
            
            # Print organized results
            print("\n" + "=" * 80)
            print("� FOLDER STRUCTURE ANALYSIS")
            print("=" * 80)
            
            for folder_path in sorted(files.keys()):
                folder_files = files[folder_path]
                if folder_files:
                    folder_name = folder_path.split('/')[-1] if '/' in folder_path else folder_path
                    print(f"\n📁 {folder_name}")
                    print(f"   📍 Path: {folder_path}")
                    print(f"   📊 Files: {len(folder_files)}")
                    
                    # Show file types in this folder
                    folder_file_types = defaultdict(int)
                    for file_name in folder_files:
                        if '.' in file_name:
                            ext = '.' + file_name.split('.')[-1].lower()
                            folder_file_types[ext] += 1
                        else:
                            folder_file_types['no_extension'] += 1
                    
                    print(f"   � File types:")
                    for ext, count in sorted(folder_file_types.items()):
                        print(f"      {ext}: {count}")
                    
                    # Show first few files as examples
                    print(f"   📄 Files:")
                    for file_name in sorted(folder_files)[:5]:  # Show first 5 files
                        print(f"      • {file_name}")
                    if len(folder_files) > 5:
                        print(f"      ... and {len(folder_files) - 5} more files")
            
            # Print final summary
            print("\n" + "=" * 80)
            print("📋 FINAL SUMMARY")
            print("=" * 80)
            print(f"Total folders found: {total_folders}")
            print(f"Total files found: {total_files}")
            
            if file_types:
                print(f"\n📊 FILE TYPES BREAKDOWN:")
                for ext, count in sorted(file_types.items(), key=lambda x: x[1], reverse=True):
                    print(f"   {ext}: {count} files")
            
            # Look for target folder content
            target_folders = [path for path in files.keys() if 'continuidad' in path.lower() and 'negocio' in path.lower()]
            if target_folders:
                print(f"\n🎯 TARGET FOLDER ANALYSIS:")
                for target_folder in target_folders:
                    target_files = files[target_folder]
                    pdf_files = [f for f in target_files if f.lower().endswith('.pdf')]
                    print(f"   📁 {target_folder}")
                    print(f"   📄 Total files: {len(target_files)}")
                    print(f"   📄 PDF files: {len(pdf_files)}")
                    if pdf_files:
                        print(f"   📋 PDF files:")
                        for pdf_file in pdf_files:
                            print(f"      • {pdf_file}")
        
        except Exception as items_error:
            print(f"❌ Error loading library items: {items_error}")
            
    except Exception as e:
        print(f"❌ Connection error: {e}")

def explore_documentos_only(sharepoint_site_url: str, client_id: str, client_secret: str, tenant_id: str):
    """
    Focus only on the Documentos library
    """
    try:
        # Authenticate to SharePoint
        print("🔐 Authenticating to SharePoint...")
        credentials = ClientCredential(client_id, client_secret)
        ctx = ClientContext(sharepoint_site_url).with_credentials(credentials)
        
        # Test connection
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        print(f"✅ Connected to: {web.title}")
        print("=" * 70)
        
        # Get all lists and libraries
        lists = ctx.web.lists
        ctx.load(lists)
        ctx.execute_query()
        
        # Find the Documentos library
        documentos_lib = None
        for lib in lists:
            if lib.title == "Documentos":
                documentos_lib = lib
                break
        
        if not documentos_lib:
            print("❌ Documentos library not found")
            return
        
        print(f"📚 EXPLORING DOCUMENTOS LIBRARY ({documentos_lib.item_count} items)")
        print("=" * 60)
        
        # Method 1: Try CAML Query to get all items
        try:
            print("🔍 Method 1: Using CAML Query...")
            from office365.sharepoint.caml.caml_query import CamlQuery
            
            # Query to get all items
            caml_query = CamlQuery.create_all_items_query()
            items = documentos_lib.get_items(caml_query)
            ctx.load(items)
            ctx.execute_query()
            
            if len(items) > 0:
                print(f"   ✅ Found {len(items)} items via CAML query")
                
                folders = []
                files = []
                
                for item in items:
                    try:
                        # Load all properties
                        ctx.load(item)
                        ctx.execute_query()
                        
                        item_name = item.properties.get('FileLeafRef', 'Unknown')
                        item_type = item.properties.get('FSObjType', 'Unknown')
                        item_path = item.properties.get('FileRef', 'Unknown')
                        
                        print(f"      📄 {item_name} (Type: {item_type})")
                        
                        if item_type == 1:  # Folder
                            folders.append({'name': item_name, 'path': item_path})
                        elif item_type == 0:  # File
                            files.append({'name': item_name, 'path': item_path})
                    except Exception as item_error:
                        print(f"      ❌ Error processing item: {item_error}")
                
                print(f"\n   📊 SUMMARY: {len(folders)} folders, {len(files)} files")
                
                # Show folders
                if folders:
                    print(f"\n   📁 FOLDERS ({len(folders)} total):")
                    for i, folder in enumerate(folders, 1):
                        print(f"      {i}. {folder['name']}")
                
                # Show files with extensions
                if files:
                    print(f"\n   📄 FILES ({len(files)} total):")
                    extension_count = defaultdict(int)
                    for file_info in files:
                        file_name = file_info['name']
                        if '.' in file_name:
                            ext = '.' + file_name.split('.')[-1].lower()
                            extension_count[ext] += 1
                        else:
                            extension_count['no_extension'] += 1
                        print(f"      • {file_name}")
                    
                    print(f"\n   📊 FILE TYPES:")
                    for ext, count in sorted(extension_count.items()):
                        print(f"      {ext}: {count} files")
                
            else:
                print("   ❌ No items found via CAML query")
                
        except Exception as e:
            print(f"   ❌ CAML query failed: {str(e)[:100]}...")
        
        # Method 2: Try to access via REST API endpoint
        try:
            print(f"\n🔍 Method 2: Direct REST API access...")
            
            # Get the library by title
            list_by_title = ctx.web.lists.get_by_title("Documentos")
            ctx.load(list_by_title)
            ctx.execute_query()
            
            # Get all items from the list
            all_items = list_by_title.items
            ctx.load(all_items)
            ctx.execute_query()
            
            print(f"   ✅ Found {len(all_items)} items via REST API")
            
            if len(all_items) > 0:
                folders = []
                files = []
                
                for item in all_items:
                    try:
                        # Get item properties
                        file_ref = item.properties.get('FileRef', '')
                        file_name = item.properties.get('FileLeafRef', 'Unknown')
                        obj_type = item.properties.get('FSObjType', 0)
                        
                        print(f"      📄 {file_name} (Path: {file_ref}, Type: {obj_type})")
                        
                        if obj_type == 1:  # Folder
                            folders.append({'name': file_name, 'path': file_ref})
                        else:  # File
                            files.append({'name': file_name, 'path': file_ref})
                            
                    except Exception as item_error:
                        print(f"      ❌ Error: {item_error}")
                
                print(f"\n   📊 FINAL SUMMARY: {len(folders)} folders, {len(files)} files")
                
                if folders:
                    print(f"\n   📁 ROOT FOLDERS:")
                    for i, folder in enumerate(folders, 1):
                        print(f"      {i}. 📁 {folder['name']}")
                        
                        # Try to explore each folder
                        try:
                            folder_obj = ctx.web.get_folder_by_server_relative_url(folder['path'])
                            ctx.load(folder_obj)
                            ctx.load(folder_obj.files)
                            ctx.load(folder_obj.folders)
                            ctx.execute_query()
                            
                            subfolder_count = len(folder_obj.folders)
                            file_count = len(folder_obj.files)
                            
                            print(f"         📊 Contains: {subfolder_count} subfolders, {file_count} files")
                            
                            if file_count > 0:
                                extension_count = defaultdict(int)
                                for file_obj in folder_obj.files:
                                    try:
                                        file_name = getattr(file_obj, 'name', 'Unknown')
                                        if '.' in file_name:
                                            ext = '.' + file_name.split('.')[-1].lower()
                                            extension_count[ext] += 1
                                    except:
                                        pass
                                
                                print("         📄 File types:")
                                for ext, count in sorted(extension_count.items()):
                                    print(f"            {ext}: {count}")
                            
                        except Exception as folder_error:
                            print(f"         ❌ Cannot access folder: {str(folder_error)[:60]}...")
                
                if files:
                    print(f"\n   📄 ROOT FILES:")
                    extension_count = defaultdict(int)
                    for file_info in files:
                        file_name = file_info['name']
                        print(f"      • {file_name}")
                        if '.' in file_name:
                            ext = '.' + file_name.split('.')[-1].lower()
                            extension_count[ext] += 1
                    
                    if extension_count:
                        print(f"\n   📊 ROOT FILE TYPES:")
                        for ext, count in sorted(extension_count.items()):
                            print(f"      {ext}: {count} files")
                
        except Exception as e:
            print(f"   ❌ REST API access failed: {str(e)[:100]}...")
        
    except Exception as e:
        print(f"❌ Connection error: {e}")

def main():
    """Main function with Key Vault fallback authentication (same as function_app.py)"""
    print("🚀 SHAREPOINT DOCUMENTOS COMPREHENSIVE EXPLORER")
    print("=" * 60)
    
    # Load environment variables
    load_dotenv()
    
    # Get SharePoint credentials with Key Vault fallback (same as function_app.py)
    try:
        # Try Key Vault first (for production), fallback to environment variables (for local development)
        try:
            sharepoint_site_url = os.getenv("SHAREPOINT_SITE_URL", "")  # Not in Key Vault
            client_id = get_kv_variable("ApplicationId-secret")
            client_secret = get_kv_variable("ValueClient-secret")
            tenant_id = get_kv_variable("Tenantid-secret")
            print("✅ Successfully retrieved SharePoint credentials from Key Vault")
        except Exception as kv_error:
            print(f"⚠️  Failed to retrieve credentials from Key Vault: {kv_error}")
            print("🔄 Falling back to environment variables for SharePoint credentials")
            sharepoint_site_url = os.getenv("SHAREPOINT_SITE_URL", "")
            client_id = os.getenv("SHAREPOINT_CLIENT_ID", "")
            client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET", "")
            tenant_id = os.getenv("SHAREPOINT_TENANT_ID", "")

        if not all([sharepoint_site_url, client_id, client_secret, tenant_id]):
            print("❌ Missing required SharePoint configuration")
            missing = []
            if not sharepoint_site_url: missing.append("SHAREPOINT_SITE_URL")
            if not client_id: missing.append("SHAREPOINT_CLIENT_ID")
            if not client_secret: missing.append("SHAREPOINT_CLIENT_SECRET")
            if not tenant_id: missing.append("SHAREPOINT_TENANT_ID")
            print(f"Missing: {', '.join(missing)}")
        else:
            print("✅ SharePoint credentials loaded successfully")
            print(f"🌐 Site URL: {sharepoint_site_url}")
            print()
            
            # Run comprehensive exploration
            explore_documentos_comprehensive(sharepoint_site_url, client_id, client_secret, tenant_id)
            
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        
    print("\n🏁 Exploration completed!")

if __name__ == "__main__":
    main()
