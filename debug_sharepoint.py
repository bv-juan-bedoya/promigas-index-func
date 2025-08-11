import os
from dotenv import load_dotenv
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext

def debug_sharepoint_permissions():
    """Debug SharePoint permissions and access levels"""
    load_dotenv()
    
    # Get credentials
    sharepoint_site_url = os.getenv("SHAREPOINT_SITE_URL")
    client_id = os.getenv("SHAREPOINT_CLIENT_ID")
    client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
    tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
    
    print("🔍 SHAREPOINT DOCUMENTOS LIBRARY CHECKER")
    print("=" * 50)
    print(f"Target: {sharepoint_site_url}/Documentos")
    print()
    
    # Authenticate
    credentials = ClientCredential(client_id, client_secret)
    ctx = ClientContext(sharepoint_site_url).with_credentials(credentials)
    
    # Test basic connectivity
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    print(f"✅ Connected to: {web.title}")
    
    # Access only the Documentos library directly
    print(f"\n� ACCESSING DOCUMENTOS LIBRARY ONLY:")
    try:
        # Get the Documentos library specifically
        doc_library = ctx.web.lists.get_by_title("Documentos")
        ctx.load(doc_library)
        ctx.execute_query()
        
        print(f"   ✅ Documentos library found")
        print(f"   📊 Item Count: {doc_library.item_count}")
        
        # Method 1: Try to get all items
        print(f"\n🔍 METHOD 1: Getting all items from Documentos")
        try:
            items = doc_library.items
            ctx.load(items)
            ctx.execute_query()
            print(f"   Items loaded: {len(items)}")
            
            if len(items) > 0:
                print(f"   🎯 FOUND {len(items)} ITEMS:")
                for i, item in enumerate(items):
                    try:
                        file_name = item.properties.get('FileLeafRef', 'Unknown')
                        file_path = item.properties.get('FileRef', 'Unknown')
                        file_type = item.properties.get('File_x0020_Type', 'Unknown')
                        object_type = item.properties.get('FSObjType', 'Unknown')
                        
                        print(f"      {i+1}. {file_name}")
                        print(f"         Type: {file_type}")
                        print(f"         Path: {file_path}")
                        print(f"         Object Type: {object_type}")
                        
                        if file_name.lower().endswith('.pdf'):
                            print(f"         🎯 PDF FILE!")
                        print()
                        
                    except Exception as item_error:
                        print(f"      {i+1}. Error reading item: {str(item_error)}")
            else:
                print(f"   ❌ No items accessible (permissions issue)")
                
        except Exception as items_error:
            print(f"   ❌ Cannot access items: {str(items_error)}")
        
        # Method 2: Try root folder approach
        print(f"\n🔍 METHOD 2: Accessing root folder")
        try:
            root_folder = doc_library.root_folder
            ctx.load(root_folder)
            ctx.load(root_folder.files)
            ctx.load(root_folder.folders)
            ctx.execute_query()
            
            print(f"   Root folder: {len(root_folder.files)} files, {len(root_folder.folders)} folders")
            
            total_items = 0
            
            # Show files in root
            if len(root_folder.files) > 0:
                print(f"   📄 FILES IN ROOT:")
                for file_obj in root_folder.files:
                    try:
                        file_name = getattr(file_obj, 'name', 'Unknown')
                        total_items += 1
                        if file_name.lower().endswith('.pdf'):
                            print(f"      🎯 PDF: {file_name}")
                        else:
                            print(f"      • {file_name}")
                    except Exception as file_error:
                        print(f"      • Error: {str(file_error)}")
            
            # Show folders and their contents
            if len(root_folder.folders) > 0:
                print(f"   📁 FOLDERS:")
                for folder_obj in root_folder.folders:
                    try:
                        folder_name = getattr(folder_obj, 'name', 'Unknown')
                        if folder_name not in ['Forms', 'Item', '_vti_cnf']:
                            print(f"      📁 {folder_name}")
                            total_items += 1
                            
                            try:
                                ctx.load(folder_obj.files)
                                ctx.execute_query()
                                
                                if len(folder_obj.files) > 0:
                                    for file_obj in folder_obj.files:
                                        try:
                                            file_name = getattr(file_obj, 'name', 'Unknown')
                                            total_items += 1
                                            if file_name.lower().endswith('.pdf'):
                                                print(f"         🎯 PDF: {file_name}")
                                            else:
                                                print(f"         • {file_name}")
                                        except Exception as file_error:
                                            print(f"         • Error: {str(file_error)}")
                                else:
                                    print(f"         (Empty folder)")
                            except Exception as folder_files_error:
                                print(f"         Cannot access files: {str(folder_files_error)}")
                    except Exception as folder_error:
                        print(f"      � Error: {str(folder_error)}")
            
            print(f"\n   �📊 TOTAL ACCESSIBLE ITEMS: {total_items}")
            print(f"   📊 LIBRARY REPORTS: {doc_library.item_count} items")
            
            if total_items != doc_library.item_count:
                print(f"   ⚠️  MISMATCH: {doc_library.item_count - total_items} items are hidden due to permissions")
            
        except Exception as folder_error:
            print(f"   ❌ Root folder access failed: {str(folder_error)}")
        
    except Exception as lib_error:
        print(f"❌ Cannot access Documentos library: {str(lib_error)}")

if __name__ == "__main__":
    debug_sharepoint_permissions()
