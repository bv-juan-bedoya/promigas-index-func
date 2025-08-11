import os
import logging
from collections import defaultdict
from dotenv import load_dotenv
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext

def explore_all_approaches(sharepoint_site_url: str, client_id: str, client_secret: str, tenant_id: str):
    """
    Try multiple approaches to access SharePoint content
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
        
        print("📚 ALL LIBRARIES AND LISTS:")
        print("-" * 40)
        
        target_libraries = []
        
        for lib in lists:
            print(f"📋 {lib.title} (Type: {lib.base_type}, Items: {lib.item_count})")
            if lib.base_type == 1 and lib.item_count > 0:  # Document libraries with content
                target_libraries.append(lib)
        
        print(f"\n🎯 EXPLORING {len(target_libraries)} DOCUMENT LIBRARIES WITH CONTENT:")
        print("=" * 60)
        
        for lib in target_libraries:
            print(f"\n📚 LIBRARY: {lib.title} ({lib.item_count} items)")
            print("-" * 50)
            
            # Approach 1: Try to get items directly
            try:
                print("   Method 1: Direct items access...")
                items = lib.items
                ctx.load(items)
                ctx.execute_query()
                
                if len(items) > 0:
                    print(f"   ✅ Found {len(items)} items via direct access")
                    
                    folders = []
                    files = []
                    
                    for item in items:
                        try:
                            item_name = item.properties.get('FileLeafRef', 'Unknown')
                            item_type = item.properties.get('file_system_object_type', 'Unknown')
                            item_path = item.properties.get('FileRef', 'Unknown')
                            
                            print(f"      Item: {item_name} (Type: {item_type}, Path: {item_path})")
                            
                            if item_type == 1:  # Folder
                                folders.append({'name': item_name, 'path': item_path})
                            elif item_type == 0:  # File
                                files.append({'name': item_name, 'path': item_path})
                        except Exception as item_error:
                            print(f"      ❌ Error processing item: {item_error}")
                    
                    print(f"   📊 Summary: {len(folders)} folders, {len(files)} files")
                    
                    # Show folders
                    if folders:
                        print("   📁 Folders found:")
                        for folder in folders[:10]:  # Show first 10
                            print(f"      • {folder['name']}")
                        if len(folders) > 10:
                            print(f"      ... and {len(folders) - 10} more folders")
                    
                    # Show files with extensions
                    if files:
                        print("   📄 Files found:")
                        extension_count = defaultdict(int)
                        for file_info in files:
                            file_name = file_info['name']
                            if '.' in file_name:
                                ext = '.' + file_name.split('.')[-1].lower()
                                extension_count[ext] += 1
                            else:
                                extension_count['no_extension'] += 1
                        
                        for ext, count in sorted(extension_count.items()):
                            print(f"      {ext}: {count} files")
                        
                        # Show first few file names
                        print("   📄 Sample files:")
                        for file_info in files[:5]:
                            print(f"      • {file_info['name']}")
                        if len(files) > 5:
                            print(f"      ... and {len(files) - 5} more files")
                
                else:
                    print("   ❌ No items found via direct access")
                    
            except Exception as e:
                print(f"   ❌ Direct access failed: {str(e)[:100]}...")
            
            # Approach 2: Try to access root folder
            try:
                print("   Method 2: Root folder access...")
                root_folder = lib.root_folder
                ctx.load(root_folder)
                ctx.load(root_folder.files)
                ctx.load(root_folder.folders)
                ctx.execute_query()
                
                folder_count = len(root_folder.folders)
                file_count = len(root_folder.files)
                
                print(f"   ✅ Root folder access: {folder_count} folders, {file_count} files")
                
                if folder_count > 0:
                    print("   📁 Root folders:")
                    for folder in root_folder.folders:
                        try:
                            folder_name = getattr(folder, 'name', 'Unknown')
                            print(f"      • {folder_name}")
                        except:
                            print(f"      • [Access Error]")
                
                if file_count > 0:
                    print("   📄 Root files:")
                    extension_count = defaultdict(int)
                    for file_obj in root_folder.files:
                        try:
                            file_name = getattr(file_obj, 'name', 'Unknown')
                            print(f"      • {file_name}")
                            if '.' in file_name:
                                ext = '.' + file_name.split('.')[-1].lower()
                                extension_count[ext] += 1
                        except:
                            print(f"      • [Access Error]")
                    
                    if extension_count:
                        print("   📊 File types:")
                        for ext, count in sorted(extension_count.items()):
                            print(f"      {ext}: {count}")
                
            except Exception as e:
                print(f"   ❌ Root folder access failed: {str(e)[:100]}...")
        
    except Exception as e:
        print(f"❌ Connection error: {e}")

def main():
    """Main function"""
    # Load environment variables
    load_dotenv()
    
    # Get SharePoint configuration
    sharepoint_site_url = os.getenv("SHAREPOINT_SITE_URL", "")
    sharepoint_client_id = os.getenv("SHAREPOINT_CLIENT_ID", "")
    sharepoint_client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET", "")
    sharepoint_tenant_id = os.getenv("SHAREPOINT_TENANT_ID", "")
    
    if not all([sharepoint_site_url, sharepoint_client_id, sharepoint_client_secret, sharepoint_tenant_id]):
        print("❌ Missing SharePoint configuration in .env file")
        return
    
    print("🔍 COMPREHENSIVE SHAREPOINT EXPLORER")
    print("=" * 50)
    print(f"🌐 Site: {sharepoint_site_url.split('/')[-1]}")
    print(f"🔑 Client: {sharepoint_client_id[:8]}...")
    print()
    
    explore_all_approaches(
        sharepoint_site_url=sharepoint_site_url,
        client_id=sharepoint_client_id,
        client_secret=sharepoint_client_secret,
        tenant_id=sharepoint_tenant_id
    )

if __name__ == "__main__":
    main()
