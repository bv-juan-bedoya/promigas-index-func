import requests

# 1. Obtener access token
def get_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET, SCOPE):
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": SCOPE
    }
    resp = requests.post(url, headers=headers, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


# 2. Obtener site id de SharePoint
def get_site_id(access_token, DOMINIO, SITE):
    site_url=f"{DOMINIO}.sharepoint.com:/sites/{SITE}"
    url = f"https://graph.microsoft.com/v1.0/sites/{site_url}"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

# 3. Obtener drive id del site
def get_drive_id(access_token, site_id):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    drives = resp.json().get("value", [])
    if drives:
        return drives[0]["id"]  # Retorna el primer drive id
    return None


# 4. Listar archivos en la carpeta especificada
def list_drive_folder(
    access_token, drive_id, folder_path, parent_path="", file_counter=None
):
    if file_counter is None:
        file_counter = {"count": 0}
    
    # Handle empty folder path or root access
    if not folder_path or folder_path.strip() == "":
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
        current_folder = ""
    else:
        # Ensure folder_path doesn't start with a slash and construct the URL
        clean_folder_path = folder_path.strip().lstrip('/')
        url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:"
            f"/{clean_folder_path}:/children"
        )
        current_folder = clean_folder_path
    
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    items = resp.json().get("value", [])
    items_list = []
    
    for item in items:
        # Build the full path including the current folder
        if current_folder:
            if parent_path:
                item_path = f"{parent_path}/{item['name']}"
            else:
                item_path = f"{current_folder}/{item['name']}"
        else:
            item_path = f"{parent_path}/{item['name']}".lstrip("/")
            
        if "folder" in item:
            # print(f"Carpeta: {item_path}")
            # Recursivamente listar el contenido de la subcarpeta
            if current_folder:
                subfolder_path = f"{current_folder}/{item['name']}"
            else:
                subfolder_path = item['name']
            list_drive_folder(
                access_token, drive_id, subfolder_path, item_path, file_counter
            )
        else:
            # print(f"Archivo: {item_path}")
            file_counter["count"] += 1
            items_list.append(item_path)
    return file_counter["count"], items_list