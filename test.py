import requests
 
url = "https://apin0-l5ix2i2w7uqfk.azure-api.net/searchindex/ragindex/docs"
 
payload = {}
headers = {
  'Ocp-Apim-Subscription-Key': '6988ed70638946e395fb8dd86ef1f1e3'
}
 
response = requests.request("GET", url, headers=headers, data=payload)
 
print(response.text)