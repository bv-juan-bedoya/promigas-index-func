FROM mcr.microsoft.com/azure-functions/python:4-python3.11

WORKDIR /home/site/wwwroot

COPY . /home/site/wwwroot

RUN pip install -r requirements.txt

# Start the Azure Functions host
CMD ["python", "-m", "azure_functions_worker"]