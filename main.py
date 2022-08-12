# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC - Clone all content of metastore
# MAGIC   - Storage credentials [X]
# MAGIC   - External locations [X]
# MAGIC   - Catalogs, Databases, Tables, Views [X]
# MAGIC   - ACLs []
# MAGIC   - Delta Sharing []
# MAGIC - What about data? It only works with external tables []

# COMMAND ----------

def get_catalogs(source_workspace_url, source_workspace_header):
  response = requests.get('{}/catalogs/'.format(source_workspace_url), headers=source_workspace_header)
  return response.json()

def get_schemas(source_workspace_url, source_workspace_header, catalog_name):
  response = requests.get('{}schemas?catalog_name={}'.format(source_workspace_url, catalog_name), headers=source_workspace_header)
  return response.json()

def get_tables(source_workspace_url, source_workspace_header, catalog_name, schema_name):
  response = requests.get('{}tables?catalog_name={}&schema_name={}'.format(source_workspace_url, catalog_name, schema_name), headers=source_workspace_header)
  return response.json()

# What about the secret?
def get_storage_credentials(source_workspace_url, source_workspace_header):
  response = requests.get('{}storage-credentials'.format(source_workspace_url), headers=source_workspace_header)
  return response.json()

# What about the secret?
def get_storage_credentials(source_workspace_url, source_workspace_header):
  response = requests.get('{}storage-credentials'.format(source_workspace_url), headers=source_workspace_header)
  return response.json()

def get_external_locations(source_workspace_url, source_workspace_header):
  response = requests.get('{}external-locations'.format(source_workspace_url), headers=source_workspace_header)
  return response.json()

# COMMAND ----------

#element = {}
def post_catalog(source_workspace_url, source_workspace_header, element):
  response = requests.post('{}/catalogs/'.format(source_workspace_url), json=element, headers=source_workspace_header)
  return response.json()

#element = {'name': 'manufacturing_junichi_maruyama', 'owner': 'junichi.maruyama@databricks.com', 'catalog_name': 'catalog_junichi_maruyama'}
def post_schema(source_workspace_url, source_workspace_header, element):
  response = requests.post('{}schemas'.format(source_workspace_url), json=element, headers=source_workspace_header)
  return response.json()

#element = {'name': 'pump', 'owner': 'junichi.maruyama@databricks.com', 'catalog_name': 'catalog_junichi_maruyama', 'schema_name': 'manufacturing_junichi_maruyama', 'table_type': 'MANAGED', 'data_source_format': 'DELTA', 'properties': {'delta.lastCommitTimestamp': '1657105958000', 'delta.lastUpdateVersion': '0', 'delta.minReaderVersion': '1', 'delta.minWriterVersion': '2'}, 'columns': [{'name': 'country', 'type_text': 'string', 'type_json': '{"name":"country","type":"string","nullable":true,"metadata":{}}', 'type_name': 'STRING', 'type_precision': 0, 'type_scale': 0, 'position': 0, 'nullable': True}, {'name': 'manufacturer', 'type_text': 'string', 'type_json': '{"name":"manufacturer","type":"string","nullable":true,"metadata":{}}', 'type_name': 'STRING', 'type_precision': 0, 'type_scale': 0, 'position': 1, 'nullable': True}, {'name': 'pumpId', 'type_text': 'int', 'type_json': '{"name":"pumpId","type":"integer","nullable":true,"metadata":{}}', 'type_name': 'INT', 'type_precision': 0, 'type_scale': 0, 'position': 2, 'nullable': True}, {'name': 'region', 'type_text': 'string', 'type_json': '{"name":"region","type":"string","nullable":true,"metadata":{}}', 'type_name': 'STRING', 'type_precision': 0, 'type_scale': 0, 'position': 3, 'nullable': True}]}
def post_table(source_workspace_url, source_workspace_header, element):
  response = requests.post('{}tables'.format(source_workspace_url), json=element, headers=source_workspace_header)
  return response.json()

#element = {'name': 'mzeni-test-api', 'comment': 'This is a test to create a Storage Credential from APIs', 'skip_validation': False, 'azure_service_principal': {'directory_id': '9f37a392-f0ae-4280-9796-f1864a10effc', 'application_id': 'ed573937-9c53-4ed6-b016-929e765443eb', 'client_secret': 'secret'}}
#element = {'name': 'mzeni-test-api-mi', 'comment': 'This is a test to create a Storage Credential from APIs', 'skip_validation': False, 'azure_managed_identity': {'access_connector_id': '/subscriptions/3f2e4d32-8e8d-46d6-82bc-5bb8d962328b/resourceGroups/mzeni-tests/providers/Microsoft.Databricks/accessConnectors/connector-mi-unity-catalog'}}
def post_storage_credential(source_workspace_url, source_workspace_header, element):
  response = requests.post('{}storage-credentials'.format(source_workspace_url), json=element, headers=source_workspace_header)
  return response.json()

#element = {'name': 'birexternallocation', 'comment': 'testtt', 'url': 'abfss://deltalake@oneenvadls.dfs.core.windows.net/mattia/test', 'credential_name': 'mzeni-test-api-mi', 'read_only': False}
def post_external_location(source_workspace_url, source_workspace_header, element):
  response = requests.post('{}external-locations'.format(source_workspace_url), json=element, headers=source_workspace_header)
  return response.json()

# COMMAND ----------

def create_catalogs(destination_workspace_url, destination_workspace_header, catalogs):
  for element in process_catalogs(catalogs):
    post_catalogs(element)
    print(response)

# COMMAND ----------

def process_catalogs(catalogs_get_output):
  if 'catalogs' not in catalogs_get_output:
    return
  
  catalogs = []
  
  for catalog_in_output in catalogs_get_output['catalogs']:
    catalog = {}
    catalog['name'] = catalog_in_output['name']
    catalog['owner'] = catalog_in_output['name']
    catalogs.append(catalog)
  return catalogs

# COMMAND ----------

import requests

source_workspace_url = '{}api/2.0/unity-catalog/'.format(dbutils.widgets.get('source_workspace_url'))
source_workspace_header = {'Authorization': 'Bearer ' + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)}

destination_workspace_url = '{}api/2.0/unity-catalog/'.format(dbutils.widgets.get('destination_workspace_url'))
destination_workspace_header = {'Authorization': 'Bearer ' + dbutils.widgets.get('destination_workspace_token')}

#catalogs = get_catalogs(source_workspace_url, source_workspace_header)
#schemas = get_schemas(source_workspace_url, source_workspace_header, 'catalog_junichi_maruyama')
#tables = get_tables(source_workspace_url, source_workspace_header, 'catalog_junichi_maruyama', 'manufacturing_junichi_maruyama')
#storage_credentials = get_storage_credentials(source_workspace_url, source_workspace_header)
#external_locations = get_external_locations(source_workspace_url, source_workspace_header)

#print(catalogs)
#print(schemas)
#print(tables)
#print(storage_credentials)
#print(external_locations)

#create_catalogs(destination_workspace_url, destination_workspace_header, catalogs)
#post_schema(destination_workspace_url, destination_workspace_header, element)
#post_table(destination_workspace_url, destination_workspace_header, element)
#post_storage_credential(destination_workspace_url, destination_workspace_header, element)

#post_external_location(destination_workspace_url, destination_workspace_header, element)
